// Shared .ims helpers for the tests and the imscompare tool (header-only, C++17, HDF5
// C library + lz4.h).
//
// An Imaris 5.5 .ims file is HDF5: /DataSet/ResolutionLevel r/TimePoint t/Channel c
// holds a chunked 3-D Data dataset (dims z,y,x) next to its Histogram datasets, and
// /DataSetInfo holds the metadata as string attributes. ImarisWriter compresses every
// chunk itself (byte shuffle for 2- and 4-byte integers, then LZ4 in the HDF5 lz4
// filter format, id 32004) and writes it with H5Dwrite_chunk, so these helpers read
// raw chunks and undo the filters here; no HDF5 filter plugin is needed.
//
//   imsReadVolume(file, level, t, c, info)  -> the voxels of one dataset, x fastest,
//                                              cropped to ImageSizeX/Y/Z
//   imsCompare(a, b, report[, options])     -> true if the two files hold the same
//                                              chunks (compressed bytes), histograms,
//                                              attributes and thumbnail; options can
//                                              map (t,c) of a to (t,c) of b
//   imsDeepCheck(file, report[, threads])   -> true if every chunk decodes and every
//                                              stored histogram matches the decoded data
//   imsReadAttribute(s) / imsReadHistogram / imsReadThumbnail / imsExists  -> small accessors for checks
//   imsCheckDataSetInfo(file, expected, problems) -> every /DataSetInfo attribute against
//                                              the expected metadata
//
// Portable (Linux, macOS, Windows): std::thread and std::ifstream only, no pread or
// OpenMP. Chunks are listed with one H5Dchunk_iter pass per dataset, never with
// H5Dget_chunk_info by index (which walks the chunk index from the start each time).
// HDF5 is called from one thread only; worker threads read chunk bytes straight from
// the file at the addresses H5Dchunk_iter reports (absolute file offsets, user block
// included) and compare or decode them, which needs no thread-safe HDF5 build.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "hdf5.h"
#include "lz4.h"

namespace imsutil {

// Owns one HDF5 id and closes it with the matching H5?close.
class Hid {
public:
    Hid() = default;
    Hid(hid_t id, herr_t (*close)(hid_t)) : id_(id), close_(close) {}
    Hid(const Hid&) = delete;
    Hid& operator=(const Hid&) = delete;
    Hid(Hid&& o) noexcept : id_(o.id_), close_(o.close_) { o.id_ = -1; }
    Hid& operator=(Hid&& o) noexcept {
        if(this != &o){ reset(); id_ = o.id_; close_ = o.close_; o.id_ = -1; }
        return *this;
    }
    ~Hid(){ reset(); }
    void reset(){ if(id_ >= 0 && close_) close_(id_); id_ = -1; }
    bool valid() const { return id_ >= 0; }
    operator hid_t() const { return id_; }
private:
    hid_t id_ = -1;
    herr_t (*close_)(hid_t) = nullptr;
};

// Turns off HDF5's error-stack printing while alive (absent objects are probed on purpose).
class QuietErrors {
public:
    QuietErrors(){ H5Eget_auto2(H5E_DEFAULT, &func_, &data_); H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr); }
    ~QuietErrors(){ H5Eset_auto2(H5E_DEFAULT, func_, data_); }
    QuietErrors(const QuietErrors&) = delete;
    QuietErrors& operator=(const QuietErrors&) = delete;
private:
    H5E_auto2_t func_ = nullptr;
    void* data_ = nullptr;
};

inline Hid openFile(const std::filesystem::path& p){
    return Hid(H5Fopen(p.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
}

// True if every component of an absolute path exists (H5Lexists fails on a missing parent).
inline bool exists(hid_t file, const std::string& path){
    if(path.empty() || path == "/") return true;
    size_t pos = 0;
    while(pos != std::string::npos){
        pos = path.find('/', pos + 1);
        std::string prefix = path.substr(0, pos);
        if(H5Lexists(file, prefix.c_str(), H5P_DEFAULT) <= 0) return false;
    }
    return H5Oexists_by_name(file, path.c_str(), H5P_DEFAULT) > 0;
}

inline std::string datasetGroup(int level, int timepoint, int channel){
    return "/DataSet/ResolutionLevel " + std::to_string(level) + "/TimePoint " +
           std::to_string(timepoint) + "/Channel " + std::to_string(channel);
}

// Names of the links in a group, sorted.
inline std::vector<std::string> childNames(hid_t file, const std::string& path){
    std::vector<std::string> names;
    Hid g(H5Gopen2(file, path.c_str(), H5P_DEFAULT), H5Gclose);
    if(!g.valid()) return names;
    H5G_info_t gi;
    if(H5Gget_info(g, &gi) < 0) return names;
    for(hsize_t i = 0; i < gi.nlinks; ++i){
        ssize_t n = H5Lget_name_by_idx(g, ".", H5_INDEX_NAME, H5_ITER_INC, i, nullptr, 0, H5P_DEFAULT);
        if(n < 0) continue;
        std::vector<char> buf((size_t)n + 1, 0);
        H5Lget_name_by_idx(g, ".", H5_INDEX_NAME, H5_ITER_INC, i, buf.data(), buf.size(), H5P_DEFAULT);
        names.emplace_back(buf.data());
    }
    std::sort(names.begin(), names.end());
    return names;
}

inline herr_t attrNameCb(hid_t, const char* name, const H5A_info_t*, void* ud){
    static_cast<std::vector<std::string>*>(ud)->push_back(name);
    return 0;
}
inline std::vector<std::string> attrNames(hid_t obj){
    std::vector<std::string> names;
    hsize_t idx = 0;
    H5Aiterate2(obj, H5_INDEX_NAME, H5_ITER_INC, &idx, attrNameCb, &names);
    std::sort(names.begin(), names.end());
    return names;
}

// An attribute's value as comparable bytes plus a printable form. .ims attributes are
// 1-D arrays of 1-byte strings without a NUL; others are read in their native type.
struct AttrValue {
    H5T_class_t cls = H5T_NO_CLASS;
    size_t typeSize = 0;
    hssize_t npoints = 0;
    std::vector<char> bytes;
    std::string text() const {
        if(cls == H5T_STRING) return std::string(bytes.begin(), bytes.end());
        return "<" + std::to_string(npoints) + " values of " + std::to_string(typeSize) + " bytes>";
    }
    bool operator==(const AttrValue& o) const {
        return cls == o.cls && typeSize == o.typeSize && npoints == o.npoints && bytes == o.bytes;
    }
};

inline bool readAttr(hid_t obj, const std::string& name, AttrValue& v){
    Hid a(H5Aopen(obj, name.c_str(), H5P_DEFAULT), H5Aclose);
    if(!a.valid()) return false;
    Hid t(H5Aget_type(a), H5Tclose), s(H5Aget_space(a), H5Sclose);
    v.cls = H5Tget_class(t);
    v.npoints = H5Sget_simple_extent_npoints(s);
    if(v.npoints < 0) v.npoints = 0;
    if(v.cls == H5T_STRING && H5Tis_variable_str(t) > 0){
        std::vector<char*> p((size_t)v.npoints, nullptr);
        Hid mt(H5Tcopy(H5T_C_S1), H5Tclose);
        H5Tset_size(mt, H5T_VARIABLE);
        if(v.npoints && H5Aread(a, mt, p.data()) < 0) return false;
        for(char* q : p){ if(q) v.bytes.insert(v.bytes.end(), q, q + std::strlen(q)); v.bytes.push_back('\n'); }
        if(v.npoints) H5Treclaim(mt, s, H5P_DEFAULT, p.data());
        v.typeSize = 0;
        return true;
    }
    Hid mt(v.cls == H5T_STRING ? H5Tcopy(t) : H5Tget_native_type(t, H5T_DIR_ASCEND), H5Tclose);
    v.typeSize = H5Tget_size(mt);
    v.bytes.assign((size_t)v.npoints * v.typeSize, 0);
    if(!v.bytes.empty() && H5Aread(a, mt, v.bytes.data()) < 0) return false;
    if(v.cls == H5T_STRING){   // drop NUL padding so "abc" and "abc\0" compare as text
        auto e = std::find(v.bytes.begin(), v.bytes.end(), '\0');
        v.bytes.erase(e, v.bytes.end());
    }
    return true;
}

// The data of a small dataset (Histogram, Thumbnail) in its native type.
inline bool readDatasetBytes(hid_t d, std::vector<char>& bytes, size_t& typeSize){
    Hid t(H5Dget_type(d), H5Tclose), s(H5Dget_space(d), H5Sclose);
    Hid mt(H5Tget_native_type(t, H5T_DIR_ASCEND), H5Tclose);
    hssize_t n = H5Sget_simple_extent_npoints(s);
    if(n < 0) return false;
    typeSize = H5Tget_size(mt);
    bytes.assign((size_t)n * typeSize, 0);
    return bytes.empty() || H5Dread(d, mt, H5S_ALL, H5S_ALL, H5P_DEFAULT, bytes.data()) >= 0;
}

struct Filter { H5Z_filter_t id; std::vector<unsigned> cd; };
inline std::vector<Filter> filters(hid_t dcpl){
    std::vector<Filter> out;
    int nf = H5Pget_nfilters(dcpl);
    for(int i = 0; i < nf; ++i){
        unsigned flags = 0, cfg = 0; size_t ncd = 16; unsigned cd[16] = {0}; char nm[128];
        H5Z_filter_t id = H5Pget_filter2(dcpl, (unsigned)i, &flags, &ncd, cd, sizeof nm, nm, &cfg);
        Filter f; f.id = id; f.cd.assign(cd, cd + std::min<size_t>(ncd, 16));
        out.push_back(f);
    }
    return out;
}

inline uint64_t readBE(const uint8_t* p, int n){
    uint64_t v = 0;
    for(int i = 0; i < n; ++i) v = (v << 8) | p[i];
    return v;
}

const H5Z_filter_t kFilterLZ4 = 32004;

// Undo the lz4 filter: [8 B raw size][4 B block size], then per block [4 B compressed
// size][data], all big-endian; a block stored with its raw size is not compressed.
inline bool lz4Decode(const std::vector<uint8_t>& in, std::vector<uint8_t>& out, std::string& err){
    if(in.size() < 12){ err = "lz4 chunk shorter than its header"; return false; }
    uint64_t total = readBE(in.data(), 8), block = readBE(in.data() + 8, 4);
    if(block == 0){ err = "lz4 block size 0"; return false; }
    out.assign((size_t)total, 0);
    size_t ip = 12, op = 0;
    while(op < total){
        size_t want = (size_t)std::min<uint64_t>(block, total - op);
        if(ip + 4 > in.size()){ err = "lz4 chunk truncated"; return false; }
        size_t comp = (size_t)readBE(in.data() + ip, 4); ip += 4;
        if(ip + comp > in.size()){ err = "lz4 block runs past the chunk"; return false; }
        if(comp == want){
            std::memcpy(out.data() + op, in.data() + ip, want);
        } else {
            int n = LZ4_decompress_safe(reinterpret_cast<const char*>(in.data() + ip),
                                        reinterpret_cast<char*>(out.data() + op), (int)comp, (int)want);
            if(n != (int)want){ err = "lz4 decode gave " + std::to_string(n) + " of " + std::to_string(want) + " bytes"; return false; }
        }
        ip += comp; op += want;
    }
    return true;
}

// Undo the HDF5 byte shuffle: byte b of element k was stored at b*n + k.
inline void unshuffle(const std::vector<uint8_t>& in, std::vector<uint8_t>& out, size_t es){
    out.resize(in.size());
    size_t n = in.size() / es;
    if(es == 2){
        const uint8_t* lo = in.data(); const uint8_t* hi = in.data() + n;
        for(size_t k = 0; k < n; ++k){ out[2*k] = lo[k]; out[2*k + 1] = hi[k]; }
    } else {
        for(size_t b = 0; b < es; ++b)
            for(size_t k = 0; k < n; ++k) out[k*es + b] = in[b*n + k];
    }
    std::copy(in.begin() + (std::ptrdiff_t)(n*es), in.end(), out.begin() + (std::ptrdiff_t)(n*es));
}

// Undoes the filter pipeline fl of one raw chunk in place (a in, decoded bytes out;
// tmp is scratch). Bit i of mask set means filter i was skipped when it was written.
inline bool decodeChunk(std::vector<uint8_t>& a, std::vector<uint8_t>& tmp, const std::vector<Filter>& fl,
                        unsigned mask, size_t shuffleBytes, std::string& err){
    for(int i = (int)fl.size() - 1; i >= 0; --i){
        if(mask & (1u << i)) continue;
        if(fl[i].id == kFilterLZ4){
            if(!lz4Decode(a, tmp, err)) return false;
        } else if(fl[i].id == H5Z_FILTER_SHUFFLE){
            if(shuffleBytes > 1) unshuffle(a, tmp, shuffleBytes); else tmp = a;
        } else { err = "unsupported filter " + std::to_string((int)fl[i].id); return false; }
        a.swap(tmp);
    }
    return true;
}

struct Chunk { hsize_t off[3]; unsigned mask; haddr_t addr; hsize_t size; };
inline int chunkCb(const hsize_t* off, unsigned mask, haddr_t addr, hsize_t size, void* ud){
    Chunk c; c.off[0] = off[0]; c.off[1] = off[1]; c.off[2] = off[2]; c.mask = mask; c.addr = addr; c.size = size;
    static_cast<std::vector<Chunk>*>(ud)->push_back(c);
    return 0;
}
inline bool chunkLess(const Chunk& a, const Chunk& b){
    return std::lexicographical_compare(a.off, a.off + 3, b.off, b.off + 3);
}
// All stored chunks of a 3-D dataset, sorted by offset (z, y, x).
inline bool listChunks(hid_t d, std::vector<Chunk>& out){
    out.clear();
    if(H5Dchunk_iter(d, H5P_DEFAULT, chunkCb, &out) < 0) return false;
    std::sort(out.begin(), out.end(), chunkLess);
    return true;
}
inline bool readRawChunk(hid_t d, const Chunk& c, std::vector<uint8_t>& buf){
    buf.resize((size_t)c.size);
    uint32_t fm = 0;
    return H5Dread_chunk(d, H5P_DEFAULT, c.off, &fm, buf.data()) >= 0;
}
inline std::string offStr(const hsize_t* o){
    return "(z,y,x)=(" + std::to_string((unsigned long long)o[0]) + "," + std::to_string((unsigned long long)o[1]) +
           "," + std::to_string((unsigned long long)o[2]) + ")";
}

// The name of the file that holds an object (differs from the opened file when the
// path runs through an external link, as in a link master + parts/ output).
inline std::string fileOf(hid_t obj){
    Hid f(H5Iget_file_id(obj), H5Fclose);
    if(!f.valid()) return std::string();
    ssize_t n = H5Fget_name(f, nullptr, 0);
    if(n <= 0) return std::string();
    std::vector<char> buf((size_t)n + 1, 0);
    H5Fget_name(f, buf.data(), buf.size());
    return std::string(buf.data());
}

// Reads byte ranges of files at absolute offsets; owned by one thread. Unbuffered
// streams: chunks are large and each is read once.
class RawReader {
public:
    bool read(const std::string& file, haddr_t offset, size_t n, std::vector<uint8_t>& buf){
        buf.resize(n);
        if(offset == HADDR_UNDEF) return false;
        std::unique_ptr<std::ifstream>& s = files_[file];
        if(!s){
            s.reset(new std::ifstream());
            s->rdbuf()->pubsetbuf(nullptr, 0);
            s->open(std::filesystem::path(file), std::ios::binary);
        }
        if(!s->is_open()) return false;
        s->clear();
        s->seekg((std::streamoff)offset);
        if(n) s->read(reinterpret_cast<char*>(buf.data()), (std::streamsize)n);
        return !s->fail() && (size_t)s->gcount() == n;
    }
    void close(){ files_.clear(); }
private:
    std::map<std::string, std::unique_ptr<std::ifstream>> files_;
};

// Runs fn(i, reader, thread) for every i in [0, n) on up to readers.size() threads
// (one reader each), handing out indices in small runs in increasing order.
template<class Fn> inline void parallelFor(size_t n, std::vector<RawReader>& readers, Fn&& fn){
    const size_t batch = 8;
    size_t nt = std::max<size_t>(1, std::min(readers.size(), (n + batch - 1) / batch));
    std::atomic<size_t> next(0);
    auto work = [&](size_t t){
        for(;;){
            size_t b = next.fetch_add(batch);
            if(b >= n) return;
            for(size_t i = b; i < std::min(n, b + batch); ++i) fn(i, readers[t], t);
        }
    };
    std::vector<std::thread> pool;
    for(size_t t = 1; t < nt; ++t) pool.emplace_back(work, t);
    work(0);
    for(std::thread& th : pool) th.join();
}

inline uint64_t parseU64(const std::string& s){ return std::strtoull(s.c_str(), nullptr, 10); }

} // namespace imsutil

// ------------------------------------------------------------------------------------
// Decoding one dataset
// ------------------------------------------------------------------------------------

struct ImsVolumeInfo {
    uint64_t sizeX = 0, sizeY = 0, sizeZ = 0;     // ImageSizeX/Y/Z of this level
    uint64_t chunkX = 0, chunkY = 0, chunkZ = 0;  // HDF5 chunk dims
    size_t elementSize = 0;                       // bytes per voxel
    H5T_class_t typeClass = H5T_NO_CLASS;         // H5T_INTEGER or H5T_FLOAT
    bool isSigned = false;
    bool shuffle = false;                         // the dataset has the shuffle filter
    uint64_t chunkCount = 0;                      // chunks stored
    uint64_t expectedChunks = 0;                  // chunks that cover ImageSizeX/Y/Z
    std::string error;                            // set when imsReadVolume returns nothing
};

// The voxels of /DataSet/ResolutionLevel <level>/TimePoint <t>/Channel <c>/Data, x
// fastest, cropped to ImageSizeX/Y/Z (the Data dims are padded to whole chunks).
// Chunks that are not stored read as 0. On error: empty vector and info.error set.
inline std::vector<uint8_t> imsReadVolume(const std::filesystem::path& file, int level, int timepoint,
                                          int channel, ImsVolumeInfo& info){
    using namespace imsutil;
    QuietErrors quiet;
    info = ImsVolumeInfo();
    std::vector<uint8_t> vol;
    Hid f = openFile(file);
    if(!f.valid()){ info.error = "cannot open " + file.string(); return vol; }
    const std::string gp = datasetGroup(level, timepoint, channel);
    if(!exists(f, gp + "/Data")){ info.error = gp + "/Data does not exist"; return vol; }
    Hid g(H5Gopen2(f, gp.c_str(), H5P_DEFAULT), H5Gclose);
    Hid d(H5Dopen2(g, "Data", H5P_DEFAULT), H5Dclose);
    Hid t(H5Dget_type(d), H5Tclose), sp(H5Dget_space(d), H5Sclose), pl(H5Dget_create_plist(d), H5Pclose);

    AttrValue ax, ay, az;
    if(!readAttr(g, "ImageSizeX", ax) || !readAttr(g, "ImageSizeY", ay) || !readAttr(g, "ImageSizeZ", az)){
        info.error = gp + ": ImageSizeX/Y/Z missing"; return vol; }
    info.sizeX = parseU64(ax.text()); info.sizeY = parseU64(ay.text()); info.sizeZ = parseU64(az.text());
    info.typeClass = H5Tget_class(t);
    info.elementSize = H5Tget_size(t);
    info.isSigned = info.typeClass == H5T_INTEGER && H5Tget_sign(t) == H5T_SGN_2;

    if(H5Sget_simple_extent_ndims(sp) != 3){ info.error = gp + "/Data is not 3-D"; return vol; }
    hsize_t dims[3] = {0, 0, 0}, cd[3] = {0, 0, 0};
    H5Sget_simple_extent_dims(sp, dims, nullptr);
    if(H5Pget_layout(pl) != H5D_CHUNKED || H5Pget_chunk(pl, 3, cd) != 3){ info.error = gp + "/Data is not chunked"; return vol; }
    info.chunkZ = cd[0]; info.chunkY = cd[1]; info.chunkX = cd[2];
    if(info.sizeX > dims[2] || info.sizeY > dims[1] || info.sizeZ > dims[0]){
        info.error = gp + ": ImageSize larger than the Data dims"; return vol; }
    info.expectedChunks = ((info.sizeX + cd[2] - 1) / cd[2]) * ((info.sizeY + cd[1] - 1) / cd[1]) *
                          ((info.sizeZ + cd[0] - 1) / cd[0]);

    const std::vector<Filter> fl = filters(pl);
    size_t shuffleBytes = info.elementSize;
    for(const Filter& x : fl){
        if(x.id == H5Z_FILTER_SHUFFLE){ info.shuffle = true; if(!x.cd.empty() && x.cd[0]) shuffleBytes = x.cd[0]; }
        else if(x.id != kFilterLZ4){ info.error = gp + "/Data: unsupported filter " + std::to_string((int)x.id); return vol; }
    }

    std::vector<Chunk> chunks;
    if(!listChunks(d, chunks)){ info.error = gp + "/Data: H5Dchunk_iter failed"; return vol; }
    info.chunkCount = chunks.size();

    const size_t es = info.elementSize;
    const size_t chunkBytes = (size_t)(cd[0] * cd[1] * cd[2]) * es;
    vol.assign((size_t)(info.sizeX * info.sizeY * info.sizeZ) * es, 0);
    std::vector<uint8_t> raw, a, b;
    for(const Chunk& c : chunks){
        if(!readRawChunk(d, c, raw)){ info.error = gp + ": cannot read chunk " + offStr(c.off); vol.clear(); return vol; }
        a.swap(raw);
        std::string err;
        if(!decodeChunk(a, b, fl, c.mask, shuffleBytes, err)){ info.error = gp + " chunk " + offStr(c.off) + ": " + err; vol.clear(); return vol; }
        if(a.size() != chunkBytes){
            info.error = gp + " chunk " + offStr(c.off) + ": " + std::to_string(a.size()) + " bytes decoded, " +
                         std::to_string(chunkBytes) + " expected";
            vol.clear(); return vol;
        }
        const hsize_t oz = c.off[0], oy = c.off[1], ox = c.off[2];
        if(ox >= info.sizeX) continue;
        const size_t nx = (size_t)std::min<uint64_t>(cd[2], info.sizeX - ox);
        for(hsize_t z = 0; z < cd[0] && oz + z < info.sizeZ; ++z)
            for(hsize_t y = 0; y < cd[1] && oy + y < info.sizeY; ++y)
                std::memcpy(&vol[(size_t)(ox + (oy + y) * info.sizeX + (oz + z) * info.sizeX * info.sizeY) * es],
                            &a[(size_t)(y * cd[2] + z * cd[2] * cd[1]) * es], nx * es);
    }
    return vol;
}

// ------------------------------------------------------------------------------------
// Small accessors
// ------------------------------------------------------------------------------------

inline bool imsExists(const std::filesystem::path& file, const std::string& objectPath){
    imsutil::QuietErrors quiet;
    imsutil::Hid f = imsutil::openFile(file);
    return f.valid() && imsutil::exists(f, objectPath);
}

// A (string) attribute of any object, e.g. ("/DataSetInfo/Image", "ExtMax0").
inline std::optional<std::string> imsReadAttribute(const std::filesystem::path& file, const std::string& objectPath,
                                                   const std::string& name){
    using namespace imsutil;
    QuietErrors quiet;
    Hid f = openFile(file);
    if(!f.valid() || !exists(f, objectPath)) return std::nullopt;
    Hid o(H5Oopen(f, objectPath.c_str(), H5P_DEFAULT), H5Oclose);
    AttrValue v;
    if(!o.valid() || H5Aexists(o, name.c_str()) <= 0 || !readAttr(o, name, v)) return std::nullopt;
    return v.text();
}

// Every attribute of one object as text, by name (empty if the object is missing), e.g. the
// whole of "/DataSetInfo/Channel 0".
inline std::map<std::string, std::string> imsReadAttributes(const std::filesystem::path& file, const std::string& objectPath){
    using namespace imsutil;
    QuietErrors quiet;
    std::map<std::string, std::string> all;
    Hid f = openFile(file);
    if(!f.valid() || !exists(f, objectPath)) return all;
    Hid o(H5Oopen(f, objectPath.c_str(), H5P_DEFAULT), H5Oclose);
    if(!o.valid()) return all;
    for(const std::string& n : attrNames(o)){
        AttrValue v;
        all[n] = readAttr(o, n, v) ? v.text() : std::string("<unreadable>");
    }
    return all;
}

// A histogram dataset ("Histogram" or "Histogram1024") of one level/timepoint/channel.
inline std::vector<uint64_t> imsReadHistogram(const std::filesystem::path& file, int level, int timepoint,
                                              int channel, const char* name = "Histogram"){
    using namespace imsutil;
    QuietErrors quiet;
    std::vector<uint64_t> h;
    Hid f = openFile(file);
    const std::string p = datasetGroup(level, timepoint, channel) + "/" + name;
    if(!f.valid() || !exists(f, p)) return h;
    Hid d(H5Dopen2(f, p.c_str(), H5P_DEFAULT), H5Dclose);
    Hid s(H5Dget_space(d), H5Sclose);
    hssize_t n = H5Sget_simple_extent_npoints(s);
    if(n <= 0) return h;
    h.assign((size_t)n, 0);
    if(H5Dread(d, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, h.data()) < 0) h.clear();
    return h;
}

// /Thumbnail/Data: its shape (rows, bytes per row = 4 * pixels per row) and its RGBA bytes; false if absent.
inline bool imsReadThumbnail(const std::filesystem::path& file, uint64_t& rows, uint64_t& rowBytes, std::vector<uint8_t>& rgba){
    using namespace imsutil;
    QuietErrors quiet;
    rows = rowBytes = 0; rgba.clear();
    Hid f = openFile(file);
    if(!f.valid() || !exists(f, "/Thumbnail/Data")) return false;
    Hid d(H5Dopen2(f, "/Thumbnail/Data", H5P_DEFAULT), H5Dclose);
    Hid s(H5Dget_space(d), H5Sclose);
    hsize_t dims[2] = {0, 0};
    if(!s.valid() || H5Sget_simple_extent_ndims(s) != 2 || H5Sget_simple_extent_dims(s, dims, nullptr) != 2) return false;
    rows = dims[0]; rowBytes = dims[1];
    rgba.assign((size_t)(rows * rowBytes), 0);
    return rgba.empty() || H5Dread(d, H5T_NATIVE_UCHAR, H5S_ALL, H5S_ALL, H5P_DEFAULT, rgba.data()) >= 0;
}

// ------------------------------------------------------------------------------------
// Comparing two files
// ------------------------------------------------------------------------------------

// Compare /DataSet/ResolutionLevel r/TimePoint tA/Channel cA of file a with
// /DataSet/ResolutionLevel r/TimePoint tB/Channel cB of file b, for every level r.
struct ImsMapping { int tA = 0, cA = 0, tB = 0, cB = 0; };

struct ImsCompareOptions {
    bool histograms = true;         // the datasets next to Data (Histogram, Histogram1024)
    bool datasetAttributes = true;  // attributes of the /DataSet groups (ImageSize, HistogramMin/Max) and of Data
    bool dataSetInfo = true;        // the root's links and attributes and every /DataSetInfo group's attributes
    bool thumbnail = true;          // /Thumbnail: presence, values and attributes (reported apart)
    std::vector<ImsMapping> map;    // empty: every dataset with its namesake; else only these (see above),
                                    // and the root, /DataSetInfo and /Thumbnail are not compared
    int threads = 1;                // threads reading and comparing chunk bytes
};

struct ImsCompareReport {
    std::vector<std::string> differences;  // one line each, starting with the object's path
    std::vector<std::string> thumbnail;    // /Thumbnail differences (often expected, so kept apart)
    std::vector<std::string> notes;        // what was not compared or checked, for the summary
    std::string error;                     // a file could not be opened or read
    int datasets = 0;                      // Data datasets compared (or checked by imsDeepCheck)
    uint64_t chunks = 0;                   // chunks compared (or decoded)
    uint64_t bytes = 0;                    // compressed bytes compared per file (or decoded)
    bool identicalExceptThumbnail() const { return error.empty() && differences.empty() && datasets > 0; }
    bool identical() const { return identicalExceptThumbnail() && thumbnail.empty(); }
};

namespace imsutil {

// "p" if both files use the same path, else "pa (b: pb)".
inline std::string pairLabel(const std::string& pa, const std::string& pb){
    return pa == pb ? pa : pa + " (b: " + pb + ")";
}

inline void compareAttrs(hid_t fa, const std::string& pa, hid_t fb, const std::string& pb,
                         std::vector<std::string>& out){
    const std::string lbl = pairLabel(pa, pb);
    Hid oa(H5Oopen(fa, pa.c_str(), H5P_DEFAULT), H5Oclose), ob(H5Oopen(fb, pb.c_str(), H5P_DEFAULT), H5Oclose);
    if(!oa.valid() || !ob.valid()){ out.push_back(lbl + ": cannot open"); return; }
    std::vector<std::string> na = attrNames(oa), nb = attrNames(ob);
    std::set<std::string> all(na.begin(), na.end());
    all.insert(nb.begin(), nb.end());
    for(const std::string& n : all){
        bool inA = std::binary_search(na.begin(), na.end(), n), inB = std::binary_search(nb.begin(), nb.end(), n);
        if(!inA || !inB){ out.push_back(lbl + " attribute " + n + ": only in " + (inA ? "a" : "b")); continue; }
        AttrValue va, vb;
        if(!readAttr(oa, n, va) || !readAttr(ob, n, vb)){ out.push_back(lbl + " attribute " + n + ": unreadable"); continue; }
        if(!(va == vb)) out.push_back(lbl + " attribute " + n + ": a=\"" + va.text() + "\" b=\"" + vb.text() + "\"");
    }
}

// Cross-checks the raw file read of one chunk against H5Dread_chunk, so a wrong
// address or base offset cannot pass unnoticed.
inline bool rawReadAgrees(hid_t d, const std::string& file, const Chunk& c, RawReader& rd){
    std::vector<uint8_t> x, y;
    return rd.read(file, c.addr, (size_t)c.size, x) && readRawChunk(d, c, y) && x == y;
}

inline void compareData(hid_t fa, const std::string& pa, hid_t fb, const std::string& pb,
                        std::vector<RawReader>& readers, ImsCompareReport& r){
    const std::string lbl = pairLabel(pa, pb);
    Hid da(H5Dopen2(fa, pa.c_str(), H5P_DEFAULT), H5Dclose), db(H5Dopen2(fb, pb.c_str(), H5P_DEFAULT), H5Dclose);
    if(!da.valid() || !db.valid()){ r.differences.push_back(lbl + ": cannot open"); return; }
    ++r.datasets;
    Hid ta(H5Dget_type(da), H5Tclose), tb(H5Dget_type(db), H5Tclose);
    Hid sa(H5Dget_space(da), H5Sclose), sb(H5Dget_space(db), H5Sclose);
    Hid pla(H5Dget_create_plist(da), H5Pclose), plb(H5Dget_create_plist(db), H5Pclose);
    if(H5Tequal(ta, tb) <= 0){ r.differences.push_back(lbl + ": element types differ"); return; }
    if(H5Sget_simple_extent_ndims(sa) != 3 || H5Sget_simple_extent_ndims(sb) != 3){ r.differences.push_back(lbl + ": not 3-D"); return; }
    hsize_t dimA[3], dimB[3], cdA[3] = {0, 0, 0}, cdB[3] = {0, 0, 0};
    H5Sget_simple_extent_dims(sa, dimA, nullptr); H5Sget_simple_extent_dims(sb, dimB, nullptr);
    if(!std::equal(dimA, dimA + 3, dimB)){ r.differences.push_back(lbl + ": dims a=" + offStr(dimA) + " b=" + offStr(dimB)); return; }
    if(H5Pget_layout(pla) != H5D_CHUNKED || H5Pget_layout(plb) != H5D_CHUNKED){ r.differences.push_back(lbl + ": not chunked"); return; }
    H5Pget_chunk(pla, 3, cdA); H5Pget_chunk(plb, 3, cdB);
    if(!std::equal(cdA, cdA + 3, cdB)){ r.differences.push_back(lbl + ": chunk dims a=" + offStr(cdA) + " b=" + offStr(cdB)); return; }
    std::vector<Filter> fla = filters(pla), flb = filters(plb);
    bool sameFilters = fla.size() == flb.size();
    for(size_t i = 0; sameFilters && i < fla.size(); ++i) sameFilters = fla[i].id == flb[i].id && fla[i].cd == flb[i].cd;
    if(!sameFilters){ r.differences.push_back(lbl + ": filter pipelines differ"); return; }

    std::vector<Chunk> ca, cb;
    if(!listChunks(da, ca) || !listChunks(db, cb)){ r.differences.push_back(lbl + ": H5Dchunk_iter failed"); return; }
    const std::string fileA = fileOf(da), fileB = fileOf(db);
    if(!ca.empty() && !rawReadAgrees(da, fileA, ca[0], readers[0])){ r.error = pa + ": reading chunks at their file addresses failed"; return; }
    if(!cb.empty() && !rawReadAgrees(db, fileB, cb[0], readers[0])){ r.error = pb + ": reading chunks at their file addresses failed"; return; }

    // Walk both sorted lists together; pairs with the same size and mask get their bytes
    // compared below. pos = position in the merged order, so "first" is by coordinate.
    struct Pair { size_t i, j, pos; };
    std::vector<Pair> pairs;
    uint64_t onlyA = 0, onlyB = 0, differ = 0;
    size_t firstPos = SIZE_MAX; std::string firstDiff;
    auto note = [&](size_t pos, const std::string& what){ if(pos < firstPos){ firstPos = pos; firstDiff = what; } };
    size_t i = 0, j = 0, pos = 0;
    for(; i < ca.size() || j < cb.size(); ++pos){
        if(j == cb.size() || (i < ca.size() && chunkLess(ca[i], cb[j]))){
            note(pos, "chunk " + offStr(ca[i].off) + " only in a"); ++onlyA; ++i; continue;
        }
        if(i == ca.size() || chunkLess(cb[j], ca[i])){
            note(pos, "chunk " + offStr(cb[j].off) + " only in b"); ++onlyB; ++j; continue;
        }
        const Chunk& x = ca[i]; const Chunk& y = cb[j];
        if(x.size != y.size || x.mask != y.mask){
            ++differ;
            note(pos, "chunk " + offStr(x.off) + " differs (" + std::to_string((unsigned long long)x.size) + " vs " +
                      std::to_string((unsigned long long)y.size) + " bytes, filter mask " + std::to_string(x.mask) +
                      " vs " + std::to_string(y.mask) + ")");
        } else pairs.push_back(Pair{i, j, pos});
        ++i; ++j;
    }

    // 0 = same bytes, 1 = different bytes, 2 = a read failed.
    std::vector<uint8_t> state(pairs.size(), 0);
    std::vector<std::vector<uint8_t>> bufA(readers.size()), bufB(readers.size());
    parallelFor(pairs.size(), readers, [&](size_t k, RawReader& rd, size_t t){
        const Chunk& x = ca[pairs[k].i]; const Chunk& y = cb[pairs[k].j];
        if(!rd.read(fileA, x.addr, (size_t)x.size, bufA[t]) || !rd.read(fileB, y.addr, (size_t)y.size, bufB[t])) state[k] = 2;
        else if(bufA[t] != bufB[t]) state[k] = 1;
    });
    uint64_t unreadable = 0;
    for(size_t k = 0; k < pairs.size(); ++k){
        const Chunk& x = ca[pairs[k].i];
        if(state[k] == 0){ ++r.chunks; r.bytes += x.size; continue; }
        if(state[k] == 2){ ++unreadable; note(pairs[k].pos, "chunk " + offStr(x.off) + " could not be read"); continue; }
        ++differ;
        note(pairs[k].pos, "chunk " + offStr(x.off) + " differs (same size " + std::to_string((unsigned long long)x.size) +
                           ", different bytes)");
    }
    if(onlyA || onlyB || differ || unreadable)
        r.differences.push_back(lbl + ": " + std::to_string(ca.size()) + " vs " + std::to_string(cb.size()) + " chunks, " +
                                std::to_string(differ) + " differ, " + std::to_string(onlyA) + " only in a, " +
                                std::to_string(onlyB) + " only in b" +
                                (unreadable ? ", " + std::to_string(unreadable) + " unreadable" : std::string()) +
                                "; first: " + firstDiff);
}

inline void compareSmallDataset(hid_t fa, const std::string& pa, hid_t fb, const std::string& pb,
                                std::vector<std::string>& out){
    const std::string lbl = pairLabel(pa, pb);
    Hid da(H5Dopen2(fa, pa.c_str(), H5P_DEFAULT), H5Dclose), db(H5Dopen2(fb, pb.c_str(), H5P_DEFAULT), H5Dclose);
    std::vector<char> va, vb; size_t sa = 0, sb = 0;
    if(!da.valid() || !db.valid() || !readDatasetBytes(da, va, sa) || !readDatasetBytes(db, vb, sb)){
        out.push_back(lbl + ": unreadable"); return; }
    if(sa != sb || va.size() != vb.size()){ out.push_back(lbl + ": sizes differ (" + std::to_string(va.size() / std::max<size_t>(sa, 1)) +
                                                          " vs " + std::to_string(vb.size() / std::max<size_t>(sb, 1)) + " values)"); return; }
    if(va != vb){
        size_t k = 0; while(va[k] == vb[k]) ++k;
        out.push_back(lbl + ": values differ (first at value " + std::to_string(k / std::max<size_t>(sa, 1)) + ")");
    }
}

// Compares the members of one /DataSet/.../Channel c group of a with one of b.
inline void compareChannelGroup(hid_t fa, const std::string& ga, hid_t fb, const std::string& gb,
                                const ImsCompareOptions& o, std::vector<RawReader>& readers, ImsCompareReport& r){
    if(o.datasetAttributes) compareAttrs(fa, ga, fb, gb, r.differences);
    std::vector<std::string> ma = childNames(fa, ga), mb = childNames(fb, gb);
    std::set<std::string> all(ma.begin(), ma.end());
    all.insert(mb.begin(), mb.end());
    for(const std::string& m : all){
        const std::string pa = ga + "/" + m, pb = gb + "/" + m;
        bool inA = std::binary_search(ma.begin(), ma.end(), m), inB = std::binary_search(mb.begin(), mb.end(), m);
        if(m == "Data"){
            if(!inA || !inB){ r.differences.push_back(pairLabel(pa, pb) + ": only in " + std::string(inA ? "a" : "b")); continue; }
            compareData(fa, pa, fb, pb, readers, r);
            if(o.datasetAttributes) compareAttrs(fa, pa, fb, pb, r.differences);
        } else if(o.histograms){
            if(!inA || !inB) r.differences.push_back(pairLabel(pa, pb) + ": only in " + std::string(inA ? "a" : "b"));
            else compareSmallDataset(fa, pa, fb, pb, r.differences);
        }
    }
}

// Walks /DataSet down to depth 3 (ResolutionLevel / TimePoint / Channel) in both files.
inline void walkDataSet(hid_t fa, hid_t fb, const std::string& path, int depth, const ImsCompareOptions& o,
                        std::vector<RawReader>& readers, ImsCompareReport& r){
    if(depth == 3){ compareChannelGroup(fa, path, fb, path, o, readers, r); return; }
    if(o.datasetAttributes) compareAttrs(fa, path, fb, path, r.differences);
    std::vector<std::string> na = childNames(fa, path), nb = childNames(fb, path);
    std::set<std::string> all(na.begin(), na.end());
    all.insert(nb.begin(), nb.end());
    for(const std::string& n : all){
        bool inA = std::binary_search(na.begin(), na.end(), n), inB = std::binary_search(nb.begin(), nb.end(), n);
        if(!inA || !inB){ r.differences.push_back(path + "/" + n + ": only in " + (inA ? "a" : "b")); continue; }
        walkDataSet(fa, fb, path + "/" + n, depth + 1, o, readers, r);
    }
}

// Every (t,c) pair of a mapping, at every resolution level either file has.
inline void compareMapped(hid_t fa, hid_t fb, const ImsCompareOptions& o, std::vector<RawReader>& readers,
                          ImsCompareReport& r){
    std::vector<std::string> la = childNames(fa, "/DataSet"), lb = childNames(fb, "/DataSet");
    std::set<std::string> levels(la.begin(), la.end());
    levels.insert(lb.begin(), lb.end());
    for(const ImsMapping& m : o.map){
        bool any = false;
        for(const std::string& lvl : levels){
            const std::string ga = "/DataSet/" + lvl + "/TimePoint " + std::to_string(m.tA) + "/Channel " + std::to_string(m.cA);
            const std::string gb = "/DataSet/" + lvl + "/TimePoint " + std::to_string(m.tB) + "/Channel " + std::to_string(m.cB);
            bool ea = exists(fa, ga), eb = exists(fb, gb);
            if(!ea && !eb) continue;
            any = true;
            if(ea != eb) r.differences.push_back(pairLabel(ga, gb) + ": only in " + (ea ? "a" : "b"));
            else compareChannelGroup(fa, ga, fb, gb, o, readers, r);
        }
        if(!any) r.differences.push_back("timepoint " + std::to_string(m.tA) + " channel " + std::to_string(m.cA) + " of a / timepoint " +
                                          std::to_string(m.tB) + " channel " + std::to_string(m.cB) + " of b: in neither file");
    }
    r.notes.push_back("only the mapped datasets were compared (not the root, /DataSetInfo or /Thumbnail)");
}

} // namespace imsutil

// True if a and b hold the same data: for every /DataSet/ResolutionLevel r/TimePoint t/
// Channel c the same chunk set with the same compressed bytes (and sizes and filter
// masks), the same Histogram / Histogram1024 values and the same attributes; the same
// root links and attributes and /DataSetInfo/* attributes (Image, Channel N, TimeInfo,
// ImarisDataSet, ...); the same /Thumbnail (its differences go to report.thumbnail).
// Options switch off the parts other than the chunks, or map (t,c) pairs. The file
// layout (chunk addresses, object order) is not compared. report lists every difference.
inline bool imsCompare(const std::filesystem::path& a, const std::filesystem::path& b, ImsCompareReport& report,
                       const ImsCompareOptions& options = ImsCompareOptions()){
    using namespace imsutil;
    QuietErrors quiet;
    report = ImsCompareReport();
    Hid fa = openFile(a), fb = openFile(b);
    if(!fa.valid() || !fb.valid()){ report.error = "cannot open " + (fa.valid() ? b : a).string(); return false; }
    if(!exists(fa, "/DataSet") || !exists(fb, "/DataSet")){ report.error = "no /DataSet group"; return false; }
    std::vector<RawReader> readers((size_t)std::max(1, options.threads));
    if(!options.map.empty()){
        compareMapped(fa, fb, options, readers, report);
        return report.identical();
    }
    walkDataSet(fa, fb, "/DataSet", 0, options, readers, report);
    if(options.dataSetInfo){
        compareAttrs(fa, "/", fb, "/", report.differences);
        std::vector<std::string> ra = childNames(fa, "/"), rb = childNames(fb, "/");   // /Thumbnail: see below
        for(const std::string& n : ra) if(n != "Thumbnail" && !std::binary_search(rb.begin(), rb.end(), n)) report.differences.push_back("/" + n + ": only in a");
        for(const std::string& n : rb) if(n != "Thumbnail" && !std::binary_search(ra.begin(), ra.end(), n)) report.differences.push_back("/" + n + ": only in b");
        std::vector<std::string> ga = childNames(fa, "/DataSetInfo"), gb = childNames(fb, "/DataSetInfo");
        std::set<std::string> all(ga.begin(), ga.end());
        all.insert(gb.begin(), gb.end());
        for(const std::string& g : all){
            bool inA = std::binary_search(ga.begin(), ga.end(), g), inB = std::binary_search(gb.begin(), gb.end(), g);
            if(!inA || !inB) report.differences.push_back("/DataSetInfo/" + g + ": only in " + (inA ? "a" : "b"));
            else compareAttrs(fa, "/DataSetInfo/" + g, fb, "/DataSetInfo/" + g, report.differences);
        }
    } else report.notes.push_back("the root and /DataSetInfo were not compared");
    if(options.thumbnail){
        bool ta = exists(fa, "/Thumbnail/Data"), tb = exists(fb, "/Thumbnail/Data");
        if(ta != tb) report.thumbnail.push_back(std::string("/Thumbnail/Data: only in ") + (ta ? "a" : "b"));
        else if(ta){
            compareSmallDataset(fa, "/Thumbnail/Data", fb, "/Thumbnail/Data", report.thumbnail);
            compareAttrs(fa, "/Thumbnail", fb, "/Thumbnail", report.thumbnail);
            compareAttrs(fa, "/Thumbnail/Data", fb, "/Thumbnail/Data", report.thumbnail);
        }
    } else report.notes.push_back("/Thumbnail was not compared");
    if(!options.histograms) report.notes.push_back("the Histogram datasets were not compared");
    if(!options.datasetAttributes) report.notes.push_back("the /DataSet attributes (ImageSize, HistogramMin/Max) were not compared");
    return report.identical();
}

// ------------------------------------------------------------------------------------
// Checking one file without a reference
// ------------------------------------------------------------------------------------

namespace imsutil {

inline std::string fmt3(double v){ char b[64]; std::snprintf(b, sizeof b, "%.3f", v); return b; }

// The Histogram / Histogram1024 datasets and HistogramMin/Max attributes ImarisWriter
// writes for exact integer bins (bin i = count of value i): uint8 keeps its 256 bins
// over [0, 255]; uint16 (bpHistogramBuilder<bpUInt16>) trims trailing empty bins down to
// a top of at least 256, resamples to at most 1024 bins (Histogram1024, written when that
// is not 256 bins) and from that to 256 (Histogram); both start at 0.
struct ExpectedHistogram { std::vector<uint64_t> h256, h1024; std::string min, max; };
inline std::vector<uint64_t> resampleBins(const std::vector<uint64_t>& in, size_t n){
    if(in.size() == n) return in;
    std::vector<uint64_t> o(n, 0);
    for(size_t i = 0; i < in.size(); ++i) o[(size_t)((uint64_t)i * n / in.size())] += in[i];
    return o;
}
inline ExpectedHistogram expectedHistogram(const std::vector<uint64_t>& bins){
    ExpectedHistogram e;
    size_t top = bins.size() - 1;
    if(bins.size() > 256) while(256 < top && bins[top] == 0) --top;
    std::vector<uint64_t> trimmed(bins.begin(), bins.begin() + (std::ptrdiff_t)(top + 1));
    std::vector<uint64_t> h = trimmed.size() > 1024 ? resampleBins(trimmed, 1024) : trimmed;
    e.h256 = resampleBins(h, 256);
    if(h.size() != 256) e.h1024 = h;
    e.min = fmt3(0.0); e.max = fmt3((double)top);
    return e;
}

inline void checkStoredHistogram(hid_t g, const std::string& gp, const char* name, const std::vector<uint64_t>& want,
                                 std::vector<std::string>& out){
    const std::string p = gp + "/" + name;
    if(want.empty()){
        if(H5Lexists(g, name, H5P_DEFAULT) > 0) out.push_back(p + ": present, but ImarisWriter writes none for these values");
        return;
    }
    if(H5Lexists(g, name, H5P_DEFAULT) <= 0){ out.push_back(p + ": missing"); return; }
    Hid d(H5Dopen2(g, name, H5P_DEFAULT), H5Dclose);
    Hid s(H5Dget_space(d), H5Sclose);
    hssize_t n = d.valid() ? H5Sget_simple_extent_npoints(s) : -1;
    std::vector<uint64_t> got(n > 0 ? (size_t)n : 0, 0);
    if(n < 0 || (n > 0 && H5Dread(d, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, got.data()) < 0)){ out.push_back(p + ": unreadable"); return; }
    if(got.size() != want.size()){ out.push_back(p + ": " + std::to_string(got.size()) + " bins, the decoded data gives " + std::to_string(want.size())); return; }
    for(size_t k = 0; k < got.size(); ++k)
        if(got[k] != want[k]){
            out.push_back(p + ": bin " + std::to_string(k) + " holds " + std::to_string((unsigned long long)got[k]) +
                          ", the decoded data gives " + std::to_string((unsigned long long)want[k]));
            return;
        }
}

inline void checkStoredAttr(hid_t g, const std::string& gp, const std::string& name, const std::string& want,
                            std::vector<std::string>& out){
    AttrValue v;
    if(H5Aexists(g, name.c_str()) <= 0 || !readAttr(g, name, v)){ out.push_back(gp + " attribute " + name + ": missing"); return; }
    if(v.text() != want) out.push_back(gp + " attribute " + name + ": \"" + v.text() + "\", the decoded data gives \"" + want + "\"");
}

// Decodes every chunk of one Data dataset, counts its voxels inside ImageSizeX/Y/Z and
// checks them against the stored histograms.
// countsOnly is set for types whose histograms can only be checked by their totals.
inline void deepCheckDataset(hid_t f, const std::string& gp, std::vector<RawReader>& readers, ImsCompareReport& r,
                             bool& countsOnly){
    Hid g(H5Gopen2(f, gp.c_str(), H5P_DEFAULT), H5Gclose);
    if(!g.valid() || H5Lexists(g, "Data", H5P_DEFAULT) <= 0){ r.differences.push_back(gp + "/Data: missing"); return; }
    Hid d(H5Dopen2(g, "Data", H5P_DEFAULT), H5Dclose);
    if(!d.valid()){ r.differences.push_back(gp + "/Data: cannot open"); return; }
    ++r.datasets;
    const std::string dp = gp + "/Data";
    Hid t(H5Dget_type(d), H5Tclose), sp(H5Dget_space(d), H5Sclose), pl(H5Dget_create_plist(d), H5Pclose);
    AttrValue ax, ay, az;
    if(!readAttr(g, "ImageSizeX", ax) || !readAttr(g, "ImageSizeY", ay) || !readAttr(g, "ImageSizeZ", az)){
        r.differences.push_back(gp + ": ImageSizeX/Y/Z missing"); return; }
    const uint64_t sx = parseU64(ax.text()), sy = parseU64(ay.text()), sz = parseU64(az.text());
    if(H5Sget_simple_extent_ndims(sp) != 3){ r.differences.push_back(dp + ": not 3-D"); return; }
    hsize_t dims[3] = {0, 0, 0}, cd[3] = {0, 0, 0};
    H5Sget_simple_extent_dims(sp, dims, nullptr);
    if(H5Pget_layout(pl) != H5D_CHUNKED || H5Pget_chunk(pl, 3, cd) != 3 || !cd[0] || !cd[1] || !cd[2]){ r.differences.push_back(dp + ": not chunked"); return; }
    if(sx > dims[2] || sy > dims[1] || sz > dims[0]){ r.differences.push_back(gp + ": ImageSize larger than the Data dims"); return; }
    const H5T_class_t cls = H5Tget_class(t);
    const size_t es = H5Tget_size(t);
    const bool isU8 = cls == H5T_INTEGER && es == 1 && H5Tget_sign(t) == H5T_SGN_NONE;
    const bool isU16 = cls == H5T_INTEGER && es == 2 && H5Tget_sign(t) == H5T_SGN_NONE;
    const size_t nbins = isU8 ? 256 : isU16 ? 65536 : 0;   // 0: other types are only decoded and counted
    if(!nbins) countsOnly = true;

    const std::vector<Filter> fl = filters(pl);
    size_t shuffleBytes = es;
    for(const Filter& x : fl){
        if(x.id == H5Z_FILTER_SHUFFLE){ if(!x.cd.empty() && x.cd[0]) shuffleBytes = x.cd[0]; }
        else if(x.id != kFilterLZ4){ r.differences.push_back(dp + ": unsupported filter " + std::to_string((int)x.id) + ", not decoded"); return; }
    }
    std::vector<Chunk> chunks;
    if(!listChunks(d, chunks)){ r.differences.push_back(dp + ": H5Dchunk_iter failed"); return; }
    const uint64_t expected = ((sx + cd[2] - 1) / cd[2]) * ((sy + cd[1] - 1) / cd[1]) * ((sz + cd[0] - 1) / cd[0]);
    uint64_t inside = 0;
    for(const Chunk& c : chunks)
        if(c.off[0] % cd[0] == 0 && c.off[1] % cd[1] == 0 && c.off[2] % cd[2] == 0 && c.off[0] < sz && c.off[1] < sy && c.off[2] < sx) ++inside;
    if(inside != expected || chunks.size() != expected)
        r.differences.push_back(dp + ": " + std::to_string(chunks.size()) + " chunks stored (" + std::to_string(inside) +
                                " inside the image), " + std::to_string(expected) + " cover ImageSizeX/Y/Z");
    const std::string file = fileOf(d);
    if(!chunks.empty() && !rawReadAgrees(d, file, chunks[0], readers[0])){ r.error = dp + ": reading chunks at their file addresses failed"; return; }

    const size_t nt = readers.size();
    const size_t chunkBytes = (size_t)(cd[0] * cd[1] * cd[2]) * es;
    std::vector<std::vector<uint64_t>> hist(nt, std::vector<uint64_t>(nbins, 0));
    std::vector<uint64_t> voxels(nt, 0), bytes(nt, 0);
    std::vector<std::string> err(chunks.size());
    std::vector<std::vector<uint8_t>> bufA(nt), bufB(nt);
    parallelFor(chunks.size(), readers, [&](size_t k, RawReader& rd, size_t th){
        const Chunk& c = chunks[k];
        std::vector<uint8_t>& a = bufA[th];
        if(!rd.read(file, c.addr, (size_t)c.size, a)){ err[k] = "could not be read"; return; }
        bytes[th] += c.size;
        std::string e;
        if(!decodeChunk(a, bufB[th], fl, c.mask, shuffleBytes, e)){ err[k] = e; return; }
        if(a.size() != chunkBytes){ err[k] = std::to_string(a.size()) + " bytes decoded, " + std::to_string(chunkBytes) + " expected"; return; }
        if(c.off[0] >= sz || c.off[1] >= sy || c.off[2] >= sx) return;
        const size_t nx = (size_t)std::min<uint64_t>(cd[2], sx - c.off[2]);
        const size_t ny = (size_t)std::min<uint64_t>(cd[1], sy - c.off[1]);
        const size_t nz = (size_t)std::min<uint64_t>(cd[0], sz - c.off[0]);
        voxels[th] += (uint64_t)nx * ny * nz;
        if(!nbins) return;
        uint64_t* h = hist[th].data();
        for(size_t z = 0; z < nz; ++z)
            for(size_t y = 0; y < ny; ++y){
                const uint8_t* row = a.data() + (y * cd[2] + z * cd[2] * cd[1]) * es;
                if(isU8) for(size_t x = 0; x < nx; ++x) ++h[row[x]];
                else for(size_t x = 0; x < nx; ++x){ uint16_t v; std::memcpy(&v, row + 2*x, 2); ++h[v]; }
            }
    });
    uint64_t bad = 0; std::string firstBad;
    for(size_t k = 0; k < chunks.size(); ++k)
        if(!err[k].empty()){ if(!bad) firstBad = "chunk " + offStr(chunks[k].off) + ": " + err[k]; ++bad; }
    for(size_t th = 0; th < nt; ++th) r.bytes += bytes[th];
    r.chunks += chunks.size() - bad;
    if(bad){ r.differences.push_back(dp + ": " + std::to_string(bad) + " of " + std::to_string(chunks.size()) + " chunks do not decode; first: " + firstBad); return; }

    uint64_t total = 0;
    for(uint64_t v : voxels) total += v;
    if(nbins){
        std::vector<uint64_t> bins(nbins, 0);
        for(const auto& h : hist) for(size_t k = 0; k < nbins; ++k) bins[k] += h[k];
        // A dataset whose chunks are missing would count fewer voxels; that is reported above.
        ExpectedHistogram e = expectedHistogram(bins);
        checkStoredHistogram(g, gp, "Histogram", e.h256, r.differences);
        checkStoredHistogram(g, gp, "Histogram1024", e.h1024, r.differences);
        checkStoredAttr(g, gp, "HistogramMin", e.min, r.differences);
        checkStoredAttr(g, gp, "HistogramMax", e.max, r.differences);
        if(!e.h1024.empty()){
            checkStoredAttr(g, gp, "HistogramMin1024", e.min, r.differences);
            checkStoredAttr(g, gp, "HistogramMax1024", e.max, r.differences);
        }
    } else {
        // Float histograms adapt their range to the order the values arrive in; only
        // their totals can be checked.
        for(const char* name : {"Histogram", "Histogram1024"}){
            if(H5Lexists(g, name, H5P_DEFAULT) <= 0){
                if(std::strcmp(name, "Histogram") == 0) r.differences.push_back(gp + "/Histogram: missing");
                continue;
            }
            Hid hd(H5Dopen2(g, name, H5P_DEFAULT), H5Dclose);
            Hid hs(H5Dget_space(hd), H5Sclose);
            hssize_t n = hd.valid() ? H5Sget_simple_extent_npoints(hs) : -1;
            std::vector<uint64_t> got(n > 0 ? (size_t)n : 0, 0);
            if(n < 0 || (n > 0 && H5Dread(hd, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, got.data()) < 0)){
                r.differences.push_back(gp + "/" + name + ": unreadable"); continue; }
            uint64_t sum = 0;
            for(uint64_t v : got) sum += v;
            if(sum != total) r.differences.push_back(gp + "/" + name + ": counts " + std::to_string((unsigned long long)sum) +
                                                     " voxels, the image has " + std::to_string((unsigned long long)total));
        }
    }
}

} // namespace imsutil

// True if every /DataSet/ResolutionLevel r/TimePoint t/Channel c of file decodes (every
// chunk that covers ImageSizeX/Y/Z is stored, passes LZ4 and has the full chunk size)
// and its stored histograms equal the ones rebuilt from the decoded voxels: Histogram,
// Histogram1024 and HistogramMin/Max exactly for uint8 and uint16, the totals only for
// other types (noted in report.notes). End-to-end integrity without a reference file.
// report.chunks/bytes count the chunks decoded and their compressed bytes.
inline bool imsDeepCheck(const std::filesystem::path& file, ImsCompareReport& report, int threads = 1){
    using namespace imsutil;
    QuietErrors quiet;
    report = ImsCompareReport();
    Hid f = openFile(file);
    if(!f.valid()){ report.error = "cannot open " + file.string(); return false; }
    if(!exists(f, "/DataSet")){ report.error = "no /DataSet group"; return false; }
    std::vector<RawReader> readers((size_t)std::max(1, threads));
    bool countsOnly = false;
    for(const std::string& l : childNames(f, "/DataSet"))
        for(const std::string& t : childNames(f, "/DataSet/" + l))
            for(const std::string& c : childNames(f, "/DataSet/" + l + "/" + t)){
                const std::string gp = "/DataSet/" + l + "/" + t + "/" + c;
                deepCheckDataset(f, gp, readers, report, countsOnly);
                if(!report.error.empty()) return false;
            }
    if(countsOnly) report.notes.push_back("histograms of non-uint8/uint16 datasets: only their voxel totals were checked");
    return report.error.empty() && report.differences.empty() && report.datasets > 0;
}

// Prints the report: the differences and thumbnail differences (at most maxLines in
// all), the notes, and a one-line summary (deep: worded for an imsDeepCheck report).
inline void imsPrintReport(const ImsCompareReport& r, FILE* out = stdout, size_t maxLines = 40, bool deep = false){
    if(!r.error.empty()) std::fprintf(out, "  ERROR: %s\n", r.error.c_str());
    size_t shown = 0;
    for(const std::string& d : r.differences) if(shown++ < maxLines) std::fprintf(out, "  DIFFERS: %s\n", d.c_str());
    for(const std::string& d : r.thumbnail) if(shown++ < maxLines) std::fprintf(out, "  DIFFERS (thumbnail): %s\n", d.c_str());
    if(shown > maxLines) std::fprintf(out, "  ... and %zu more differences\n", shown - maxLines);
    for(const std::string& n : r.notes) std::fprintf(out, "  note: %s\n", n.c_str());
    const char* verdict = deep ? (r.identical() ? "clean" : "PROBLEMS") :
                          r.identical() ? "identical" : r.identicalExceptThumbnail() ? "identical except /Thumbnail" : "DIFFERENT";
    std::fprintf(out, "  %s: %d Data datasets, %llu chunks, %llu compressed bytes %s\n", verdict, r.datasets,
                 (unsigned long long)r.chunks, (unsigned long long)r.bytes, deep ? "decoded" : "compared");
}

// ------------------------------------------------------------------------------------
// The metadata of a file
// ------------------------------------------------------------------------------------

// What /DataSetInfo should hold. Strings are the attribute texts; an empty wavelength means
// the attribute must be absent.
struct ImsExpectedMeta {
    uint64_t size[3] = {0, 0, 0};                       // X, Y, Z
    float voxel[3] = {1, 1, 1};
    std::vector<std::string> names, colors, emission, excitation;   // per channel
    std::vector<std::string> times;                     // per time point
    std::string creator = "parallelimariswriter", version;
};

// Checks every attribute of the root and of every /DataSetInfo group: the set of names
// exactly, and every value (ColorRange only for its "<min> <max>" form; it is computed
// from the data). Appends one line per problem; true if there is none.
inline bool imsCheckDataSetInfo(const std::filesystem::path& file, const ImsExpectedMeta& e, std::vector<std::string>& problems){
    using namespace imsutil;
    const size_t before = problems.size();
    auto fixed = [](float v, int p){ std::ostringstream o; o.setf(std::ios::fixed); o << std::setprecision(p) << v; return o.str(); };
    auto expect = [&](const std::string& group, const std::map<std::string, std::string>& want, const std::set<std::string>& anyValue){
        const std::map<std::string, std::string> got = imsReadAttributes(file, group);
        for(const auto& w : want){
            auto g = got.find(w.first);
            if(g == got.end()) problems.push_back(group + " " + w.first + ": missing (expected \"" + w.second + "\")");
            else if(!anyValue.count(w.first) && g->second != w.second)
                problems.push_back(group + " " + w.first + ": \"" + g->second + "\", expected \"" + w.second + "\"");
        }
        for(const auto& g : got) if(!want.count(g.first)) problems.push_back(group + " " + g.first + ": unexpected (\"" + g.second + "\")");
    };
    {
        QuietErrors quiet;
        Hid f = openFile(file);
        if(!f.valid()){ problems.push_back("cannot open " + file.string()); return false; }
        std::vector<std::string> groups = childNames(f, "/DataSetInfo"), want = {"Image", "ImarisDataSet", "Log", "TimeInfo"};
        for(size_t c = 0; c < e.names.size(); ++c) want.push_back("Channel " + std::to_string(c));
        std::sort(want.begin(), want.end());
        if(groups != want){
            std::string g; for(const std::string& n : groups) g += " [" + n + "]";
            problems.push_back("/DataSetInfo groups:" + g);
        }
    }
    expect("/", {{"ImarisDataSet", "ImarisDataSet"}, {"ImarisVersion", "5.5.0"}, {"DataSetInfoDirectoryName", "DataSetInfo"},
                 {"ThumbnailDirectoryName", "Thumbnail"}, {"DataSetDirectoryName", "DataSet"}, {"NumberOfDataSets", ""}},
           {"NumberOfDataSets"});
    std::map<std::string, std::string> image = {{"Name", "(name not specified)"}, {"Description", "(description not specified)"}, {"Unit", "um"},
        {"ResampleDimensionX", "true"}, {"ResampleDimensionY", "true"}, {"ResampleDimensionZ", "true"},
        {"RecordingDate", e.times.empty() ? std::string() : e.times[0]}};
    for(int a = 0; a < 3; ++a){
        image["ExtMin" + std::to_string(a)] = fixed(0.f, 9);
        image["ExtMax" + std::to_string(a)] = fixed((float)e.size[a] * e.voxel[a], 9);
        image[std::string(1, (char)('X' + a))] = std::to_string(e.size[a]);
    }
    expect("/DataSetInfo/Image", image, {});
    for(size_t c = 0; c < e.names.size(); ++c){
        std::map<std::string, std::string> ch = {{"Name", e.names[c]}, {"Description", "(description not specified)"}, {"ColorMode", "BaseColor"},
            {"Color", e.colors[c]}, {"ColorRange", ""}, {"ColorOpacity", "0.000"}, {"GammaCorrection", "1.000"}};
        if(c < e.emission.size() && !e.emission[c].empty()) ch["LSMEmissionWavelength"] = e.emission[c];
        if(c < e.excitation.size() && !e.excitation[c].empty()) ch["LSMExcitationWavelength"] = e.excitation[c];
        const std::string g = "/DataSetInfo/Channel " + std::to_string(c);
        expect(g, ch, {"ColorRange"});
        const std::string range = imsReadAttribute(file, g, "ColorRange").value_or("");
        float lo = 0, hi = 0; char rest = 0;
        if(std::sscanf(range.c_str(), "%f %f%c", &lo, &hi, &rest) != 2 || !(lo <= hi))
            problems.push_back(g + " ColorRange: \"" + range + "\" is not \"<min> <max>\"");
    }
    std::map<std::string, std::string> ti = {{"FileTimePoints", std::to_string(e.times.size())}, {"DatasetTimePoints", std::to_string(e.times.size())}};
    for(size_t t = 0; t < e.times.size(); ++t) ti["TimePoint" + std::to_string(t + 1)] = e.times[t];
    expect("/DataSetInfo/TimeInfo", ti, {});
    expect("/DataSetInfo/ImarisDataSet", {{"NumberOfImages", "1"}, {"Creator", e.creator}, {"Version", e.version}}, {});
    expect("/DataSetInfo/Log", {{"Entries", "0"}}, {});
    return problems.size() == before;
}
