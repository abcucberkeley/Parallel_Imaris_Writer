// Shared .ims helpers for the tests (header-only, C++17, HDF5 C library + lz4.h).
//
// An Imaris 5.5 .ims file is HDF5: /DataSet/ResolutionLevel r/TimePoint t/Channel c
// holds a chunked 3-D Data dataset (dims z,y,x) next to its Histogram datasets, and
// /DataSetInfo holds the metadata as string attributes. ImarisWriter compresses every
// chunk itself (byte shuffle for 2- and 4-byte integers, then LZ4 in the HDF5 lz4
// filter format, id 32004) and writes it with H5Dwrite_chunk, so these helpers read
// raw chunks with H5Dread_chunk and undo the filters here; no HDF5 filter plugin is
// needed.
//
//   imsReadVolume(file, level, t, c, info)  -> the voxels of one dataset, x fastest,
//                                              cropped to ImageSizeX/Y/Z
//   imsCompare(a, b, report[, options])     -> true if the two files hold the same
//                                              chunks (compressed bytes), histograms
//                                              and attributes
//   imsReadAttribute / imsReadHistogram / imsExists  -> small accessors for checks
//
// Only portable calls (no pread, no OpenMP): it builds the same on Linux, macOS and
// Windows. Chunks are listed with one H5Dchunk_iter pass per dataset, never with
// H5Dget_chunk_info by index (which walks the chunk index from the start each time).
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <set>
#include <string>
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
    for(size_t b = 0; b < es; ++b)
        for(size_t k = 0; k < n; ++k) out[k*es + b] = in[b*n + k];
    std::copy(in.begin() + (std::ptrdiff_t)(n*es), in.end(), out.begin() + (std::ptrdiff_t)(n*es));
}

struct Chunk { hsize_t off[3]; unsigned mask; hsize_t size; };
inline int chunkCb(const hsize_t* off, unsigned mask, haddr_t, hsize_t size, void* ud){
    Chunk c; c.off[0] = off[0]; c.off[1] = off[1]; c.off[2] = off[2]; c.mask = mask; c.size = size;
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
        // Undo the pipeline in reverse; bit i of the mask set means filter i was skipped.
        a.swap(raw);
        for(int i = (int)fl.size() - 1; i >= 0; --i){
            if(c.mask & (1u << i)) continue;
            std::string err;
            if(fl[i].id == kFilterLZ4){
                if(!lz4Decode(a, b, err)){ info.error = gp + " chunk " + offStr(c.off) + ": " + err; vol.clear(); return vol; }
            } else {
                if(shuffleBytes > 1) unshuffle(a, b, shuffleBytes); else b = a;
            }
            a.swap(b);
        }
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

// ------------------------------------------------------------------------------------
// Comparing two files
// ------------------------------------------------------------------------------------

struct ImsCompareOptions {
    bool histograms = true;         // the datasets next to Data (Histogram, Histogram1024)
    bool datasetAttributes = true;  // attributes of the /DataSet/.../Channel c groups (ImageSize, HistogramMin/Max)
    bool dataSetInfo = true;        // root attributes and every /DataSetInfo group's attributes
    bool thumbnail = true;          // /Thumbnail/Data present in both or in neither
};

struct ImsCompareReport {
    std::vector<std::string> differences;  // one line each, starting with the object's path
    std::string error;                     // a file could not be opened
    int datasets = 0;                      // Data datasets compared
    uint64_t chunks = 0;                   // chunks compared
    uint64_t bytes = 0;                    // compressed bytes compared
};

namespace imsutil {

inline void compareAttrs(hid_t fa, hid_t fb, const std::string& path, ImsCompareReport& r){
    Hid oa(H5Oopen(fa, path.c_str(), H5P_DEFAULT), H5Oclose), ob(H5Oopen(fb, path.c_str(), H5P_DEFAULT), H5Oclose);
    if(!oa.valid() || !ob.valid()){ r.differences.push_back(path + ": cannot open"); return; }
    std::vector<std::string> na = attrNames(oa), nb = attrNames(ob);
    std::set<std::string> all(na.begin(), na.end());
    all.insert(nb.begin(), nb.end());
    for(const std::string& n : all){
        bool inA = std::binary_search(na.begin(), na.end(), n), inB = std::binary_search(nb.begin(), nb.end(), n);
        if(!inA || !inB){ r.differences.push_back(path + " attribute " + n + ": only in " + (inA ? "a" : "b")); continue; }
        AttrValue va, vb;
        if(!readAttr(oa, n, va) || !readAttr(ob, n, vb)){ r.differences.push_back(path + " attribute " + n + ": unreadable"); continue; }
        if(!(va == vb)) r.differences.push_back(path + " attribute " + n + ": a=\"" + va.text() + "\" b=\"" + vb.text() + "\"");
    }
}

inline void compareData(hid_t fa, hid_t fb, const std::string& path, ImsCompareReport& r){
    Hid da(H5Dopen2(fa, path.c_str(), H5P_DEFAULT), H5Dclose), db(H5Dopen2(fb, path.c_str(), H5P_DEFAULT), H5Dclose);
    if(!da.valid() || !db.valid()){ r.differences.push_back(path + ": cannot open"); return; }
    ++r.datasets;
    Hid ta(H5Dget_type(da), H5Tclose), tb(H5Dget_type(db), H5Tclose);
    Hid sa(H5Dget_space(da), H5Sclose), sb(H5Dget_space(db), H5Sclose);
    Hid pa(H5Dget_create_plist(da), H5Pclose), pb(H5Dget_create_plist(db), H5Pclose);
    if(H5Tequal(ta, tb) <= 0){ r.differences.push_back(path + ": element types differ"); return; }
    if(H5Sget_simple_extent_ndims(sa) != 3 || H5Sget_simple_extent_ndims(sb) != 3){ r.differences.push_back(path + ": not 3-D"); return; }
    hsize_t dimA[3], dimB[3], cdA[3] = {0, 0, 0}, cdB[3] = {0, 0, 0};
    H5Sget_simple_extent_dims(sa, dimA, nullptr); H5Sget_simple_extent_dims(sb, dimB, nullptr);
    if(!std::equal(dimA, dimA + 3, dimB)){ r.differences.push_back(path + ": dims a=" + offStr(dimA) + " b=" + offStr(dimB)); return; }
    H5Pget_chunk(pa, 3, cdA); H5Pget_chunk(pb, 3, cdB);
    if(!std::equal(cdA, cdA + 3, cdB)){ r.differences.push_back(path + ": chunk dims a=" + offStr(cdA) + " b=" + offStr(cdB)); return; }
    std::vector<Filter> fla = filters(pa), flb = filters(pb);
    bool sameFilters = fla.size() == flb.size();
    for(size_t i = 0; sameFilters && i < fla.size(); ++i) sameFilters = fla[i].id == flb[i].id && fla[i].cd == flb[i].cd;
    if(!sameFilters){ r.differences.push_back(path + ": filter pipelines differ"); return; }

    std::vector<Chunk> ca, cb;
    if(!listChunks(da, ca) || !listChunks(db, cb)){ r.differences.push_back(path + ": H5Dchunk_iter failed"); return; }
    uint64_t onlyA = 0, onlyB = 0, differ = 0;
    std::string firstDiff;
    std::vector<uint8_t> ba, bb;
    size_t i = 0, j = 0;
    while(i < ca.size() || j < cb.size()){
        if(j == cb.size() || (i < ca.size() && chunkLess(ca[i], cb[j]))){
            if(firstDiff.empty()) firstDiff = "chunk " + offStr(ca[i].off) + " only in a";
            ++onlyA; ++i; continue;
        }
        if(i == ca.size() || chunkLess(cb[j], ca[i])){
            if(firstDiff.empty()) firstDiff = "chunk " + offStr(cb[j].off) + " only in b";
            ++onlyB; ++j; continue;
        }
        const Chunk& x = ca[i++]; const Chunk& y = cb[j++];
        ++r.chunks; r.bytes += x.size;
        bool same = x.size == y.size && x.mask == y.mask && readRawChunk(da, x, ba) && readRawChunk(db, y, bb) && ba == bb;
        if(!same){
            ++differ;
            if(firstDiff.empty()) firstDiff = "chunk " + offStr(x.off) + " differs (" + std::to_string((unsigned long long)x.size) +
                                              " vs " + std::to_string((unsigned long long)y.size) + " bytes)";
        }
    }
    if(onlyA || onlyB || differ)
        r.differences.push_back(path + ": " + std::to_string(ca.size()) + " vs " + std::to_string(cb.size()) + " chunks, " +
                                std::to_string(differ) + " differ, " + std::to_string(onlyA) + " only in a, " +
                                std::to_string(onlyB) + " only in b; first: " + firstDiff);
}

inline void compareSmallDataset(hid_t fa, hid_t fb, const std::string& path, ImsCompareReport& r){
    Hid da(H5Dopen2(fa, path.c_str(), H5P_DEFAULT), H5Dclose), db(H5Dopen2(fb, path.c_str(), H5P_DEFAULT), H5Dclose);
    std::vector<char> va, vb; size_t sa = 0, sb = 0;
    if(!da.valid() || !db.valid() || !readDatasetBytes(da, va, sa) || !readDatasetBytes(db, vb, sb)){
        r.differences.push_back(path + ": unreadable"); return; }
    if(sa != sb || va != vb) r.differences.push_back(path + ": values differ");
}

// Compares the members of one /DataSet/.../Channel c group.
inline void compareChannelGroup(hid_t fa, hid_t fb, const std::string& gp, const ImsCompareOptions& o, ImsCompareReport& r){
    if(o.datasetAttributes) compareAttrs(fa, fb, gp, r);
    std::vector<std::string> ma = childNames(fa, gp), mb = childNames(fb, gp);
    std::set<std::string> all(ma.begin(), ma.end());
    all.insert(mb.begin(), mb.end());
    for(const std::string& m : all){
        const std::string p = gp + "/" + m;
        bool inA = std::binary_search(ma.begin(), ma.end(), m), inB = std::binary_search(mb.begin(), mb.end(), m);
        if(m == "Data"){
            if(!inA || !inB) r.differences.push_back(p + ": only in " + std::string(inA ? "a" : "b"));
            else compareData(fa, fb, p, r);
        } else if(o.histograms){
            if(!inA || !inB) r.differences.push_back(p + ": only in " + std::string(inA ? "a" : "b"));
            else compareSmallDataset(fa, fb, p, r);
        }
    }
}

// Walks /DataSet down to depth 3 (ResolutionLevel / TimePoint / Channel) in both files.
inline void walkDataSet(hid_t fa, hid_t fb, const std::string& path, int depth, const ImsCompareOptions& o, ImsCompareReport& r){
    if(depth == 3){ compareChannelGroup(fa, fb, path, o, r); return; }
    std::vector<std::string> na = childNames(fa, path), nb = childNames(fb, path);
    std::set<std::string> all(na.begin(), na.end());
    all.insert(nb.begin(), nb.end());
    for(const std::string& n : all){
        bool inA = std::binary_search(na.begin(), na.end(), n), inB = std::binary_search(nb.begin(), nb.end(), n);
        if(!inA || !inB){ r.differences.push_back(path + "/" + n + ": only in " + (inA ? "a" : "b")); continue; }
        walkDataSet(fa, fb, path + "/" + n, depth + 1, o, r);
    }
}

} // namespace imsutil

// True if a and b hold the same data: for every /DataSet/ResolutionLevel r/TimePoint t/
// Channel c the same chunk set with the same compressed bytes, the same Histogram /
// Histogram1024 values and the same attributes; the same root and /DataSetInfo/*
// attributes (Image, Channel N, TimeInfo, ImarisDataSet, ...); /Thumbnail/Data in both
// or in neither. Options switch off the parts other than the chunks. The file layout
// (chunk addresses, object order) is not compared. report lists every difference.
inline bool imsCompare(const std::filesystem::path& a, const std::filesystem::path& b, ImsCompareReport& report,
                       const ImsCompareOptions& options = ImsCompareOptions()){
    using namespace imsutil;
    QuietErrors quiet;
    report = ImsCompareReport();
    Hid fa = openFile(a), fb = openFile(b);
    if(!fa.valid() || !fb.valid()){ report.error = "cannot open " + (fa.valid() ? b : a).string(); return false; }
    if(!exists(fa, "/DataSet") || !exists(fb, "/DataSet")){ report.error = "no /DataSet group"; return false; }
    walkDataSet(fa, fb, "/DataSet", 0, options, report);
    if(options.dataSetInfo){
        compareAttrs(fa, fb, "/", report);
        std::vector<std::string> ga = childNames(fa, "/DataSetInfo"), gb = childNames(fb, "/DataSetInfo");
        std::set<std::string> all(ga.begin(), ga.end());
        all.insert(gb.begin(), gb.end());
        for(const std::string& g : all){
            bool inA = std::binary_search(ga.begin(), ga.end(), g), inB = std::binary_search(gb.begin(), gb.end(), g);
            if(!inA || !inB) report.differences.push_back("/DataSetInfo/" + g + ": only in " + (inA ? "a" : "b"));
            else compareAttrs(fa, fb, "/DataSetInfo/" + g, report);
        }
    }
    if(options.thumbnail){
        bool ta = exists(fa, "/Thumbnail/Data"), tb = exists(fb, "/Thumbnail/Data");
        if(ta != tb) report.differences.push_back(std::string("/Thumbnail/Data: only in ") + (ta ? "a" : "b"));
    }
    return report.error.empty() && report.differences.empty() && report.datasets > 0;
}

// Prints the report: the differences (at most maxLines) and a one-line summary.
inline void imsPrintReport(const ImsCompareReport& r, FILE* out = stdout, size_t maxLines = 40){
    if(!r.error.empty()) std::fprintf(out, "  ERROR: %s\n", r.error.c_str());
    for(size_t i = 0; i < r.differences.size() && i < maxLines; ++i)
        std::fprintf(out, "  DIFFERS: %s\n", r.differences[i].c_str());
    if(r.differences.size() > maxLines)
        std::fprintf(out, "  ... and %zu more differences\n", r.differences.size() - maxLines);
    std::fprintf(out, "  %s: %d Data datasets, %llu chunks, %llu compressed bytes compared\n",
                 (r.error.empty() && r.differences.empty() && r.datasets > 0) ? "identical" : "DIFFERENT",
                 r.datasets, (unsigned long long)r.chunks, (unsigned long long)r.bytes);
}
