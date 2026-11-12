// Library-API test.
//
// piw::convert (src/converter.cpp, linked in from the core objects like the executable) must
// write the same .ims as the shipped executable for the same inputs, and must fail with a
// Status -- the process alive and no partial output left -- where the command line used to
// crash. Inputs are generated here (no committed data):
//   - piw::convert against the executable: TIFF and Zarr, whole and with a processing block
//     (-C), and 2 channels x 2 time points: the raw compressed chunks of every Data dataset
//     (all levels, time points, channels) and the per-channel DataSetInfo must be equal
//   - error returns: a .zarr folder without metadata, a truncated chunk, inputs of different
//     sizes (refused before any output), channels with different numbers of files, a pattern
//     without a match; the command line exits 1 (not a crash) for the cases that used to crash
//   - cancel after a few polls; progress never decreasing and ending at 1
//   - two conversions one after the other, two at once from two threads, numThreads (and the
//     caller's OpenMP setting restored), piw::runCli twice in one process
//   - metadata (A-10): every /DataSetInfo attribute with the defaults (names "Channel c", R/G/B,
//     no wavelengths, 1 s apart from the first input's modification time), with the options
//     (names, colors, wavelengths, start time, interval), from "<n>msecAbs" stamps in the file
//     names; the same options as command-line flags, or given to writeVolumes with the same data,
//     give the same file; writeVolumes without a start time takes the current time; bad metadata
//     options are refused before any output; imsmeta.h's time and color helpers
//   - piw::writeVolumes and the read-back (piw::imsInfo, piw::readIms): uint8, uint16, uint32
//     and float32 volumes of odd sizes, one plane and smaller than a block read back equal to
//     the source at every level (and equal to tests/imsutil.h's independent decoder); the same
//     data in other memory layouts ((z, y, x) C order, z fastest, gaps) and with other block
//     sizes gives the same file (CRCs, histograms, attributes, thumbnail); writeVolumes equals
//     convert of Zarrs of the same data (2 channels x 2 time points); level 1 is the 2x2x2 mean;
//     region reads into strided outputs; error returns
//   - xOnImarisX: the image's x on Imaris X, for Zarr and TIFF, equals writeVolumes of the
//     transposed data
//   - bounding boxes on TIFF input (-B, through the API and the executable, whole and with -C)
//     give the cropped input, the same file as on a Zarr of the same data
//   usage: apiTest <path-to-parallelimariswriter> [workdir]
//          apiTest --tiff <file.tif> <workdir> [bx,by,bz]   (timing: writeVolumes of a TIFF from
//          its (y, x, z) buffer and from a (z, y, x) C-contiguous copy; the files are left there)
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include <sys/stat.h>
#include <ctime>
#include <omp.h>
#include "parallelimariswriter.h"
#include "imsmeta.h"             // src/: the metadata helpers both conversion paths use
#include "imsutil.h"             // independent .ims decoder (imsReadVolume), imsCheckDataSetInfo, imsCompare
#include "helperfunctions.h"     // cpp-tiff's (getImageSize, getDataType): its folder comes first
#include "parallelreadtiff.h"    // cpp-tiff
#include "parallelwritetiff.h"
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "hdf5.h"                // core HDF5 (statically linked)

namespace fs = std::filesystem;

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    std::printf("  %s: %s\n", ok ? "ok" : "FAIL", what.c_str());
    std::fflush(stdout);
    if(!ok) ++gFailures;
}

static std::string native(const fs::path& p){ return fs::path(p).make_preferred().string(); }

// --- inputs ---

static std::vector<uint16_t> makeVolume(uint64_t X, uint64_t Y, uint64_t Z, int seed){
    std::vector<uint16_t> buf(X*Y*Z);
    // value varies in all three axes so any transpose/offset error corrupts it
    for(uint64_t z=0; z<Z; z++)
      for(uint64_t y=0; y<Y; y++)
        for(uint64_t x=0; x<X; x++)
          buf[x + y*X + z*X*Y] = (uint16_t)((x*7 + y*13 + z*17 + seed*137) % 4096 + 1);
    return buf;
}

// A uint16 Zarr of buf (X fastest: dims 0, 1, 2 = X, Y, Z).
static void writeZarrBuffer(const fs::path& path, std::vector<uint16_t> buf, uint64_t X, uint64_t Y, uint64_t Z){
    std::vector<uint64_t> shape={X,Y,Z}, chunks={32,32,32};   // 32^3 so -C 32,32,32 splits every axis
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0,0,0}, shape);
    if(parallelWriteZarr(Zw, buf.data(), {0,0,0}, shape, shape, 16, false, false, false)){
        std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); std::exit(1); }
}

static void writeZarr(const fs::path& path, uint64_t X, uint64_t Y, uint64_t Z, int seed){
    writeZarrBuffer(path, makeVolume(X, Y, Z, seed), X, Y, Z);
}

static void writeTiff(const fs::path& path, uint64_t X, uint64_t Y, uint64_t Z, int seed){
    std::vector<uint16_t> buf = makeVolume(X, Y, Z, seed);
    if(writeTiffParallelWrapper(X,Y,Z,path.string().c_str(), buf.data(),16,0,8,"w",false,std::string("none"),1)){
        std::fprintf(stderr,"TIFF write failed\n"); std::exit(1); }
}

// --- the executable ---

static int exitStatus(int rc){
#ifdef _WIN32
    return rc;
#else
    if(rc == -1) return -1;
    if(WIFEXITED(rc)) return WEXITSTATUS(rc);
    return 128 + WTERMSIG(rc);   // died from a signal
#endif
}

// Runs the executable with args, its output going to log; returns its exit status.
static int runExe(const std::string& exe, const std::string& args, const fs::path& log){
    std::string cmd = "\"" + native(exe) + "\" " + args + " > \"" + native(log) + "\" 2>&1";
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    return exitStatus(std::system(cmd.c_str()));
}

static std::string readText(const fs::path& p){
    std::ifstream in(p);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

// --- .ims comparison: raw compressed chunks of every Data dataset (all resolution levels,
// time points and channels) + per-channel DataSetInfo; as tests/paralleltest.cpp ---

static bool sameData(const fs::path& fa, const fs::path& fb){
    H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){
        std::printf("  cannot open %s or %s\n", fa.string().c_str(), fb.string().c_str());
        if(a >= 0) H5Fclose(a);
        if(b >= 0) H5Fclose(b);
        return false;
    }
    bool ok = true; int ndsets = 0; long nchunks = 0;
    for(int r=0; ok; ++r){
        bool foundLevel = false;
        for(int t=0; ok; ++t){
            bool foundTp = false;
            for(int c=0; ok; ++c){
                char path[512];
                std::snprintf(path, sizeof(path), "/DataSet/ResolutionLevel %d/TimePoint %d/Channel %d/Data", r, t, c);
                hid_t da = H5Dopen2(a, path, H5P_DEFAULT);
                if(da < 0) break;                 // no more channels here
                foundLevel = foundTp = true; ++ndsets;
                hid_t db = H5Dopen2(b, path, H5P_DEFAULT);
                if(db < 0){ std::printf("  DIFFERS: %s missing in second file\n", path); ok=false; H5Dclose(da); break; }
                hid_t sa = H5Dget_space(da), sb = H5Dget_space(db);
                hsize_t nch = 0, nchb = 0; H5Dget_num_chunks(da, sa, &nch); H5Dget_num_chunks(db, sb, &nchb);
                if(nch != nchb){ std::printf("  DIFFERS: %s chunk count %llu vs %llu\n", path,
                    (unsigned long long)nch, (unsigned long long)nchb); ok=false; }
                for(hsize_t i=0; i<nch && ok; ++i){
                    hsize_t off[8]; unsigned fma; haddr_t addra; hsize_t sza;
                    H5Dget_chunk_info(da, sa, i, off, &fma, &addra, &sza);
                    hsize_t szb; unsigned fmb; haddr_t addrb;
                    if(H5Dget_chunk_info_by_coord(db, off, &fmb, &addrb, &szb) < 0 || szb != sza){
                        std::printf("  DIFFERS: %s chunk %llu size/coord\n", path, (unsigned long long)i); ok=false; break;
                    }
                    std::vector<char> ba(sza), bb(szb); unsigned fm;
                    H5Dread_chunk(da, H5P_DEFAULT, off, &fm, ba.data());
                    H5Dread_chunk(db, H5P_DEFAULT, off, &fm, bb.data());
                    if(ba != bb){ std::printf("  DIFFERS: %s chunk %llu bytes\n", path, (unsigned long long)i); ok=false; break; }
                    ++nchunks;
                }
                H5Sclose(sa); H5Sclose(sb); H5Dclose(da); H5Dclose(db);
            }
            if(!foundTp) break;
        }
        if(!foundLevel) break;
    }
    H5Fclose(a); H5Fclose(b);
    if(ok) std::printf("  identical data (%d Data dsets, %ld chunks)\n", ndsets, nchunks);
    return ok && ndsets > 0;
}

// .ims string attributes are a 1-D array of H5T_C_S1 (size-1) elements (no NUL).
static std::string readStrAttr(hid_t grp, const char* name){
    if(H5Aexists(grp, name) <= 0) return "<absent>";
    hid_t at = H5Aopen(grp, name, H5P_DEFAULT);
    hid_t tp = H5Aget_type(at); hid_t sp = H5Aget_space(at);
    hssize_t n = H5Sget_simple_extent_npoints(sp); if(n < 0) n = 0;
    size_t bytes = (size_t)n * H5Tget_size(tp);
    std::vector<char> buf(bytes + 1, 0);
    H5Aread(at, tp, buf.data());
    H5Sclose(sp); H5Tclose(tp); H5Aclose(at);
    return std::string(buf.data(), bytes);
}

static bool sameChannelInfo(const fs::path& fa, const fs::path& fb){
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){
        if(a >= 0) H5Fclose(a);
        if(b >= 0) H5Fclose(b);
        return false;
    }
    static const char* KEYS[] = {"Name","Description","ColorMode","Color","ColorRange",
        "ColorOpacity","GammaCorrection","LSMEmissionWavelength","OtherChannelParameter"};
    bool ok = true; int c = 0;
    for(;;){
        std::string g = "/DataSetInfo/Channel " + std::to_string(c);
        htri_t ea = H5Lexists(a, g.c_str(), H5P_DEFAULT), eb = H5Lexists(b, g.c_str(), H5P_DEFAULT);
        if(ea <= 0 && eb <= 0) break;
        if(ea <= 0 || eb <= 0){ std::printf("  DIFFERS: Channel %d present in only one file\n", c); ok=false; break; }
        hid_t ga = H5Gopen2(a, g.c_str(), H5P_DEFAULT), gb = H5Gopen2(b, g.c_str(), H5P_DEFAULT);
        for(const char* k : KEYS){
            std::string va = readStrAttr(ga, k), vb = readStrAttr(gb, k);
            if(va != vb){ std::printf("  DIFFERS: Channel %d %s: %s vs %s\n", c, k, va.c_str(), vb.c_str()); ok=false; }
        }
        H5Gclose(ga); H5Gclose(gb); ++c;
    }
    H5Fclose(a); H5Fclose(b);
    return ok && c > 0;
}

static bool sameIms(const fs::path& fa, const fs::path& fb){
    return sameData(fa, fb) && sameChannelInfo(fa, fb);
}

// --- piw::convert with recording callbacks ---

struct Recorder {
    std::mutex mutex;
    std::vector<float> progress;
    std::string text;
    std::atomic<int> polls{0};
    int cancelOnPoll = 0;            // 0: never
};
static void onMessage(const char* text, void* user){
    Recorder* r = (Recorder*)user; std::lock_guard<std::mutex> lock(r->mutex); r->text += text;
}
static void onProgress(float fraction, unsigned long long, void* user){
    Recorder* r = (Recorder*)user; std::lock_guard<std::mutex> lock(r->mutex); r->progress.push_back(fraction);
}
static int onCancel(void* user){
    Recorder* r = (Recorder*)user; int n = ++r->polls; return r->cancelOnPoll > 0 && n >= r->cancelOnPoll;
}

static piw::Callbacks callbacksFor(Recorder& r){
    piw::Callbacks cb;
    cb.message = onMessage; cb.progress = onProgress; cb.cancel = onCancel; cb.user = &r;
    return cb;
}

// names: the channel names (the executable names the channels after their -P patterns).
static piw::ConvertOptions optionsFor(const std::string& reader, const std::vector<std::vector<std::string>>& files,
                                      const fs::path& out, const std::string& processBlock,
                                      const std::vector<std::string>& names = std::vector<std::string>()){
    piw::ConvertOptions o;
    o.reader = reader;
    o.files = files;
    o.channelNames = names;
    o.outputFile = native(out);
    o.voxelSize[0] = o.voxelSize[1] = o.voxelSize[2] = 1.0f;   // the executable runs with -v 1,1,1
    if(!processBlock.empty()){
        o.hasProcessBlock = true;
        std::sscanf(processBlock.c_str(), "%llu,%llu,%llu", (unsigned long long*)&o.processBlock[0],
                    (unsigned long long*)&o.processBlock[1], (unsigned long long*)&o.processBlock[2]);
    }
    return o;
}

static std::vector<std::vector<std::string>> findFiles(const fs::path& dir, const std::vector<std::string>& patterns){
    std::vector<std::vector<std::string>> files;
    piw::Status s = piw::findInputFiles({native(dir)}, patterns, files);
    if(!s.ok()){ std::printf("  findInputFiles failed: %s\n", s.message.c_str()); ++gFailures; }
    return files;
}

static bool progressOk(const Recorder& r){
    if(r.progress.empty()) return false;
    for(size_t i = 1; i < r.progress.size(); ++i) if(r.progress[i] < r.progress[i-1]) return false;
    return r.progress.back() == 1.0f;
}

static size_t countLines(const std::string& text, const std::string& prefix){
    size_t n = 0, pos = 0;
    while((pos = text.find(prefix, pos)) != std::string::npos){ if(pos == 0 || text[pos-1] == '\n') ++n; pos += prefix.size(); }
    return n;
}

// --- in-memory volumes (piw::writeVolumes) and read-back (piw::imsInfo, piw::readIms) ---

template<typename T> static piw::DataType typeOf();
template<> piw::DataType typeOf<uint8_t>(){ return piw::DataType::UInt8; }
template<> piw::DataType typeOf<uint16_t>(){ return piw::DataType::UInt16; }
template<> piw::DataType typeOf<uint32_t>(){ return piw::DataType::UInt32; }
template<> piw::DataType typeOf<float>(){ return piw::DataType::Float32; }
template<typename T> static const char* typeName(){
    return std::is_same<T, uint8_t>::value ? "uint8" : std::is_same<T, uint16_t>::value ? "uint16" :
           std::is_same<T, uint32_t>::value ? "uint32" : "float32";
}

// A value that differs along every axis (uint32 well above 65535, float with a fraction).
template<typename T> static T sample(uint64_t x, uint64_t y, uint64_t z, int seed){
    const uint64_t k = x*7 + y*13 + z*17 + (uint64_t)seed*137;
    if(std::is_same<T, uint8_t>::value) return (T)(k % 251 + 1);
    if(std::is_same<T, uint16_t>::value) return (T)(k % 4096 + 1);
    if(std::is_same<T, uint32_t>::value) return (T)(70000 + (x*7919 + y*104729 + z*1299709 + (uint64_t)seed*15485863) % 100000);
    return (T)((float)(k % 4096) * 0.25f + 0.5f);
}

struct Dims { uint64_t x, y, z; uint64_t n() const { return x*y*z; } };

// X fastest (the layout of the readers' buffers).
template<typename T> static std::vector<T> makeTyped(const Dims& d, int seed){
    std::vector<T> v(d.n());
    for(uint64_t z = 0; z < d.z; z++)
        for(uint64_t y = 0; y < d.y; y++)
            for(uint64_t x = 0; x < d.x; x++) v[x + y*d.x + z*d.x*d.y] = sample<T>(x, y, z, seed);
    return v;
}

// The same voxels in a buffer with element strides s along X, Y, Z (gaps hold a marker value).
template<typename T> static std::vector<T> relayout(const std::vector<T>& v, const Dims& d, const int64_t s[3]){
    std::vector<T> out((size_t)((d.x - 1)*s[0] + (d.y - 1)*s[1] + (d.z - 1)*s[2] + 1), (T)77);
    for(uint64_t z = 0; z < d.z; z++)
        for(uint64_t y = 0; y < d.y; y++)
            for(uint64_t x = 0; x < d.x; x++) out[x*s[0] + y*s[1] + z*s[2]] = v[x + y*d.x + z*d.x*d.y];
    return out;
}

// The start time of every writeVolumes call here and of the conversions they are compared with
// (without one, writeVolumes takes the current time and a conversion its first input's).
static const char* const kStartTime = "2021-03-04 05:06:07";

static piw::WriteOptions writeOptions(const fs::path& out, piw::DataType type, const Dims& d, unsigned channels = 1,
                                      unsigned timepoints = 1){
    piw::WriteOptions o;
    o.startTime = kStartTime;
    o.outputFile = native(out);
    o.dataType = type;
    o.size[0] = d.x; o.size[1] = d.y; o.size[2] = d.z;
    o.channels = channels; o.timepoints = timepoints;
    o.voxelSize[0] = 0.5f; o.voxelSize[1] = 0.25f; o.voxelSize[2] = 2.0f;
    return o;
}

template<typename T> static piw::Status writeOne(const fs::path& out, const Dims& d, const T* data, const int64_t s[3],
                                                 const uint64_t* block = nullptr, Recorder* rec = nullptr){
    piw::WriteOptions o = writeOptions(out, typeOf<T>(), d);
    if(block){ o.hasBlockSize = true; for(int i = 0; i < 3; ++i) o.blockSize[i] = block[i]; }
    piw::Volume v; v.data = data; v.stride[0] = s[0]; v.stride[1] = s[1]; v.stride[2] = s[2];
    Recorder local;
    return piw::writeVolumes(o, {v}, callbacksFor(rec ? *rec : local));
}

static piw::ImsInfo infoOf(const fs::path& f, bool checksums = true){
    piw::ImsInfo info;
    const piw::Status s = piw::imsInfo(native(f), info, checksums);
    if(!s.ok()){ std::printf("  imsInfo(%s) failed: %s\n", f.string().c_str(), s.message.c_str()); ++gFailures; }
    return info;
}

// The histograms of resolution levels 1 and up (the Histogram/Histogram1024 datasets and the
// HistogramMin/HistogramMax attributes of their groups) are left out of the comparisons below:
// ImarisWriter (b128e6e, bpMultiresolutionImsImage::FinishWriteDataBlocks) stops waiting once its
// resample counter is 0, while the last resample's copy into the lower level -- which queues that
// level's histogram jobs -- can still be pending on its compute thread. The lower level's histogram
// is then still empty and is not written: about 1 test run in 10 here, the voxels always complete.
// Pass lowerHistograms = true to compare them too.
static bool isLowerLevelHistogramAttribute(const std::string& key){
    const std::string prefix = "/DataSet/ResolutionLevel ";
    if(key.compare(0, prefix.size(), prefix) != 0 || key.compare(prefix.size(), 2, "0/") == 0) return false;
    const size_t slash = key.rfind('/');
    return key.compare(slash + 1, 9, "Histogram") == 0;
}

// Everything imsInfo reports, compared; why names the first difference.
static bool sameInfo(const piw::ImsInfo& a, const piw::ImsInfo& b, std::string& why, bool lowerHistograms = false){
    auto differ = [&](const std::string& what){ why = what; return false; };
    for(int i = 0; i < 3; ++i){
        if(a.size[i] != b.size[i]) return differ("size");
        if(a.extentMin[i] != b.extentMin[i] || a.extentMax[i] != b.extentMax[i]) return differ("extents");
    }
    if(a.channels != b.channels || a.timepoints != b.timepoints || a.levels != b.levels) return differ("channels/time points/levels");
    if(a.dataType != b.dataType) return differ("data type");
    if(a.thumbnailCrc != b.thumbnailCrc) return differ("thumbnail CRC");
    if(a.channelNames != b.channelNames || a.channelColors != b.channelColors || a.timePoints != b.timePoints) return differ("channel names/colors/time points");
    if(a.datasets.size() != b.datasets.size()) return differ("dataset count");
    if(a.datasets.empty()) return differ("no datasets");
    for(size_t i = 0; i < a.datasets.size(); ++i){
        const piw::ImsDataset &x = a.datasets[i], &y = b.datasets[i];
        const std::string at = " of level " + std::to_string(x.level) + " time point " + std::to_string(x.timepoint) +
                               " channel " + std::to_string(x.channel);
        if(x.level != y.level || x.timepoint != y.timepoint || x.channel != y.channel) return differ("dataset order");
        for(int k = 0; k < 3; ++k) if(x.size[k] != y.size[k] || x.chunk[k] != y.chunk[k]) return differ("size/chunk" + at);
        if(x.chunks != y.chunks) return differ("chunk count" + at);
        if(x.crc != y.crc) return differ("data CRC" + at);
        if((x.level == 0 || lowerHistograms) && x.histogramCrc != y.histogramCrc) return differ("histogram/attribute CRC" + at);
    }
    std::vector<std::pair<std::string, std::string>> aa, ba;
    for(const auto& kv : a.attributes) if(lowerHistograms || !isLowerLevelHistogramAttribute(kv.first)) aa.push_back(kv);
    for(const auto& kv : b.attributes) if(lowerHistograms || !isLowerLevelHistogramAttribute(kv.first)) ba.push_back(kv);
    if(aa.size() != ba.size()) return differ("attribute count " + std::to_string(aa.size()) + " vs " + std::to_string(ba.size()));
    for(size_t i = 0; i < aa.size(); ++i)
        if(aa[i] != ba[i])
            return differ("attribute " + aa[i].first + "=\"" + aa[i].second + "\" vs " + ba[i].first + "=\"" + ba[i].second + "\"");
    return true;
}

static void checkSameInfo(const fs::path& a, const fs::path& b, const std::string& what){
    std::string why;
    const bool same = sameInfo(infoOf(a), infoOf(b), why);
    check(same, what + (same ? "" : " (differs: " + why + ")"));
}

// Reads one whole dataset with readIms, X fastest.
template<typename T> static bool readWhole(const fs::path& f, unsigned level, const Dims& d, std::vector<T>& out,
                                           unsigned t = 0, unsigned c = 0){
    out.assign(d.n(), (T)0);
    const uint64_t start[3] = {0, 0, 0}, end[3] = {d.x, d.y, d.z};
    const int64_t stride[3] = {1, (int64_t)d.x, (int64_t)(d.x*d.y)};
    const piw::Status s = piw::readIms(native(f), level, t, c, start, end, typeOf<T>(), out.data(), stride);
    if(!s.ok()) std::printf("  readIms failed: %s\n", s.message.c_str());
    return s.ok();
}

// The mean of each 2x2x2 box (along the axes that halved) as ImarisWriter computes it.
template<typename T> static std::vector<T> boxMean(const std::vector<T>& v, const Dims& d, const uint64_t st[3], const Dims& lo){
    std::vector<T> out(lo.n());
    for(uint64_t z = 0; z < lo.z; z++)
        for(uint64_t y = 0; y < lo.y; y++)
            for(uint64_t x = 0; x < lo.x; x++){
                float sum = 0;
                for(uint64_t dz = 0; dz < st[2]; dz++)
                    for(uint64_t dy = 0; dy < st[1]; dy++)
                        for(uint64_t dx = 0; dx < st[0]; dx++)
                            sum += (float)v[(x*st[0] + dx) + (y*st[1] + dy)*d.x + (z*st[2] + dz)*d.x*d.y];
                sum /= (float)(st[0]*st[1]*st[2]);
                out[x + y*lo.x + z*lo.x*lo.y] = (T)sum;
            }
    return out;
}

// One type and size: write from an X-fastest buffer, read back every level (equal to the source
// at level 0 and to imsutil's decoder at every level); then other layouts and block sizes.
template<typename T> static void volumeRoundTrip(const fs::path& work, const Dims& d, bool layouts){
    const std::string tag = std::string(typeName<T>()) + "_" + std::to_string(d.x) + "x" + std::to_string(d.y) + "x" + std::to_string(d.z);
    std::printf("[volumes %s]\n", tag.c_str());
    const std::vector<T> v = makeTyped<T>(d, 3);
    const int64_t xFast[3] = {1, (int64_t)d.x, (int64_t)(d.x*d.y)};
    const fs::path a = work/("vol_" + tag + ".ims");
    Recorder rec;
    piw::Status s = writeOne<T>(a, d, v.data(), xFast, nullptr, &rec);
    check(s.ok() && progressOk(rec), "writeVolumes ok, progress ends at 1 (" + s.message + ")");
    if(!s.ok()) return;
    check(countLines(rec.text, "Current Volume: ") == 1 && countLines(rec.text, "Voxel Sizes: 0.500000,0.250000,2.000000") == 1,
          "messages: block and voxel sizes, one Current Volume line");
    const piw::ImsInfo info = infoOf(a);
    check(info.size[0] == d.x && info.size[1] == d.y && info.size[2] == d.z && info.channels == 1 && info.timepoints == 1 &&
          info.dataType == typeOf<T>() && info.levels >= 1 && info.datasets.size() == info.levels,
          "imsInfo: size, one channel and time point, type, " + std::to_string(info.levels) + " levels");
    check(std::fabs(info.extentMax[0] - 0.5*d.x) < 1e-4 && std::fabs(info.extentMax[1] - 0.25*d.y) < 1e-4 &&
          std::fabs(info.extentMax[2] - 2.0*d.z) < 1e-4 && info.extentMin[0] == 0 &&
          std::fabs(info.voxelSize[0] - 0.5f) < 1e-6 && std::fabs(info.voxelSize[2] - 2.0f) < 1e-6,
          "imsInfo: extents and voxel sizes");
    bool levelsOk = true;
    for(const piw::ImsDataset& ds : info.datasets){
        std::vector<T> back;
        const Dims ld = {ds.size[0], ds.size[1], ds.size[2]};
        if(!readWhole(a, ds.level, ld, back)){ levelsOk = false; break; }
        if(ds.level == 0 && back != v){ levelsOk = false; std::printf("  level 0 differs from the source\n"); }
        ImsVolumeInfo vi;
        const std::vector<uint8_t> ref = imsReadVolume(a, (int)ds.level, 0, 0, vi);
        if(!vi.error.empty() || ref.size() != back.size()*sizeof(T) || std::memcmp(ref.data(), back.data(), ref.size()) != 0){
            levelsOk = false; std::printf("  level %u differs from imsReadVolume (%s)\n", ds.level, vi.error.c_str());
        }
    }
    check(levelsOk, "readIms: level 0 equals the source; every level equals imsReadVolume");
    if(!layouts) return;

    // The same voxels in other layouts: the file does not change.
    const int64_t yFast[3] = {(int64_t)d.y, 1, (int64_t)(d.x*d.y)};        // a (z, y, x) C-contiguous array
    const int64_t zFast[3] = {(int64_t)d.z, (int64_t)(d.x*d.z), 1};
    const int64_t gaps[3] = {2, (int64_t)(2*d.x + 3), (int64_t)((2*d.x + 3)*d.y + 5)};
    const struct { const char* name; const int64_t* s; } views[] = {{"(z, y, x) C-contiguous", yFast}, {"z fastest", zFast},
                                                                    {"with gaps", gaps}};
    for(const auto& view : views){
        const std::vector<T> buf = relayout(v, d, view.s);
        const fs::path b = work/("vol_" + tag + "_" + std::to_string(view.s[0]) + ".ims");
        s = writeOne<T>(b, d, buf.data(), view.s);
        check(s.ok(), std::string("writeVolumes ") + view.name + " (" + s.message + ")");
        if(s.ok()) checkSameInfo(a, b, std::string(view.name) + " layout: the same file");
    }
    // Other block sizes (the pieces handed to the writer): the same file. 256x256x64 of uint16 is
    // 8 MiB, which takes the threaded fill.
    const uint64_t blocks[][3] = {{32, 16, 8}, {256, 256, 64}};
    for(const auto& bl : blocks){
        const fs::path b = work/("vol_" + tag + "_b" + std::to_string(bl[0]) + ".ims");
        const std::vector<T> buf = relayout(v, d, yFast);
        s = writeOne<T>(b, d, buf.data(), yFast, bl);
        check(s.ok(), "writeVolumes block " + std::to_string(bl[0]) + "," + std::to_string(bl[1]) + "," + std::to_string(bl[2]));
        if(s.ok()) checkSameInfo(a, b, "block " + std::to_string(bl[0]) + "," + std::to_string(bl[1]) + "," + std::to_string(bl[2]) +
                                       " from the (z, y, x) layout: the same file");
        s = writeOne<T>(b, d, v.data(), xFast, bl);
        if(s.ok()) checkSameInfo(a, b, "block " + std::to_string(bl[0]) + "," + std::to_string(bl[1]) + "," + std::to_string(bl[2]) +
                                       " from the X-fastest layout: the same file");
    }
}

static void testVolumes(const fs::path& work, const fs::path& mdir){
    volumeRoundTrip<uint8_t>(work, {37, 23, 11}, false);
    volumeRoundTrip<uint8_t>(work, {130, 70, 67}, true);
    volumeRoundTrip<uint16_t>(work, {37, 23, 11}, false);
    volumeRoundTrip<uint16_t>(work, {50, 40, 1}, false);
    volumeRoundTrip<uint16_t>(work, {130, 70, 67}, true);
    volumeRoundTrip<uint32_t>(work, {37, 23, 11}, false);
    volumeRoundTrip<uint32_t>(work, {130, 70, 67}, true);
    volumeRoundTrip<float>(work, {50, 40, 1}, false);
    volumeRoundTrip<float>(work, {130, 70, 67}, true);

    // writeVolumes equals convert of Zarrs holding the same data: 2 channels x 2 time points.
    std::printf("[volumes vs convert, 2 channels x 2 time points]\n");
    {
        const Dims d = {149, 129, 101};
        std::vector<std::vector<uint16_t>> data;
        std::vector<piw::Volume> volumes;
        for(int c = 0; c < 2; ++c)
            for(int t = 0; t < 2; ++t) data.push_back(makeVolume(d.x, d.y, d.z, 2*c + t));   // as the multi Zarrs
        for(const auto& v : data){
            piw::Volume vol; vol.data = v.data(); vol.stride[0] = 1; vol.stride[1] = (int64_t)d.x; vol.stride[2] = (int64_t)(d.x*d.y);
            volumes.push_back(vol);
        }
        piw::WriteOptions o = writeOptions(work/"vol_multi.ims", piw::DataType::UInt16, d, 2, 2);
        o.voxelSize[0] = o.voxelSize[1] = o.voxelSize[2] = 1.0f;
        Recorder rec;
        piw::Status s = piw::writeVolumes(o, volumes, callbacksFor(rec));
        check(s.ok(), "writeVolumes 2 x 2 (" + s.message + ")");
        const fs::path conv = work/"vol_multi_convert.ims";
        piw::ConvertOptions co = optionsFor("zarr", findFiles(mdir, {"CamA", "CamB"}), conv, "");
        co.startTime = kStartTime;
        s = piw::convert(co, callbacksFor(rec));
        check(s.ok(), "convert of the Zarrs (" + s.message + ")");
        checkSameInfo(conv, work/"vol_multi.ims", "writeVolumes equals convert (CRCs, histograms, every attribute, thumbnail)");
        check(sameIms(conv, work/"vol_multi.ims"), "writeVolumes equals convert (raw chunks and channel info, as the executable test)");
        const piw::ImsInfo info = infoOf(work/"vol_multi.ims");
        bool readOk = info.channels == 2 && info.timepoints == 2 && info.channelNames.size() == 2 &&
                      info.channelNames[0] == "Channel 0" && info.timePoints.size() == 2 &&
                      info.timePoints[0] == kStartTime && info.timePoints[1] == "2021-03-04 05:06:08";
        for(int c = 0; c < 2 && readOk; ++c)
            for(int t = 0; t < 2 && readOk; ++t){
                std::vector<uint16_t> back;
                readOk = readWhole(work/"vol_multi.ims", 0, d, back, t, c) && back == data[2*c + t];
            }
        check(readOk, "imsInfo channels, names, time points; readIms of every (channel, time point) equals its source");
        bool hasAttr = false;
        for(const auto& kv : info.attributes) if(kv.first == "/DataSetInfo/Image/X" && kv.second == "149") hasAttr = true;
        check(hasAttr && info.attributes.size() > 40, "imsInfo attributes include /DataSetInfo/Image/X = 149 (" +
              std::to_string(info.attributes.size()) + " attributes)");
    }

    // Level 1 is the 2x2x2 mean (float sum / 8, truncated); a region read into a strided output.
    std::printf("[read-back: level 1, regions]\n");
    {
        const Dims d = {128, 128, 64};
        const std::vector<uint16_t> v = makeTyped<uint16_t>(d, 5);
        const int64_t xFast[3] = {1, (int64_t)d.x, (int64_t)(d.x*d.y)};
        const fs::path f = work/"vol_levels.ims";
        check(writeOne<uint16_t>(f, d, v.data(), xFast).ok(), "writeVolumes 128x128x64");
        const piw::ImsInfo info = infoOf(f);
        check(info.levels >= 2, "at least 2 levels (" + std::to_string(info.levels) + ")");
        if(info.levels >= 2){
            const piw::ImsDataset& l1 = info.datasets[1];
            const Dims lo = {l1.size[0], l1.size[1], l1.size[2]};
            uint64_t st[3] = {d.x/lo.x, d.y/lo.y, d.z/lo.z};
            check(l1.level == 1 && st[0] == 2 && st[1] == 2 && st[2] == 2, "level 1 halves every axis (" +
                  std::to_string(lo.x) + "x" + std::to_string(lo.y) + "x" + std::to_string(lo.z) + ")");
            std::vector<uint16_t> back;
            check(readWhole(f, 1, lo, back) && back == boxMean(v, d, st, lo), "level 1 equals the 2x2x2 mean of level 0");
        }
        // region [10, 50) x [5, 30) x [20, 60) into a (z, y, x) C-ordered output with a gap per row
        const uint64_t start[3] = {10, 5, 20}, end[3] = {50, 30, 60};
        const uint64_t n[3] = {end[0] - start[0], end[1] - start[1], end[2] - start[2]};
        const int64_t stride[3] = {(int64_t)n[1] + 1, 1, (int64_t)((n[1] + 1)*n[0])};
        std::vector<uint16_t> out((size_t)(stride[2]*n[2]), 0);
        const piw::Status s = piw::readIms(native(f), 0, 0, 0, start, end, piw::DataType::UInt16, out.data(), stride, 3);
        bool same = s.ok();
        for(uint64_t z = 0; z < n[2] && same; ++z)
            for(uint64_t y = 0; y < n[1] && same; ++y)
                for(uint64_t x = 0; x < n[0] && same; ++x)
                    same = out[x*stride[0] + y*stride[1] + z*stride[2]] ==
                           v[(x + start[0]) + (y + start[1])*d.x + (z + start[2])*d.x*d.y];
        check(same, "region read into a strided output equals the source region (" + s.message + ")");
    }

    // Errors: nothing is written.
    std::printf("[volumes: errors]\n");
    {
        const Dims d = {20, 10, 5};
        const std::vector<uint16_t> v = makeTyped<uint16_t>(d, 0);
        const fs::path out = work/"vol_err.ims";
        const int64_t ok[3] = {1, 20, 200}, neg[3] = {1, -20, 200}, zero[3] = {0, 20, 200};
        piw::Status s = writeOne<uint16_t>(out, d, v.data(), neg);
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "negative stride: InvalidArgument, no output (" + s.message + ")");
        s = writeOne<uint16_t>(out, d, v.data(), zero);
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "zero stride: InvalidArgument (" + s.message + ")");
        s = writeOne<uint16_t>(out, d, nullptr, ok);
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "no data: InvalidArgument (" + s.message + ")");
        piw::WriteOptions o = writeOptions(out, piw::DataType::UInt16, d, 2, 1);
        piw::Volume vol; vol.data = v.data(); vol.stride[0] = 1; vol.stride[1] = 20; vol.stride[2] = 200;
        s = piw::writeVolumes(o, {vol});
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "1 volume for 2 channels: InvalidArgument (" + s.message + ")");
        o = writeOptions(out, piw::DataType::UInt16, {0, 10, 5});
        s = piw::writeVolumes(o, {vol});
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "size 0: InvalidArgument (" + s.message + ")");
        Recorder rec; rec.cancelOnPoll = 1;
        s = writeOne<uint16_t>(out, d, v.data(), ok, nullptr, &rec);
        check(s.code == piw::Code::Cancelled && !fs::exists(out), "cancel: Cancelled, output removed");

        check(writeOne<uint16_t>(out, d, v.data(), ok).ok(), "a small file to read");
        std::vector<uint16_t> buf(d.n());
        const uint64_t st[3] = {0, 0, 0}, en[3] = {20, 10, 5}, past[3] = {21, 10, 5};
        s = piw::readIms(native(out), 0, 0, 0, st, en, piw::DataType::Float32, buf.data(), ok);
        check(s.code == piw::Code::InvalidArgument, "readIms with the wrong type: InvalidArgument (" + s.message + ")");
        s = piw::readIms(native(out), 0, 0, 0, st, past, piw::DataType::UInt16, buf.data(), ok);
        check(s.code == piw::Code::InvalidArgument, "readIms past the end: InvalidArgument (" + s.message + ")");
        s = piw::readIms(native(out), 0, 0, 0, st, en, piw::DataType::UInt16, buf.data(), neg);
        check(s.code == piw::Code::InvalidArgument, "readIms with a negative stride: InvalidArgument");
        s = piw::readIms(native(out), 9, 0, 0, st, en, piw::DataType::UInt16, buf.data(), ok);
        check(s.code == piw::Code::InvalidArgument, "readIms of a missing level: InvalidArgument (" + s.message + ")");
        piw::ImsInfo info;
        s = piw::imsInfo(native(work/"no_such.ims"), info);
        check(s.code == piw::Code::NotFound, "imsInfo of a missing file: NotFound (" + s.message + ")");
        { std::ofstream(work/"not_hdf5.ims") << "not an hdf5 file"; }
        s = piw::imsInfo(native(work/"not_hdf5.ims"), info);
        check(s.code == piw::Code::ReadFailed, "imsInfo of a text file: ReadFailed (" + s.message + ")");
    }
}

// The image's x on Imaris X (xOnImarisX), and bounding boxes on TIFF input (fixed in this
// version: the TIFF was cropped in shape but read uncropped, which scrambled the data).
// writeTiff(X, Y, Z) stores makeVolume's buffer as Z pages of Y rows and X columns, and the reader
// hands the converter the rows fastest: the TIFF's axes (row, column, page) are the buffer's
// (y, x, z), the transpose of the Zarr of the same buffer. A Zarr of the TIFF's image (written here
// as tzdir) is its equal.
static void testAxesAndBoxes(const fs::path& work, const fs::path& tdir, const fs::path& zdir, const std::string& exe){
    const Dims d = {100, 70, 200};                   // the TIFF and the Zarr of main(), seed 0
    const std::vector<uint16_t> v = makeVolume(d.x, d.y, d.z, 0);
    const Dims td = {d.y, d.x, d.z};                 // the TIFF's axes
    const int64_t transpose[3] = {(int64_t)d.y, 1, (int64_t)(d.x*d.y)};
    const std::vector<uint16_t> tv = relayout(v, d, transpose);   // the TIFF's image, row fastest
    const fs::path tzdir = work/"tiffimage_zarr";
    fs::create_directories(tzdir);
    writeZarrBuffer(tzdir/"CamA_t0000.zarr", tv, td.x, td.y, td.z);
    std::printf("[xOnImarisX]\n");
    {
        // input axes (a0, a1, a2) -> Imaris (a1, a0, a2), voxel sizes swapped too: writeVolumes of
        // the same bytes with the strides of axes 0 and 1 swapped.
        for(const char* reader : {"zarr", "tiff"}){
            const bool tiff = std::string(reader) == "tiff";
            const Dims in = tiff ? td : d, im = {in.y, in.x, in.z};
            const std::string ref = std::string("axes_vol_") + reader + ".ims";
            piw::WriteOptions o = writeOptions(work/ref, piw::DataType::UInt16, im);
            o.voxelSize[0] = 0.2f; o.voxelSize[1] = 0.1f; o.voxelSize[2] = 0.3f;
            piw::Volume vol; vol.data = tiff ? tv.data() : v.data();   // the input's own buffer
            vol.stride[0] = (int64_t)in.x; vol.stride[1] = 1; vol.stride[2] = (int64_t)(in.x*in.y);
            check(piw::writeVolumes(o, {vol}).ok(), std::string("writeVolumes of the ") + reader + " image with its axes 0 and 1 swapped");
            for(const char* pb : {"", "32,32,32"}){
                Recorder rec;
                const fs::path out = work/(std::string("axes_") + reader + (*pb ? "_C" : "") + ".ims");
                piw::ConvertOptions c = optionsFor(reader, findFiles(tiff ? tdir : zdir, {"CamA"}), out, pb);
                c.voxelSize[0] = 0.1f; c.voxelSize[1] = 0.2f; c.voxelSize[2] = 0.3f;
                c.xOnImarisX = true;
                c.startTime = kStartTime;
                const piw::Status s = piw::convert(c, callbacksFor(rec));
                check(s.ok(), std::string(reader) + (*pb ? " -C" : "") + " with xOnImarisX (" + s.message + ")");
                if(s.ok()) checkSameInfo(work/ref, out, std::string(reader) + (*pb ? " -C " : " ") + pb +
                                         " with xOnImarisX: the same file as writeVolumes with axes 0 and 1 swapped");
            }
        }
        // without it, the TIFF and the Zarr of its image give the same file (the image's y on X)
        Recorder rec;
        const fs::path ta = work/"axes_tiff_default.ims", za = work/"axes_tzarr_default.ims";
        piw::ConvertOptions ct = optionsFor("tiff", findFiles(tdir, {"CamA"}), ta, ""), cz = optionsFor("zarr", findFiles(tzdir, {"CamA"}), za, "");
        ct.startTime = cz.startTime = kStartTime;
        check(piw::convert(ct, callbacksFor(rec)).ok() && piw::convert(cz, callbacksFor(rec)).ok(), "tiff and its zarr");
        checkSameInfo(ta, za, "default axes: the TIFF equals a Zarr of its image");
    }
    std::printf("[TIFF bounding boxes]\n");
    const struct { const char* box; uint64_t b[6]; const char* pb; } boxes[] = {
        {"10,10,10,60,50,100", {10, 10, 10, 60, 50, 100}, ""},
        {"10,10,10,60,50,100", {10, 10, 10, 60, 50, 100}, "200,200,30"},   // two Z-slabs, the second partial
        {"5,7,0,65,95,200",    {5, 7, 0, 65, 95, 200},    ""},             // every page: the whole-file read
        {"0,0,150,70,100,200", {0, 0, 150, 70, 100, 200}, ""},             // the last pages only
    };
    int i = 0;
    for(const auto& k : boxes){
        const std::string tag = std::to_string(i++);
        const std::string what = std::string("-B ") + k.box + (*k.pb ? std::string(" -C ") + k.pb : "");
        fs::path files[2];
        for(int r = 0; r < 2; ++r){
            const char* reader = r ? "zarr" : "tiff";
            files[r] = work/("box" + tag + "_" + reader + ".ims");
            piw::ConvertOptions c = optionsFor(reader, findFiles(r ? tzdir : tdir, {"CamA"}), files[r], k.pb, {"CamA"});
            c.startTime = kStartTime;   // as the executable's below: the TIFF and the Zarr have their own times
            c.hasBoundingBox = true;
            for(int j = 0; j < 6; ++j) c.boundingBox[j] = k.b[j];
            Recorder rec;
            const piw::Status s = piw::convert(c, callbacksFor(rec));
            check(s.ok(), std::string(reader) + " " + what + " (" + s.message + ")");
        }
        checkSameInfo(files[0], files[1], "tiff " + what + ": the same file as a Zarr of the TIFF's image");
        const Dims cd = {k.b[3] - k.b[0], k.b[4] - k.b[1], k.b[5] - k.b[2]};
        std::vector<uint16_t> back;
        bool same = readWhole(files[0], 0, cd, back);
        for(uint64_t z = 0; z < cd.z && same; ++z)
            for(uint64_t y = 0; y < cd.y && same; ++y)
                for(uint64_t x = 0; x < cd.x && same; ++x)
                    same = back[x + y*cd.x + z*cd.x*cd.y] == tv[(x + k.b[0]) + (y + k.b[1])*td.x + (z + k.b[2])*td.x*td.y];
        check(same, "tiff " + what + ": the voxels are the cropped input");
    }
    // through the executable
    for(const char* pb : {"", "200,200,30"}){
        fs::path outs[2];
        for(int r = 0; r < 2; ++r){
            const char* reader = r ? "zarr" : "tiff";
            const fs::path od = work/(std::string("exe_box_") + reader + (*pb ? "_C" : ""));
            fs::create_directories(od);
            std::string args = "-P CamA -F \"" + native(r ? tzdir : tdir) + "\" -o \"" + native(od) + "\" -r " + reader +
                               " -v 1,1,1 -B 10,10,10,60,50,100 --start-time \"" + kStartTime + "\"";
            if(*pb) args += std::string(" -C ") + pb;
            const int rc = runExe(exe, args, od/"log.txt");
            check(rc == 0, std::string("executable ") + reader + " -B exit 0");
            outs[r] = od/"output.ims";
        }
        check(sameIms(outs[0], outs[1]), std::string("executable: tiff -B equals zarr -B") + (*pb ? " (with -C)" : ""));
        checkSameInfo(outs[0], work/(std::string("box") + (*pb ? "1" : "0") + "_tiff.ims"), "executable tiff -B equals piw::convert");
    }
}

// apiTest --tiff <file> <workdir> [bx,by,bz]: writeVolumes of one TIFF from the readers' (y, x, z)
// buffer and from a (z, y, x) C-contiguous copy of it, with the seconds of each.
static int tiffTiming(const std::string& file, const fs::path& work, const char* blocks){
    fs::create_directories(work);
    uint64_t* size = getImageSize(file.c_str());
    const uint64_t bits = getDataType(file.c_str());
    if(!size || bits != 16){ std::fprintf(stderr, "need a readable 16-bit TIFF\n"); return 2; }
    const Dims d = {size[0], size[1], size[2]};
    free(size);
    auto now = []{ return std::chrono::steady_clock::now(); };
    auto seconds = [](std::chrono::steady_clock::time_point a, std::chrono::steady_clock::time_point b){
        return std::chrono::duration<double>(b - a).count(); };
    auto t0 = now();
    uint16_t* yxz = (uint16_t*)readTiffParallelWrapper(file.c_str());
    if(!yxz){ std::fprintf(stderr, "could not read %s\n", file.c_str()); return 2; }
    auto t1 = now();
    // c[z][y][x] (x the TIFF column, fastest) from the readers' buffer (the row fastest)
    std::vector<uint16_t> zyx(d.n());
    #pragma omp parallel for collapse(2) schedule(static)
    for(int64_t z = 0; z < (int64_t)d.z; ++z)
        for(int64_t x0 = 0; x0 < (int64_t)d.y; x0 += 64)
            for(uint64_t r = 0; r < d.x; ++r)
                for(uint64_t c = (uint64_t)x0; c < std::min<uint64_t>(d.y, (uint64_t)x0 + 64); ++c)
                    zyx[z*d.x*d.y + r*d.y + c] = yxz[r + c*d.x + z*d.x*d.y];
    auto t2 = now();
    std::printf("TIFF %s: %llux%llux%llu uint16, read %.2f s, (z, y, x) copy %.2f s, threads %d\n", file.c_str(),
                (unsigned long long)d.x, (unsigned long long)d.y, (unsigned long long)d.z, seconds(t0, t1), seconds(t1, t2),
                omp_get_max_threads());
    piw::WriteOptions o = writeOptions(work/"ims_yxz.ims", piw::DataType::UInt16, d);
    o.voxelSize[0] = 0.108f; o.voxelSize[1] = 0.108f; o.voxelSize[2] = 0.25f;   // the real-data harness's -v
    if(blocks){
        o.hasBlockSize = true;
        std::sscanf(blocks, "%llu,%llu,%llu", (unsigned long long*)&o.blockSize[0], (unsigned long long*)&o.blockSize[1],
                    (unsigned long long*)&o.blockSize[2]);
    }
    piw::Volume a; a.data = yxz; a.stride[0] = 1; a.stride[1] = (int64_t)d.x; a.stride[2] = (int64_t)(d.x*d.y);
    piw::Volume b; b.data = zyx.data(); b.stride[0] = (int64_t)d.y; b.stride[1] = 1; b.stride[2] = (int64_t)(d.x*d.y);
    auto t3 = now();
    piw::Status s = piw::writeVolumes(o, {a});
    auto t4 = now();
    if(!s.ok()){ std::fprintf(stderr, "writeVolumes (y, x, z): %s\n", s.message.c_str()); return 1; }
    o.outputFile = native(work/"ims_zyx.ims");
    s = piw::writeVolumes(o, {b});
    auto t5 = now();
    if(!s.ok()){ std::fprintf(stderr, "writeVolumes (z, y, x): %s\n", s.message.c_str()); return 1; }
    std::printf("writeVolumes (y, x, z) buffer: %.2f s\nwriteVolumes (z, y, x) C-contiguous: %.2f s\nratio zyx/yxz: %.3f\n",
                seconds(t3, t4), seconds(t4, t5), seconds(t4, t5)/seconds(t3, t4));
    free(yxz);
    std::string why;
    const bool same = sameInfo(infoOf(work/"ims_yxz.ims"), infoOf(work/"ims_zyx.ims"), why);
    std::printf("imsInfo checksums: %s\n", same ? "equal" : ("DIFFERENT: " + why).c_str());
    return same && gFailures == 0 ? 0 : 1;
}

struct Case { std::string name, reader; fs::path dir; std::vector<std::string> patterns; std::string processBlock; };

// --- metadata (A-10) ---

// The modification time of p plus aSeconds as "YYYY-MM-DD HH:MM:SS", local time (strftime, not the
// converter's own calendar code).
static std::string localStamp(const fs::path& p, int aSeconds){
    struct stat st;
    if(stat(native(p).c_str(), &st) != 0) return "<no mtime>";
    const time_t t = st.st_mtime + aSeconds;
    const struct tm* lt = std::localtime(&t);
    char b[64];
    if(!lt || !std::strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", lt)) return "<bad mtime>";
    return b;
}

static ImsExpectedMeta expectedMeta(uint64_t X, uint64_t Y, uint64_t Z){
    ImsExpectedMeta e;
    e.size[0] = X; e.size[1] = Y; e.size[2] = Z;
    e.version = PIW_VERSION;
    return e;
}

static void checkMeta(const fs::path& ims, const ImsExpectedMeta& e, const std::string& what){
    std::vector<std::string> problems;
    const bool ok = imsCheckDataSetInfo(ims, e, problems);
    for(size_t i = 0; i < problems.size() && i < 20; ++i) std::printf("    %s\n", problems[i].c_str());
    check(ok, what + ": every DataSetInfo attribute as expected");
}

static void metadataTests(const std::string& exe, const fs::path& work, const fs::path& mdir){
    std::printf("[metadata: imsmeta.h helpers]\n");
    {
        unsigned jd = 0; unsigned long long ns = 0; int64_t ms = 0;
        check(imsmeta::parseTime("2020-02-05 15:27:04", ms), "parseTime of a plain time");
        imsmeta::julianTime(ms, jd, ns);
        check(jd == 2458885u && ns == 55624ull * 1000000000ull, "julianTime: 2020-02-05 15:27:04 is Julian day 2458885, 55624 s");
        check(imsmeta::timeString(ms) == "2020-02-05 15:27:04", "timeString without milliseconds");
        const char* texts[] = {"2024-02-29 23:59:59.999", "1999-12-31 00:00:00.001", "2026-11-12 08:30:15.250", "1970-01-01 00:00:00", "1969-12-31 23:59:59.500"};
        for(const char* t : texts) check(imsmeta::parseTime(t, ms) && imsmeta::timeString(ms) == t, std::string("parseTime/timeString round trip: ") + t);
        check(imsmeta::parseTime("2026-03-04T05:06", ms) && imsmeta::timeString(ms) == "2026-03-04 05:06:00", "a 'T' separator and no seconds");
        check(imsmeta::parseTime("2026-03-04", ms) && imsmeta::timeString(ms) == "2026-03-04 00:00:00", "a date alone");
        check(imsmeta::parseTime("2026-03-04 05:06:07.1239", ms) && imsmeta::timeString(ms) == "2026-03-04 05:06:07.123", "a fraction beyond milliseconds");
        for(const char* bad : {"2026-02-30 00:00:00", "2025-02-29", "2026-13-01", "2026-01-01 24:00:00", "26-01-01", "2026-01-01 1:00", "2026-01-01 10:00:00.", "2026-01-01x", ""})
            check(!imsmeta::parseTime(bad, ms), std::string("parseTime refuses \"") + bad + "\"");
        int64_t st = 0;
        check(imsmeta::msecAbsStamp("/d/Scan_Iter_0001_CamA_ch0_0000123msec_0012345678msecAbs.zarr", st) && st == 12345678, "msecAbs stamp of a PetaKit5D name");
        check(imsmeta::msecAbsStamp("d\\x_0000042msecAbs_decon.tif", st) && st == 42, "msecAbs stamp with a Windows separator and a suffix");
        check(imsmeta::msecAbsStamp("/d/x_7msecAbs.zarr/", st) && st == 7, "msecAbs stamp of a folder with a trailing separator");
        check(!imsmeta::msecAbsStamp("/d/123msecAbs/CamA_0000123msec.tif", st), "no msecAbs stamp (one in a folder name does not count)");
        check(!imsmeta::msecAbsStamp("/d/CamA_msecAbs.tif", st), "no msecAbs stamp without digits");
        piw::Color c;
        check(imsmeta::parseColor("cyan", c) && c.red == 0 && c.green == 1 && c.blue == 1, "color by name");
        check(imsmeta::parseColor("FF8000", c) && c.red == 1 && c.green == 128.f / 255.f && c.blue == 0, "color as RRGGBB");
        check(imsmeta::parseColor("#0000ff", c) && c.blue == 1 && c.red == 0, "color as #rrggbb");
        check(imsmeta::parseColor("0.25:0.5:0.75", c) && c.red == 0.25f && c.green == 0.5f && c.blue == 0.75f, "color as r:g:b");
        check(!imsmeta::parseColor("purple", c) && !imsmeta::parseColor("FF80", c) && !imsmeta::parseColor("1:2", c), "bad colors refused");
        check(imsmeta::wavelengthString(488.f) == "488" && imsmeta::wavelengthString(561.5f) == "561.5", "wavelength texts");
    }

    // 2 channels x 2 time points (the inputs of the multi cases), 149 x 129 x 101
    const auto files = findFiles(mdir, {"CamA", "CamB"});
    std::printf("[metadata: defaults]\n");
    {
        const fs::path out = work/"meta_default.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.ok(), "convert with the default metadata (" + s.message + ")");
        ImsExpectedMeta e = expectedMeta(149, 129, 101);
        e.names = {"Channel 0", "Channel 1"};
        e.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
        e.times = {localStamp(files[0][0], 0), localStamp(files[0][0], 1)};
        std::printf("  first input's modification time: %s\n", e.times[0].c_str());
        checkMeta(out, e, "defaults");
    }
    std::printf("[metadata: options]\n");
    piw::ConvertOptions opt = optionsFor("zarr", files, work/"meta_options.ims", "");
    opt.channelNames = {"GFP", "mCherry 2"};
    opt.channelColors = {{0.f, 1.f, 1.f}, {0.25f, 0.5f, 0.75f}};
    opt.emissionWavelengths = {525.f, 0.f};
    opt.excitationWavelengths = {488.f, 561.5f};
    opt.startTime = "2026-01-31 23:59:58.250";
    opt.timeInterval = 2.5;
    ImsExpectedMeta eo = expectedMeta(149, 129, 101);
    eo.names = {"GFP", "mCherry 2"};
    eo.colors = {"0.000 1.000 1.000", "0.250 0.500 0.750"};
    eo.emission = {"525", ""};
    eo.excitation = {"488", "561.5"};
    eo.times = {"2026-01-31 23:59:58.250", "2026-02-01 00:00:00.750"};
    {
        Recorder rec;
        const piw::Status s = piw::convert(opt, callbacksFor(rec));
        check(s.ok(), "convert with names, colors, wavelengths, start time and interval (" + s.message + ")");
        checkMeta(opt.outputFile, eo, "options");
    }
    std::printf("[metadata: the same options on the command line]\n");
    {
        const fs::path od = work/"meta_cli"; fs::create_directories(od);
        const std::string args = "-P CamA,CamB -F \"" + native(mdir) + "\" -o \"" + native(od) + "\" -r zarr -v 1,1,1"
            " --channel-names \"GFP,mCherry 2\" --colors cyan,0.25:0.5:0.75 --emission 525,0 --excitation=488,561.5"
            " --start-time \"2026-01-31 23:59:58.250\" --time-interval 2.5";
        const int rc = runExe(exe, args, work/"meta_cli.log");
        check(rc == 0, "executable with the metadata flags (exit " + std::to_string(rc) + ")");
        checkMeta(od/"output.ims", eo, "command line");
        ImsCompareReport report;
        ImsCompareOptions co; co.thumbnail = true;
        const bool same = imsCompare(opt.outputFile, od/"output.ims", report, co);
        imsPrintReport(report);
        check(same, "command line and piw::convert write the same file (data and every attribute)");
    }
    std::printf("[metadata: writeVolumes]\n");
    {
        // the data of the conversion above, from memory, with the same metadata options: the same file
        const Dims d = {149, 129, 101};
        std::vector<std::vector<uint16_t>> data;
        std::vector<piw::Volume> volumes;
        for(int c = 0; c < 2; ++c)
            for(int t = 0; t < 2; ++t) data.push_back(makeVolume(d.x, d.y, d.z, 2*c + t));   // as the multi Zarrs
        for(const auto& v : data){
            piw::Volume vol; vol.data = v.data(); vol.stride[0] = 1; vol.stride[1] = (int64_t)d.x; vol.stride[2] = (int64_t)(d.x*d.y);
            volumes.push_back(vol);
        }
        piw::WriteOptions w = writeOptions(work/"meta_volumes.ims", piw::DataType::UInt16, d, 2, 2);
        w.voxelSize[0] = w.voxelSize[1] = w.voxelSize[2] = 1.0f;
        w.channelNames = opt.channelNames; w.channelColors = opt.channelColors; w.emissionWavelengths = opt.emissionWavelengths;
        w.excitationWavelengths = opt.excitationWavelengths; w.startTime = opt.startTime; w.timeInterval = opt.timeInterval;
        piw::Status s = piw::writeVolumes(w, volumes);
        check(s.ok(), "writeVolumes with the metadata options (" + s.message + ")");
        checkMeta(w.outputFile, eo, "writeVolumes with the metadata options");
        checkSameInfo(opt.outputFile, w.outputFile, "writeVolumes equals convert with the same metadata options");
        // without a start time: the current time, then 1 s apart
        w.outputFile = native(work/"meta_volumes_now.ims"); w.startTime.clear(); w.timeInterval = 0;
        auto stamp = [](time_t t){ char b[64]; const struct tm* lt = std::localtime(&t);
                                   return lt && std::strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", lt) ? std::string(b) : std::string(); };
        const std::string before = stamp(std::time(NULL));
        s = piw::writeVolumes(w, volumes);
        const std::string after = stamp(std::time(NULL));
        const piw::ImsInfo info = infoOf(work/"meta_volumes_now.ims");
        int64_t ms = 0;
        check(s.ok() && info.timePoints.size() == 2 && info.timePoints[0] >= before && info.timePoints[0] <= after &&
              imsmeta::parseTime(info.timePoints[0], ms) && imsmeta::timeString(ms + 1000) == info.timePoints[1],
              "writeVolumes without a start time: the current time, 1 s apart (" + (info.timePoints.empty() ? std::string("none") : info.timePoints[0]) +
              ", called at " + before + ")");
        // a bad metadata option: nothing is written
        w.outputFile = native(work/"meta_volumes_bad.ims"); w.channelNames = {"only one"};
        s = piw::writeVolumes(w, volumes);
        check(s.code == piw::Code::InvalidArgument && !fs::exists(work/"meta_volumes_bad.ims"),
              "writeVolumes with 1 name for 2 channels: InvalidArgument, no output (" + s.message + ")");
    }
    std::printf("[metadata: msecAbs stamps]\n");
    {
        // PetaKit5D-style names; the second channel's stamps are used, the first channel has none
        const fs::path sdir = work/"stamps"; fs::create_directories(sdir);
        const char* a[] = {"CamA_t0000.zarr", "CamA_t0001.zarr", "CamA_t0002.zarr"};
        const char* b[] = {"Scan_Iter_0000_CamB_ch0_0000000msec_0086399000msecAbs.zarr",
                           "Scan_Iter_0001_CamB_ch0_0001250msec_0086400250msecAbs.zarr",
                           "Scan_Iter_0002_CamB_ch0_0061000msec_0086460000msecAbs.zarr"};
        for(int t = 0; t < 3; ++t){ writeZarr(sdir/a[t], 40, 30, 20, t); writeZarr(sdir/b[t], 40, 30, 20, 10 + t); }
        const auto sf = findFiles(sdir, {"CamA", "CamB"});
        piw::ConvertOptions o = optionsFor("zarr", sf, work/"meta_stamps.ims", "", {"CamA", "CamB"});
        o.startTime = "2026-11-12 23:59:59";
        Recorder rec;
        piw::Status s = piw::convert(o, callbacksFor(rec));
        check(s.ok(), "convert with stamps and a start time (" + s.message + ")");
        ImsExpectedMeta e = expectedMeta(40, 30, 20);
        e.names = {"CamA", "CamB"};
        e.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
        e.times = {"2026-11-12 23:59:59", "2026-11-13 00:00:00.250", "2026-11-13 00:01:00"};
        checkMeta(o.outputFile, e, "msecAbs stamps from the start time");
        // without a start time: from the first input's modification time; an interval overrides the stamps
        o.startTime.clear();
        o.outputFile = native(work/"meta_stamps_mtime.ims");
        s = piw::convert(o, callbacksFor(rec));
        e.times = {localStamp(sf[0][0], 0), localStamp(sf[0][0], 1) + ".250", localStamp(sf[0][0], 61)};
        check(s.ok(), "convert with stamps, no start time (" + s.message + ")");
        checkMeta(o.outputFile, e, "msecAbs stamps from the modification time");
        o.timeInterval = 0.5;
        o.outputFile = native(work/"meta_stamps_interval.ims");
        s = piw::convert(o, callbacksFor(rec));
        e.times = {localStamp(sf[0][0], 0), localStamp(sf[0][0], 0) + ".500", localStamp(sf[0][0], 1)};
        check(s.ok(), "convert with stamps and an interval (" + s.message + ")");
        checkMeta(o.outputFile, e, "an interval overrides the stamps");
    }
    std::printf("[metadata: bad options]\n");
    {
        struct Bad { std::string what; piw::ConvertOptions o; };
        std::vector<Bad> bads;
        piw::ConvertOptions base = optionsFor("zarr", files, work/"meta_bad.ims", "");
        Bad k{"three names for two channels", base}; k.o.channelNames = {"a", "b", "c"}; bads.push_back(k);
        k = Bad{"an empty name", base}; k.o.channelNames = {"a", ""}; bads.push_back(k);
        k = Bad{"a color above 1", base}; k.o.channelColors = {{1.f, 0.f, 0.f}, {0.f, 1.5f, 0.f}}; bads.push_back(k);
        k = Bad{"one wavelength for two channels", base}; k.o.emissionWavelengths = {500.f}; bads.push_back(k);
        k = Bad{"a bad start time", base}; k.o.startTime = "2026-02-30 10:00:00"; bads.push_back(k);
        k = Bad{"a negative interval", base}; k.o.timeInterval = -1; bads.push_back(k);
        for(const Bad& b : bads){
            Recorder rec;
            const piw::Status s = piw::convert(b.o, callbacksFor(rec));
            check(s.code == piw::Code::InvalidArgument && !fs::exists(work/"meta_bad.ims") && rec.text.empty(),
                  b.what + ": InvalidArgument before any output (" + s.message + ")");
        }
        const fs::path od = work/"meta_cli_bad"; fs::create_directories(od);
        const std::string common = "-P CamA,CamB -F \"" + native(mdir) + "\" -o \"" + native(od) + "\" -r zarr ";
        for(const char* flags : {"--colors red,purple", "--start-time yesterday", "--time-interval 0", "--channel-names A", "--emission 500,x"}){
            const int rc = runExe(exe, common + flags, work/"meta_cli_bad.log");
            check(rc == 1 && !fs::exists(od/"output.ims"), std::string(flags) + ": exit 1, no output (exit " + std::to_string(rc) + ", " +
                  readText(work/"meta_cli_bad.log").substr(0, 160) + ")");
        }
    }
}

int main(int argc, char** argv){
    if(argc >= 4 && std::string(argv[1]) == "--tiff") return tiffTiming(argv[2], argv[3], argc > 4 ? argv[4] : nullptr);
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_apitest";
    fs::remove_all(work); fs::create_directories(work);
    std::printf("piw %s, default threads %d\n", piw::version(), piw::defaultThreads());
    check(std::strlen(piw::version()) > 0 && piw::defaultThreads() >= 1, "version() and defaultThreads()");

    // Inputs: none of the sizes is a multiple of the chunk sizes.
    const fs::path tdir = work/"tiff", zdir = work/"zarr", mdir = work/"multi";
    fs::create_directories(tdir); fs::create_directories(zdir); fs::create_directories(mdir);
    writeTiff(tdir/"CamA_t0000.tif", 100, 70, 200, 0);
    writeZarr(zdir/"CamA_t0000.zarr", 100, 70, 200, 0);
    for(int c = 0; c < 2; ++c)
        for(int t = 0; t < 2; ++t)
            writeZarr(mdir/(std::string(c ? "CamB" : "CamA") + "_t000" + std::to_string(t) + ".zarr"), 149, 129, 101, 2*c + t);

    // --- 1. piw::convert writes what the executable writes ---
    const std::vector<Case> cases = {
        {"tiff_whole", "tiff", tdir, {"CamA"}, ""},
        {"tiff_C",     "tiff", tdir, {"CamA"}, "200,200,78"},     // Z-slab blocking
        {"zarr_whole", "zarr", zdir, {"CamA"}, ""},
        {"zarr_C",     "zarr", zdir, {"CamA"}, "32,32,32"},       // 3-D blocking
        {"multi_whole","zarr", mdir, {"CamA","CamB"}, ""},        // 2 channels x 2 time points
        {"multi_C",    "zarr", mdir, {"CamA","CamB"}, "32,32,32"},
    };
    auto exeIms = [&](const Case& k){ return work/("exe_" + k.name)/"output.ims"; };
    for(const Case& k : cases){
        std::printf("[%s] executable vs piw::convert\n", k.name.c_str());
        const fs::path od = work/("exe_" + k.name); fs::create_directories(od);
        std::string pats = k.patterns[0]; for(size_t i = 1; i < k.patterns.size(); ++i) pats += "," + k.patterns[i];
        std::string args = "-P " + pats + " -F \"" + native(k.dir) + "\" -o \"" + native(od) + "\" -r " + k.reader + " -v 1,1,1";
        if(!k.processBlock.empty()) args += " -C " + k.processBlock;
        const int rc = runExe(exe, args, work/("exe_" + k.name + ".log"));
        check(rc == 0 && fs::exists(exeIms(k)), "executable exit 0 and output (rc " + std::to_string(rc) + ")");

        Recorder rec;
        const auto files = findFiles(k.dir, k.patterns);
        const fs::path out = work/("api_" + k.name + ".ims");
        const piw::Status s = piw::convert(optionsFor(k.reader, files, out, k.processBlock, k.patterns), callbacksFor(rec));
        check(s.ok(), "piw::convert ok (" + s.message + ")");
        check(sameIms(exeIms(k), out), "same data chunks and channel info as the executable");
        check(progressOk(rec), "progress never decreases and ends at 1 (" + std::to_string(rec.progress.size()) + " calls)");
        size_t nfiles = 0; for(const auto& ch : files) nfiles += ch.size();
        check(countLines(rec.text, "Current File: ") == nfiles && countLines(rec.text, "Voxel Sizes: 1.000000,1.000000,1.000000") == 1,
              "messages: one Current File line per input, the voxel sizes");
    }
    const Case& zarrWhole = cases[2];
    const Case& tiffWhole = cases[0];
    const Case& zarrC = cases[3];

    // --- 2. errors come back as a Status, without output ---
    std::printf("[errors]\n");
    {
        // a .zarr folder without metadata
        const fs::path bad = work/"bad"; fs::create_directories(bad/"CamA_t0000.zarr");
        const fs::path out = work/"api_bad.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(bad, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::ReadFailed && !fs::exists(out), "zarr without metadata: ReadFailed, no output (" + s.message + ")");
    }
    const fs::path trunc = work/"trunc";
    {
        // a truncated chunk: fails in the first read, after the writer was opened
        fs::create_directories(trunc);
        fs::copy(zdir/"CamA_t0000.zarr", trunc/"CamA_t0000.zarr", fs::copy_options::recursive);
        std::vector<fs::path> chunks;
        for(const auto& e : fs::directory_iterator(trunc/"CamA_t0000.zarr"))
            if(e.is_regular_file() && e.path().filename().string()[0] != '.') chunks.push_back(e.path());
        std::sort(chunks.begin(), chunks.end());
        fs::resize_file(chunks.front(), 10);
        const fs::path out = work/"api_trunc.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(trunc, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::ReadFailed && s.message.rfind("could not read ", 0) == 0 && !fs::exists(out),
              "truncated chunk: ReadFailed, output removed (" + s.message + ")");
    }
    const fs::path sizes = work/"sizes";
    {
        // inputs of different sizes are refused before any output: an existing file stays as it is
        fs::create_directories(sizes);
        writeZarr(sizes/"CamA_t0000.zarr", 100, 70, 200, 0);
        writeZarr(sizes/"CamA_t0001.zarr", 100, 70, 100, 1);
        const fs::path out = work/"api_sizes.ims";
        { std::ofstream(out) << "keep"; }
        Recorder rec;
        const auto files = findFiles(sizes, {"CamA"});
        piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument && readText(out) == "keep" && rec.text.empty(),
              "different sizes: InvalidArgument before any output (" + s.message + ")");
        // ... unless a bounding box fits all of them
        piw::ConvertOptions o = optionsFor("zarr", files, work/"api_sizes_box.ims", "");
        o.hasBoundingBox = true;
        const uint64_t box[6] = {10, 20, 30, 60, 50, 90};
        for(int i = 0; i < 6; ++i) o.boundingBox[i] = box[i];
        s = piw::convert(o, callbacksFor(rec));
        check(s.ok() && fs::exists(work/"api_sizes_box.ims"), "different sizes with a bounding box inside both: ok (" + s.message + ")");
        o.boundingBox[5] = 150;   // beyond the second file
        s = piw::convert(o, callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument, "bounding box outside one input: InvalidArgument (" + s.message + ")");
    }
    {
        // channels with different numbers of files
        auto files = findFiles(mdir, {"CamA", "CamB"});
        files[1].pop_back();
        const fs::path out = work/"api_counts.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "channels with 2 and 1 files: InvalidArgument (" + s.message + ")");
    }
    {
        // a pattern without a match, and the selection's order (by full path, folders in order)
        std::vector<std::vector<std::string>> files;
        piw::Status s = piw::findInputFiles({native(zdir)}, {"CamZ"}, files);
        check(s.code == piw::Code::NotFound && s.message == "Given pattern \"CamZ\" was not found in any of the provided folders.",
              "pattern without a match: NotFound (" + s.message + ")");
        const fs::path o1 = work/"order", o2 = work/"order2";
        fs::create_directories(o1); fs::create_directories(o2);
        for(const char* n : {"CamA_t0003.tif", "CamA_t0001.tif", "CamA_t0010.tif", "CamA_t0002.tif", "CamA_t0004.txt", "CamB_t0000.tif"})
            std::ofstream(o1/n) << "x";
        std::ofstream(o2/"CamA_t0000.tif") << "x";
        s = piw::findInputFiles({native(o2), native(o1)}, {"CamA"}, files);
        const std::vector<std::string> want = {native(o1/"CamA_t0001.tif"), native(o1/"CamA_t0002.tif"), native(o1/"CamA_t0003.tif"),
                                               native(o1/"CamA_t0010.tif"), native(o2/"CamA_t0000.tif")};
        check(s.ok() && files.size() == 1 && files[0] == want, "two folders: .tif/.zarr files containing the pattern, sorted by path");
    }

    // --- 3. cancel ---
    std::printf("[cancel]\n");
    for(int poll : {1, 3}){
        Recorder rec; rec.cancelOnPoll = poll;
        const fs::path out = work/("api_cancel" + std::to_string(poll) + ".ims");
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(zdir, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::Cancelled && rec.polls == poll && !fs::exists(out),
              "cancel on poll " + std::to_string(poll) + ": Cancelled, output removed");
    }

    // --- 4. calls in sequence, at once, with numThreads; runCli twice ---
    std::printf("[calls]\n");
    for(int i = 0; i < 2; ++i){
        Recorder rec;
        const fs::path out = work/("api_seq" + std::to_string(i) + ".ims");
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(zdir, {"CamA"}), out, "", {"CamA"}), callbacksFor(rec));
        check(s.ok() && sameIms(exeIms(zarrWhole), out), "conversion " + std::to_string(i + 1) + " in sequence");
    }
    {
        piw::Status s1, s2;
        Recorder r1, r2;
        const auto f1 = findFiles(tdir, {"CamA"}), f2 = findFiles(zdir, {"CamA"});
        const fs::path out1 = work/"api_thread1.ims", out2 = work/"api_thread2.ims";
        std::thread t1([&]{ s1 = piw::convert(optionsFor("tiff", f1, out1, "", {"CamA"}), callbacksFor(r1)); });
        std::thread t2([&]{ s2 = piw::convert(optionsFor("zarr", f2, out2, "32,32,32", {"CamA"}), callbacksFor(r2)); });
        t1.join(); t2.join();
        check(s1.ok() && s2.ok() && sameIms(exeIms(tiffWhole), out1) && sameIms(exeIms(zarrC), out2),
              "two conversions at once from two threads");
    }
    {
        const int before = omp_get_max_threads();
        Recorder rec;
        piw::ConvertOptions o = optionsFor("zarr", findFiles(zdir, {"CamA"}), work/"api_threads2.ims", "", {"CamA"});
        o.numThreads = 2;
        const piw::Status s = piw::convert(o, callbacksFor(rec));
        check(s.ok() && sameIms(exeIms(zarrWhole), work/"api_threads2.ims") && omp_get_max_threads() == before,
              "numThreads 2: same file, the caller's OpenMP thread count unchanged");
    }
    for(int i = 0; i < 2; ++i){
        const fs::path od = work/("runcli" + std::to_string(i));
        fs::create_directories(od);
        std::vector<std::string> args = {"parallelimariswriter", "-P", "CamA", "-F", native(zdir), "-o", native(od),
                                         "-r", "zarr", "-v", "1,1,1"};
        std::vector<char*> argvv;
        for(auto& a : args) argvv.push_back(&a[0]);
        argvv.push_back(nullptr);
        const int rc = piw::runCli((int)args.size(), argvv.data());
        check(rc == 0 && sameIms(exeIms(zarrWhole), od/"output.ims"), "piw::runCli call " + std::to_string(i + 1) + " in this process");
    }

    // --- 5. the command line exits 1 (not a crash) where it used to crash ---
    std::printf("[command line]\n");
    {
        const fs::path od = work/"cli_out"; fs::create_directories(od);
        const std::string o = " -o \"" + native(od) + "\"";
        struct CliCase { std::string what, args; };
        const std::vector<CliCase> cliCases = {
            {"no arguments", ""},
            {"no -F", "-P CamA -r zarr" + o},
            {"no -P", "-F \"" + native(zdir) + "\" -r zarr" + o},
            {"unreadable -v", "-P CamA -F \"" + native(zdir) + "\" -r zarr -v 1" + o},
            {"zarr without metadata", "-P CamA -F \"" + native(work/"bad") + "\" -r zarr" + o},
            {"truncated chunk", "-P CamA -F \"" + native(trunc) + "\" -r zarr" + o},
            {"different sizes", "-P CamA -F \"" + native(sizes) + "\" -r zarr" + o},
        };
        int i = 0;
        for(const CliCase& k : cliCases){
            const fs::path log = work/("cli_" + std::to_string(i++) + ".log");
            const int rc = runExe(exe, k.args, log);
            check(rc == 1 && !fs::exists(od/"output.ims"), k.what + ": exit 1, no output (exit " + std::to_string(rc) + ", " +
                  readText(log).substr(0, 160) + ")");
        }
    }

    metadataTests(exe, work, mdir);

    // --- 6. in-memory volumes and read-back; xOnImarisX; TIFF bounding boxes ---
    testVolumes(work, mdir);
    testAxesAndBoxes(work, tdir, zdir, exe);

    if(gFailures == 0){ std::printf("apitest PASSED\n"); return 0; }
    std::printf("apitest FAILED (%d checks)\n", gFailures);
    return 1;
}
