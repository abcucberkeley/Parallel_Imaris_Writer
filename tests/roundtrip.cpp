// Round-trip test for the parallelimariswriter executable: the voxels that come out
// must be the voxels that went in.
//
// Generates its inputs on the fly (no committed test data), runs the shipped
// executable on them and decodes the .ims with tests/imsutil.h:
//   1. TIFF and zstd/F-order Zarr x uint8/uint16/float32, one 149x129x101 volume
//      (odd sizes: partial chunks on every axis, a dropped voxel per axis at level 1)
//      whose value differs along every axis: exit code 0; level 0 equals the input
//      byte for byte; ImageSizeX/Y/Z, DataSetInfo/Image X/Y/Z and ExtMax0/1/2 match
//      the shape and -v; level 1 is the 2x2x2 box average of level 0 over the axes
//      that halved; the level-0 Histogram counts every voxel; /Thumbnail/Data exists.
//   2. 2 channels x 3 timepoints (TIFF and Zarr, distinct data per file): every
//      (timepoint, channel) dataset equals its own input file.
//   3. Self-test of imsCompare: two conversions of one input compare equal; inputs
//      that differ in one voxel compare different, and the report names the dataset.
// Exits 0 iff every check passes.
//   usage: roundtripTest <path-to-parallelimariswriter> [workdir]
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>
#include <filesystem>

#include "parallelwritetiff.h"   // cpp-tiff
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims decoding and comparison (HDF5 + lz4)

namespace fs = std::filesystem;

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    if(!ok){ std::printf("  FAIL: %s\n", what.c_str()); ++gFailures; }
}
// Prints "OK" after a case if it added no failure.
struct CaseResult {
    int before = gFailures;
    ~CaseResult(){ if(gFailures == before) std::printf("  OK\n"); }
};

struct Shape { uint64_t x, y, z; uint64_t n() const { return x*y*z; } };

template<typename T> static const char* typeName(){
    return std::is_same<T, uint8_t>::value ? "uint8" : std::is_same<T, uint16_t>::value ? "uint16" : "float32";
}
template<typename T> static const char* zarrDtype(){
    return std::is_same<T, uint8_t>::value ? "<u1" : std::is_same<T, uint16_t>::value ? "<u2" : "<f4";
}

// A value that changes along every axis (a swapped or shifted axis changes the data);
// seed makes the files of a multi-file case distinct. Floats are not integers, so the
// order of the level-1 sum matters for them.
template<typename T> static T value(uint64_t x, uint64_t y, uint64_t z, int seed){
    if constexpr(std::is_same<T, uint8_t>::value)  return (T)((x*7 + y*13 + z*29 + (uint64_t)seed*37) % 251);
    if constexpr(std::is_same<T, uint16_t>::value) return (T)((x*7 + y*311 + z*4099 + (uint64_t)seed*977) % 65521);
    return (float)x*0.37f + (float)y*1.13f + (float)z*2.71f + (float)seed*0.5f;
}
template<typename T> static std::vector<T> makeVolume(const Shape& s, int seed){
    std::vector<T> v(s.n());
    for(uint64_t z = 0; z < s.z; z++)
        for(uint64_t y = 0; y < s.y; y++)
            for(uint64_t x = 0; x < s.x; x++) v[x + y*s.x + z*s.x*s.y] = value<T>(x, y, z, seed);
    return v;
}

// Writes <dir>/<name>.tif or <dir>/<name>.zarr (zstd, F order, 64^3 chunks).
template<typename T> static bool writeInput(const std::string& reader, const fs::path& dir, const std::string& name,
                                            const Shape& s, const std::vector<T>& v){
    fs::create_directories(dir);
    const uint64_t bits = sizeof(T)*8;
    if(reader == "tiff"){
        // The TIFF reader returns column-major (rows, columns, z) with the row index
        // fastest, so the volume's first axis is the TIFF height: write it as a
        // column-major (transpose) buffer of width s.y and height s.x.
        const uint16_t sampleFormat = std::is_same<T, float>::value ? 3 : 1;   // IEEE float : unsigned int
        if(writeTiffParallelWrapper(s.y, s.x, s.z, (dir/(name + ".tif")).string().c_str(), v.data(),
                                    bits, 0, 8, "w", true, std::string("none"), sampleFormat)){
            std::printf("  FAIL: TIFF write of %s failed\n", name.c_str()); return false; }
        return true;
    }
    std::vector<uint64_t> shape = {s.x, s.y, s.z}, chunks = {64, 64, 64};
    zarr Zw;
    Zw.set_fileName((dir/(name + ".zarr")).string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype(zarrDtype<T>()); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0, 0, 0}, shape);
    if(parallelWriteZarr(Zw, (void*)v.data(), {0, 0, 0}, shape, shape, bits, false, false, false)){
        std::printf("  FAIL: Zarr write of %s failed: %s\n", name.c_str(), Zw.get_errString().c_str()); return false; }
    return true;
}

// Runs the converter; true if it exits 0 and writes <outdir>/output.ims.
static bool convert(const std::string& exe, const std::string& reader, const std::string& patterns,
                    const fs::path& indir, const fs::path& outdir){
    fs::remove_all(outdir); fs::create_directories(outdir);
    // Native separators: std::system() runs via cmd.exe on Windows, which cannot
    // launch an executable given a forward-slash path (CMake passes C:/... paths).
    const std::string exeN = fs::path(exe).make_preferred().string();
    const std::string inN  = fs::path(indir).make_preferred().string();
    const std::string outN = fs::path(outdir).make_preferred().string();
    std::string cmd = "\"" + exeN + "\" -P " + patterns + " -F \"" + inN + "\" -o \"" + outN +
                      "\" -r " + reader + " -v 0.1,0.2,0.3";
#ifdef _WIN32
    // std::system() runs `cmd /c <cmd>`; when <cmd> starts with a quoted path,
    // cmd.exe mangles it ("The system cannot find the path specified") unless the
    // whole command is wrapped in one more pair of quotes, which cmd then strips.
    cmd = "\"" + cmd + "\"";
#endif
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    int rc = std::system(cmd.c_str());
    std::fflush(stdout);
    check(rc == 0, "converter exit code " + std::to_string(rc));
    bool ok = fs::exists(outdir/"output.ims");
    check(ok, "no output.ims in " + outdir.string());
    return rc == 0 && ok;
}

// Compares decoded voxels with the expected ones and names the first mismatch.
template<typename T> static bool sameVoxels(const std::vector<uint8_t>& got, const std::vector<T>& want,
                                            const Shape& s, const std::string& what){
    if(got.size() != want.size()*sizeof(T)){
        check(false, what + ": decoded " + std::to_string(got.size()) + " bytes, expected " +
                     std::to_string(want.size()*sizeof(T)));
        return false;
    }
    if(std::memcmp(got.data(), want.data(), got.size()) == 0) return true;
    for(size_t i = 0; i < want.size(); i++){
        T g; std::memcpy(&g, got.data() + i*sizeof(T), sizeof(T));
        if(std::memcmp(&g, &want[i], sizeof(T)) != 0){
            char buf[256];
            std::snprintf(buf, sizeof buf, "%s: first difference at (x,y,z)=(%llu,%llu,%llu): got %.9g, expected %.9g",
                          what.c_str(), (unsigned long long)(i % s.x), (unsigned long long)((i / s.x) % s.y),
                          (unsigned long long)(i / (s.x*s.y)), (double)g, (double)want[i]);
            check(false, buf);
            break;
        }
    }
    return false;
}

// ImarisWriter's ResampleBlockT: each lower-level voxel is the mean of the stride
// box, summed as float with z outermost and x innermost, divided by the box size and
// converted back by truncation. A trailing odd voxel along a halved axis is dropped.
template<typename T> static std::vector<T> boxAverage(const std::vector<T>& v, const Shape& s,
                                                      const uint64_t st[3], const Shape& lo){
    std::vector<T> out(lo.n());
    const uint64_t count = st[0]*st[1]*st[2];
    for(uint64_t z = 0; z < lo.z; z++)
        for(uint64_t y = 0; y < lo.y; y++)
            for(uint64_t x = 0; x < lo.x; x++){
                float sum = 0;
                for(uint64_t dz = 0; dz < st[2]; dz++)
                    for(uint64_t dy = 0; dy < st[1]; dy++)
                        for(uint64_t dx = 0; dx < st[0]; dx++)
                            sum += static_cast<float>(v[(x*st[0] + dx) + (y*st[1] + dy)*s.x + (z*st[2] + dz)*s.x*s.y]);
                sum /= count;
                out[x + y*lo.x + z*lo.x*lo.y] = static_cast<T>(sum);
            }
    return out;
}

static double attrNumber(const fs::path& ims, const std::string& obj, const std::string& name){
    std::optional<std::string> s = imsReadAttribute(ims, obj, name);
    if(!s){ check(false, obj + " " + name + " missing"); return std::nan(""); }
    return std::strtod(s->c_str(), nullptr);
}

// Case 1: one volume of type T through one reader.
template<typename T> static void volumeCase(const std::string& exe, const std::string& reader, const fs::path& work){
    const Shape s = {149, 129, 101};
    const double vox[3] = {0.1, 0.2, 0.3};
    const std::string tag = reader + "_" + typeName<T>();
    std::printf("[%s]\n", tag.c_str());
    CaseResult result;
    const std::vector<T> in = makeVolume<T>(s, 0);
    const fs::path indir = work/("in_" + tag), outdir = work/("out_" + tag), ims = outdir/"output.ims";
    if(!writeInput(reader, indir, "CamA_t0000", s, in) || !convert(exe, reader, "CamA", indir, outdir)) return;

    // Level 0: the input, byte for byte.
    ImsVolumeInfo i0;
    std::vector<uint8_t> l0 = imsReadVolume(ims, 0, 0, 0, i0);
    if(!i0.error.empty()){ check(false, "level 0: " + i0.error); return; }
    check(i0.sizeX == s.x && i0.sizeY == s.y && i0.sizeZ == s.z,
          "level 0 ImageSize " + std::to_string(i0.sizeX) + "x" + std::to_string(i0.sizeY) + "x" + std::to_string(i0.sizeZ));
    const H5T_class_t cls = std::is_same<T, float>::value ? H5T_FLOAT : H5T_INTEGER;
    check(i0.elementSize == sizeof(T) && i0.typeClass == cls && !i0.isSigned, "level 0 element type");
    check(i0.chunkCount == i0.expectedChunks, "level 0 has " + std::to_string(i0.chunkCount) + " chunks, " +
                                              std::to_string(i0.expectedChunks) + " expected");
    sameVoxels(l0, in, s, "level 0");

    // Image attributes: size and extent.
    const std::string img = "/DataSetInfo/Image";
    check(attrNumber(ims, img, "X") == (double)s.x && attrNumber(ims, img, "Y") == (double)s.y &&
          attrNumber(ims, img, "Z") == (double)s.z, "DataSetInfo/Image X/Y/Z");
    const uint64_t dims[3] = {s.x, s.y, s.z};
    for(int a = 0; a < 3; a++){
        const std::string k = std::to_string(a);
        const double mx = attrNumber(ims, img, "ExtMax" + k), mn = attrNumber(ims, img, "ExtMin" + k);
        check(std::fabs(mx - (double)dims[a]*vox[a]) < 1e-4, "ExtMax" + k + " = " + std::to_string(mx));
        check(std::fabs(mn) < 1e-6, "ExtMin" + k + " = " + std::to_string(mn));
    }

    // Level 1: box average of level 0 over the axes that halved.
    ImsVolumeInfo i1;
    std::vector<uint8_t> l1 = imsReadVolume(ims, 1, 0, 0, i1);
    if(!i1.error.empty()){ check(false, "level 1: " + i1.error); }
    else {
        const uint64_t lo[3] = {i1.sizeX, i1.sizeY, i1.sizeZ};
        uint64_t st[3]; bool shapeOk = false;
        for(int a = 0; a < 3; a++){
            st[a] = lo[a] < dims[a] ? 2 : 1;
            if(st[a] == 2) shapeOk = true;
            if(lo[a] != dims[a] / st[a]) shapeOk = false;
        }
        check(shapeOk, "level 1 ImageSize " + std::to_string(lo[0]) + "x" + std::to_string(lo[1]) + "x" + std::to_string(lo[2]));
        if(shapeOk){
            const Shape ls = {lo[0], lo[1], lo[2]};
            sameVoxels(l1, boxAverage(in, s, st, ls), ls, "level 1");
        }
    }

    // Histogram counts every voxel; a thumbnail is written.
    std::vector<uint64_t> h = imsReadHistogram(ims, 0, 0, 0);
    uint64_t total = 0;
    for(uint64_t c : h) total += c;
    check(!h.empty() && total == s.n(), "level 0 Histogram sums to " + std::to_string(total) + ", expected " + std::to_string(s.n()));
    check(imsExists(ims, "/Thumbnail/Data"), "/Thumbnail/Data missing");
}

// Case 2: 2 channels x 3 timepoints, every file distinct.
static void multiCase(const std::string& exe, const std::string& reader, const fs::path& work){
    const Shape s = {67, 45, 23};
    const int nC = 2, nT = 3;
    const char* pat[nC] = {"CamA", "CamB"};
    std::printf("[%s 2 channels x 3 timepoints]\n", reader.c_str());
    CaseResult result;
    const fs::path indir = work/("in_multi_" + reader), outdir = work/("out_multi_" + reader), ims = outdir/"output.ims";
    std::vector<std::vector<uint16_t>> in(nC*nT);
    for(int c = 0; c < nC; c++)
        for(int t = 0; t < nT; t++){
            char name[64]; std::snprintf(name, sizeof name, "%s_t%04d", pat[c], t);
            in[c*nT + t] = makeVolume<uint16_t>(s, 1 + c*nT + t);
            if(!writeInput(reader, indir, name, s, in[c*nT + t])) return;
        }
    if(!convert(exe, reader, "CamA,CamB", indir, outdir)) return;
    for(int c = 0; c < nC; c++)
        for(int t = 0; t < nT; t++){
            const std::string what = "timepoint " + std::to_string(t) + " channel " + std::to_string(c);
            ImsVolumeInfo info;
            std::vector<uint8_t> got = imsReadVolume(ims, 0, t, c, info);
            if(!info.error.empty()){ check(false, what + ": " + info.error); continue; }
            check(info.sizeX == s.x && info.sizeY == s.y && info.sizeZ == s.z, what + ": ImageSize");
            sameVoxels(got, in[c*nT + t], s, what);
        }
    check(!imsExists(ims, imsutil::datasetGroup(0, nT, 0)), "an extra timepoint exists");
    check(!imsExists(ims, imsutil::datasetGroup(0, 0, nC)), "an extra channel exists");
    check(imsReadAttribute(ims, "/DataSetInfo/TimeInfo", "DatasetTimePoints").value_or("") == std::to_string(nT),
          "TimeInfo DatasetTimePoints");
}

// Case 3: imsCompare tells equal files from files that differ in one voxel.
static void compareSelfTest(const std::string& exe, const fs::path& work){
    std::printf("[imsCompare self-test]\n");
    CaseResult result;
    const Shape s = {149, 129, 101};
    std::vector<uint16_t> v = makeVolume<uint16_t>(s, 0);
    const fs::path inA = work/"in_cmp_a", inB = work/"in_cmp_b";
    if(!writeInput("zarr", inA, "CamA_t0000", s, v)) return;
    v[101 + 77*s.x + 55*s.x*s.y] ^= 0x0100;   // one voxel
    if(!writeInput("zarr", inB, "CamA_t0000", s, v)) return;
    if(!convert(exe, "zarr", "CamA", inA, work/"out_cmp_a1") || !convert(exe, "zarr", "CamA", inA, work/"out_cmp_a2") ||
       !convert(exe, "zarr", "CamA", inB, work/"out_cmp_b")) return;

    ImsCompareReport same;
    bool eq = imsCompare(work/"out_cmp_a1"/"output.ims", work/"out_cmp_a2"/"output.ims", same);
    imsPrintReport(same);
    check(eq && same.chunks > 0, "two conversions of one input do not compare equal");

    ImsCompareReport diff;
    bool eq2 = imsCompare(work/"out_cmp_a1"/"output.ims", work/"out_cmp_b"/"output.ims", diff);
    imsPrintReport(diff);
    check(!eq2, "inputs that differ in one voxel compare equal");
    bool named = false;
    for(const std::string& d : diff.differences)
        if(d.rfind(imsutil::datasetGroup(0, 0, 0) + "/Data", 0) == 0) named = true;
    check(named, "the report does not name the level-0 dataset");
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_roundtrip";
    fs::remove_all(work); fs::create_directories(work);

    for(const char* reader : {"tiff", "zarr"}){
        volumeCase<uint8_t>(exe, reader, work);
        volumeCase<uint16_t>(exe, reader, work);
        volumeCase<float>(exe, reader, work);
    }
    multiCase(exe, "tiff", work);
    multiCase(exe, "zarr", work);
    compareSelfTest(exe, work);

    if(gFailures == 0){ std::printf("roundtrip PASSED\n"); return 0; }
    std::printf("roundtrip FAILED (%d checks)\n", gFailures);
    return 1;
}
