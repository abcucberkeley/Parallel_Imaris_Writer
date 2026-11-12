// Test of the imscompare tool (tests/imscompare.cpp over tests/imsutil.h).
//
// Converts small synthetic Zarr volumes with the shipped executable (no committed
// data), makes altered copies of the outputs with HDF5, and runs the imscompare binary
// that sits next to this test binary on them, checking its exit code and that its
// output names the difference:
//   1. two conversions of one input: identical (data, --all, --deep, 1 and 3 threads)
//   2. one flipped byte in a level-0 chunk: different, the dataset and chunk named
//   3. one changed /DataSetInfo attribute: identical by default, different with --all
//   4. one missing chunk: different ("only in a"); --deep reports the missing chunk
//   5. a changed /Thumbnail: exit 3 with --all (identical except /Thumbnail), 0 with
//      --no-thumbnail
//   6. --map: timepoint 1 of a 2-timepoint file equals a 1-timepoint file of the same
//      volume; unmapped or wrongly mapped, they differ
//   7. a link file whose Channel groups are external links into a part: identical data
//   8. --deep: uint8, uint16 and float32 outputs and the 2-timepoint file check clean;
//      a truncated chunk and an altered Histogram are reported
//   9. usage errors and missing files: exit 2
// Exits 0 iff every check passes.
//   usage: imscompareTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>
#include <filesystem>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims decoding and comparison (HDF5 + lz4)

namespace fs = std::filesystem;
using imsutil::Hid;

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    if(!ok){ std::printf("  FAIL: %s\n", what.c_str()); ++gFailures; }
}
struct CaseResult {
    int before = gFailures;
    ~CaseResult(){ if(gFailures == before) std::printf("  OK\n"); }
};

struct Shape { uint64_t x, y, z; uint64_t n() const { return x*y*z; } };

template<typename T> static const char* zarrDtype(){
    return std::is_same<T, uint8_t>::value ? "<u1" : std::is_same<T, uint16_t>::value ? "<u2" : "<f4";
}
template<typename T> static std::vector<T> makeVolume(const Shape& s, int seed){
    std::vector<T> v(s.n());
    for(uint64_t z = 0; z < s.z; z++)
        for(uint64_t y = 0; y < s.y; y++)
            for(uint64_t x = 0; x < s.x; x++){
                T& o = v[x + y*s.x + z*s.x*s.y];
                if constexpr(std::is_same<T, uint8_t>::value) o = (T)((x*7 + y*13 + z*29 + (uint64_t)seed*37) % 251);
                else if constexpr(std::is_same<T, uint16_t>::value) o = (T)((x*7 + y*311 + z*4099 + (uint64_t)seed*977) % 65521);
                else o = (float)x*0.37f + (float)y*1.13f + (float)z*2.71f + (float)seed*0.5f;
            }
    return v;
}

// Writes <dir>/<name>.zarr (zstd, F order, 64^3 chunks).
template<typename T> static bool writeZarr(const fs::path& dir, const std::string& name, const Shape& s, const std::vector<T>& v){
    fs::create_directories(dir);
    std::vector<uint64_t> shape = {s.x, s.y, s.z}, chunks = {64, 64, 64};
    zarr Zw;
    Zw.set_fileName((dir/(name + ".zarr")).string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype(zarrDtype<T>()); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0, 0, 0}, shape);
    if(parallelWriteZarr(Zw, (void*)v.data(), {0, 0, 0}, shape, shape, sizeof(T)*8, false, false, false)){
        check(false, "Zarr write of " + name + " failed: " + Zw.get_errString()); return false; }
    return true;
}

// Runs a command line through the shell; returns the exit code (-1 if it did not exit).
static int run(std::string cmd){
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::fflush(stdout);
    int rc = std::system(cmd.c_str());
#ifdef _WIN32
    return rc;
#else
    return (rc != -1 && WIFEXITED(rc)) ? WEXITSTATUS(rc) : -1;
#endif
}
static std::string q(const fs::path& p){ return "\"" + fs::path(p).make_preferred().string() + "\""; }

// Converts <indir> (pattern CamA) to <outdir>/output.ims.
static bool convert(const std::string& exe, const fs::path& indir, const fs::path& outdir){
    fs::remove_all(outdir); fs::create_directories(outdir);
    std::string cmd = q(exe) + " -P CamA -F " + q(indir) + " -o " + q(outdir) + " -r zarr -v 0.1,0.2,0.3";
    std::printf("  $ %s\n", cmd.c_str());
    int rc = run(cmd);
    bool ok = rc == 0 && fs::exists(outdir/"output.ims");
    check(ok, "conversion of " + indir.string() + " failed (exit " + std::to_string(rc) + ")");
    return ok;
}

static fs::path gTool, gWork;

// Runs imscompare <args>; checks the exit code and that the output holds every string in want.
static void expect(const std::string& args, int wantRc, const std::vector<std::string>& want = {}){
    const fs::path log = gWork/"imscompare.log";
    std::printf("  $ imscompare %s\n", args.c_str());
    int rc = run(q(gTool) + " " + args + " > " + q(log) + " 2>&1");
    std::ifstream in(log);
    std::stringstream ss; ss << in.rdbuf();
    const std::string out = ss.str();
    std::istringstream lines(out);
    for(std::string l; std::getline(lines, l); ) std::printf("    | %s\n", l.c_str());
    check(rc == wantRc, "imscompare " + args + ": exit " + std::to_string(rc) + ", expected " + std::to_string(wantRc));
    for(const std::string& w : want) check(out.find(w) != std::string::npos, "imscompare " + args + ": output lacks \"" + w + "\"");
}

// --- altering copies of a file -------------------------------------------------------

// The lz4 filter (id 32004) as a stub, so datasets that use it can be opened for
// writing and created; chunks are only written raw, so it is never run.
static size_t lz4Stub(unsigned, size_t, const unsigned[], size_t, size_t*, void**){ return 0; }
static void registerLz4Stub(){
    if(H5Zfilter_avail(imsutil::kFilterLZ4) > 0) return;
    H5Z_class2_t c = {H5Z_CLASS_T_VERS, imsutil::kFilterLZ4, 1, 1, "lz4 stub", nullptr, nullptr, lz4Stub};
    H5Zregister(&c);
}

// A plain stream copy: std::filesystem::copy_file aborts the process on macOS (GCC 13).
static fs::path copyOf(const fs::path& src, const std::string& name){
    fs::path dst = gWork/(name + ".ims");
    std::ifstream in(src, std::ios::binary); std::ofstream out(dst, std::ios::binary | std::ios::trunc);
    if(!in || !out){ std::fprintf(stderr, "cannot copy %s\n", src.string().c_str()); std::exit(1); }
    out << in.rdbuf();
    return dst;
}
static Hid openRW(const fs::path& p){ return Hid(H5Fopen(p.string().c_str(), H5F_ACC_RDWR, H5P_DEFAULT), H5Fclose); }

// Chunk k of a Data dataset, raw; then writes back what edit makes of it.
template<class Edit> static bool editChunk(const fs::path& file, const std::string& dsPath, size_t k, Edit edit){
    Hid f = openRW(file);
    Hid d(f.valid() ? H5Dopen2(f, dsPath.c_str(), H5P_DEFAULT) : -1, H5Dclose);
    std::vector<imsutil::Chunk> chunks;
    std::vector<uint8_t> raw;
    if(!d.valid() || !imsutil::listChunks(d, chunks) || k >= chunks.size() || !imsutil::readRawChunk(d, chunks[k], raw)) return false;
    edit(raw);
    return H5Dwrite_chunk(d, H5P_DEFAULT, chunks[k].mask, chunks[k].off, raw.size(), raw.data()) >= 0;
}

// Recreates a Data dataset with every chunk but the last one.
static bool dropLastChunk(const fs::path& file, const std::string& gp){
    Hid f = openRW(file);
    Hid g(f.valid() ? H5Gopen2(f, gp.c_str(), H5P_DEFAULT) : -1, H5Gclose);
    Hid d(g.valid() ? H5Dopen2(g, "Data", H5P_DEFAULT) : -1, H5Dclose);
    if(!d.valid()) return false;
    std::vector<imsutil::Chunk> chunks;
    if(!imsutil::listChunks(d, chunks) || chunks.size() < 2) return false;
    chunks.pop_back();
    std::vector<std::vector<uint8_t>> raw(chunks.size());
    for(size_t k = 0; k < chunks.size(); ++k) if(!imsutil::readRawChunk(d, chunks[k], raw[k])) return false;
    Hid t(H5Dget_type(d), H5Tclose), s(H5Dget_space(d), H5Sclose), pl(H5Dget_create_plist(d), H5Pclose);
    Hid dcpl(H5Pcopy(pl), H5Pclose);
    H5Pset_alloc_time(dcpl, H5D_ALLOC_TIME_INCR);
    d.reset();
    if(H5Ldelete(g, "Data", H5P_DEFAULT) < 0) return false;
    Hid n(H5Dcreate2(g, "Data", t, s, H5P_DEFAULT, dcpl, H5P_DEFAULT), H5Dclose);
    if(!n.valid()) return false;
    for(size_t k = 0; k < chunks.size(); ++k)
        if(H5Dwrite_chunk(n, H5P_DEFAULT, chunks[k].mask, chunks[k].off, raw[k].size(), raw[k].data()) < 0) return false;
    return true;
}

// Replaces an attribute by a string in the .ims style (1-D array of 1-byte characters).
static bool setStringAttr(const fs::path& file, const std::string& obj, const std::string& name, const std::string& value){
    Hid f = openRW(file);
    Hid o(f.valid() ? H5Oopen(f, obj.c_str(), H5P_DEFAULT) : -1, H5Oclose);
    if(!o.valid()) return false;
    if(H5Aexists(o, name.c_str()) > 0 && H5Adelete(o, name.c_str()) < 0) return false;
    hsize_t n = value.size();
    Hid sp(H5Screate_simple(1, &n, nullptr), H5Sclose), ty(H5Tcopy(H5T_C_S1), H5Tclose);
    H5Tset_size(ty, 1);
    Hid a(H5Acreate2(o, name.c_str(), ty, sp, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
    return a.valid() && H5Awrite(a, ty, value.data()) >= 0;
}

// Adds 1 to the first non-empty bin of a uint64 dataset, or flips the low bit of the
// first byte of any other (small) dataset.
static bool alterSmallDataset(const fs::path& file, const std::string& path, bool histogram){
    Hid f = openRW(file);
    Hid d(f.valid() ? H5Dopen2(f, path.c_str(), H5P_DEFAULT) : -1, H5Dclose);
    if(!d.valid()) return false;
    if(histogram){
        Hid s(H5Dget_space(d), H5Sclose);
        std::vector<uint64_t> h((size_t)H5Sget_simple_extent_npoints(s), 0);
        if(H5Dread(d, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, h.data()) < 0) return false;
        for(uint64_t& v : h) if(v){ ++v; break; }
        return H5Dwrite(d, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, h.data()) >= 0;
    }
    std::vector<char> bytes; size_t es = 0;
    if(!imsutil::readDatasetBytes(d, bytes, es) || bytes.empty()) return false;
    bytes[0] ^= 1;
    Hid t(H5Dget_type(d), H5Tclose), mt(H5Tget_native_type(t, H5T_DIR_ASCEND), H5Tclose);
    return H5Dwrite(d, mt, H5S_ALL, H5S_ALL, H5P_DEFAULT, bytes.data()) >= 0;
}

// A file whose /DataSet/ResolutionLevel r/TimePoint 0/Channel 0 are external links to
// the same groups of part (a link master + parts/ output), next to it.
static fs::path makeLinkFile(const fs::path& part, int levels){
    fs::path link = part.parent_path()/"link.ims";
    Hid f(H5Fcreate(link.string().c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT), H5Fclose);
    Hid lcpl(H5Pcreate(H5P_LINK_CREATE), H5Pclose);
    H5Pset_create_intermediate_group(lcpl, 1);
    bool ok = f.valid();
    for(int r = 0; ok && r < levels; ++r){
        const std::string gp = imsutil::datasetGroup(r, 0, 0);
        ok = H5Lcreate_external(part.filename().string().c_str(), gp.c_str(), f, gp.c_str(), lcpl, H5P_DEFAULT) >= 0;
    }
    check(ok, "cannot create the link file");
    return link;
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    gWork = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_imscompare";
    fs::remove_all(gWork); fs::create_directories(gWork);
#ifdef _WIN32
    gTool = fs::absolute(argv[0]).parent_path()/"imscompare.exe";
#else
    gTool = fs::absolute(argv[0]).parent_path()/"imscompare";
#endif
    if(!fs::exists(gTool)){ std::printf("FAIL: %s not found\n", gTool.string().c_str()); return 1; }
    registerLz4Stub();

    std::printf("[converting the inputs]\n");
    const Shape s = {149, 129, 101};
    bool ok = writeZarr(gWork/"in_a", "CamA_t0000", s, makeVolume<uint16_t>(s, 0)) &&
              writeZarr(gWork/"in_u8", "CamA_t0000", s, makeVolume<uint8_t>(s, 0)) &&
              writeZarr(gWork/"in_f32", "CamA_t0000", s, makeVolume<float>(s, 0)) &&
              writeZarr(gWork/"in_t2", "CamA_t0000", s, makeVolume<uint16_t>(s, 1)) &&
              writeZarr(gWork/"in_t2", "CamA_t0001", s, makeVolume<uint16_t>(s, 2)) &&
              writeZarr(gWork/"in_s", "CamA_t0000", s, makeVolume<uint16_t>(s, 2));
    ok = ok && convert(exe, gWork/"in_a", gWork/"out_a1") && convert(exe, gWork/"in_a", gWork/"out_a2") &&
         convert(exe, gWork/"in_u8", gWork/"out_u8") && convert(exe, gWork/"in_f32", gWork/"out_f32") &&
         convert(exe, gWork/"in_t2", gWork/"out_t2") && convert(exe, gWork/"in_s", gWork/"out_s");
    if(!ok){ std::printf("imscompare test FAILED (inputs)\n"); return 1; }
    const fs::path a1 = gWork/"out_a1"/"output.ims", a2 = gWork/"out_a2"/"output.ims";
    const std::string d0 = imsutil::datasetGroup(0, 0, 0);
    std::vector<imsutil::Chunk> level0;
    {
        imsutil::Hid f = imsutil::openFile(a1);
        imsutil::Hid d(H5Dopen2(f, (d0 + "/Data").c_str(), H5P_DEFAULT), H5Dclose);
        imsutil::listChunks(d, level0);
    }
    check(level0.size() >= 2, "level 0 has " + std::to_string(level0.size()) + " chunks; the test needs at least 2");
    const std::string mid = "(z,y,x)=(" + std::to_string((unsigned long long)level0[level0.size()/2].off[0]) + "," +
                            std::to_string((unsigned long long)level0[level0.size()/2].off[1]) + "," +
                            std::to_string((unsigned long long)level0[level0.size()/2].off[2]) + ")";

    { std::printf("[identical files]\n"); CaseResult cr;
      expect(q(a1) + " " + q(a2), 0, {"imscompare: IDENTICAL"});
      expect("--all --deep --threads 3 " + q(a1) + " " + q(a2), 0, {"imscompare: IDENTICAL", "DEEP CHECK CLEAN"});
      expect("--threads 1 --all " + q(a1) + " " + q(a2), 0);
      ImsCompareReport r; ImsCompareOptions o; o.threads = 3;
      check(imsCompare(a1, a2, r, o) && r.chunks > 0, "imsCompare with 3 threads: not identical");
    }
    { std::printf("[one flipped chunk byte]\n"); CaseResult cr;
      fs::path f = copyOf(a1, "flip");
      check(editChunk(f, d0 + "/Data", level0.size()/2, [](std::vector<uint8_t>& b){ b[b.size()/2] ^= 0x20; }), "cannot flip a byte");
      for(const char* th : {"1", "4"})
          expect(std::string("--threads ") + th + " " + q(a1) + " " + q(f), 1,
                 {d0 + "/Data: ", "1 differ, 0 only in a, 0 only in b", "chunk " + mid + " differs (same size", "imscompare: DIFFERENT"});
    }
    { std::printf("[one changed attribute]\n"); CaseResult cr;
      fs::path f = copyOf(a1, "attr");
      check(setStringAttr(f, "/DataSetInfo/Channel 0", "ColorRange", "1.000 2.000"), "cannot change an attribute");
      expect(q(a1) + " " + q(f), 0, {"only the Data chunks were compared"});
      expect("--all " + q(a1) + " " + q(f), 1, {"/DataSetInfo/Channel 0 attribute ColorRange: a=\"", "b=\"1.000 2.000\""});
    }
    { std::printf("[one missing chunk]\n"); CaseResult cr;
      fs::path f = copyOf(a1, "drop");
      check(dropLastChunk(f, d0), "cannot drop a chunk");
      expect(q(a1) + " " + q(f), 1, {d0 + "/Data: ", "0 differ, 1 only in a, 0 only in b", "only in a"});
      expect("--deep " + q(f), 1, {d0 + "/Data: " + std::to_string(level0.size() - 1) + " chunks stored", "DEEP CHECK FAILED"});
    }
    { std::printf("[a changed thumbnail]\n"); CaseResult cr;
      fs::path f = copyOf(a1, "thumb");
      check(alterSmallDataset(f, "/Thumbnail/Data", false), "cannot change the thumbnail");
      expect("--all " + q(a1) + " " + q(f), 3, {"DIFFERS (thumbnail): /Thumbnail/Data: values differ", "IDENTICAL except /Thumbnail"});
      expect("--all --no-thumbnail " + q(a1) + " " + q(f), 0, {"/Thumbnail was not compared"});
      expect(q(a1) + " " + q(f), 0);
    }
    { std::printf("[--map]\n"); CaseResult cr;
      const fs::path t2 = gWork/"out_t2"/"output.ims", one = gWork/"out_s"/"output.ims";
      expect(q(t2) + " " + q(one), 1, {"TimePoint 1: only in a"});
      expect("--map 1,0=0,0 " + q(t2) + " " + q(one), 0, {"imscompare: IDENTICAL"});
      expect("--all --map 1,0=0,0 " + q(t2) + " " + q(one), 0, {"only the mapped datasets were compared"});
      expect("--map 0,0=0,0 " + q(t2) + " " + q(one), 1, {d0 + "/Data: "});
      expect("--map 1,0=0,0 --map 5,0=7,0 " + q(t2) + " " + q(one), 1, {"timepoint 5 channel 0 of a / timepoint 7 channel 0 of b: in neither file"});
      expect("--map 5,0=0,0 " + q(t2) + " " + q(one), 1, {"TimePoint 5/Channel 0 (b: /DataSet/ResolutionLevel 0/TimePoint 0/Channel 0): only in b"});
    }
    { std::printf("[external links]\n"); CaseResult cr;
      fs::path link = makeLinkFile(a1, (int)imsutil::childNames(imsutil::openFile(a1), "/DataSet").size());
      expect(q(a2) + " " + q(link), 0, {"imscompare: IDENTICAL"});
      expect("--deep " + q(link), 0, {"DEEP CHECK CLEAN"});
    }
    { std::printf("[--deep]\n"); CaseResult cr;
      expect("--deep " + q(a1), 0, {"DEEP CHECK CLEAN"});
      expect("--deep " + q(gWork/"out_u8"/"output.ims"), 0, {"DEEP CHECK CLEAN"});
      expect("--deep " + q(gWork/"out_f32"/"output.ims"), 0, {"DEEP CHECK CLEAN", "only their voxel totals were checked"});
      expect("--deep --threads 2 " + q(gWork/"out_t2"/"output.ims"), 0, {"DEEP CHECK CLEAN"});
      fs::path f = copyOf(a1, "truncated");
      check(editChunk(f, d0 + "/Data", 0, [](std::vector<uint8_t>& b){ b.resize(b.size()/2); }), "cannot truncate a chunk");
      expect("--deep " + q(f), 1, {d0 + "/Data: 1 of " + std::to_string(level0.size()) + " chunks do not decode", "DEEP CHECK FAILED"});
      fs::path h = copyOf(a1, "histogram");
      check(alterSmallDataset(h, d0 + "/Histogram", true), "cannot change a histogram");
      expect("--deep " + q(h), 1, {d0 + "/Histogram: bin ", "the decoded data gives"});
      // A deep failure on a.ims outranks an identical comparison.
      expect("--deep " + q(h) + " " + q(h), 1, {"imscompare: IDENTICAL", "DEEP CHECK FAILED"});
    }
    { std::printf("[usage errors]\n"); CaseResult cr;
      expect("", 2);
      expect(q(a1), 2);
      expect(q(a1) + " " + q(gWork/"absent.ims"), 2, {"no such file"});
      expect("--map 1,0 " + q(a1) + " " + q(a2), 2, {"--map needs"});
      expect("--threads 0 " + q(a1) + " " + q(a2), 2);
      expect("--frobnicate " + q(a1) + " " + q(a2), 2, {"unknown option"});
    }

    if(gFailures == 0){ std::printf("imscompare test PASSED\n"); return 0; }
    std::printf("imscompare test FAILED (%d checks)\n", gFailures);
    return 1;
}
