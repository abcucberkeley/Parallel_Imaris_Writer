// Mono test: the single-file cluster output of the time-series path (setup or monoinit, monoplace,
// monostatus, monofinish), on synthetic data, through the shipped executable.
//
//   main run : 2 channels x 3 timepoints of an odd-sized volume (3 resolution levels), every timepoint and
//              channel with its own intensity range. The reference is ONE single-node conversion of all six
//              volumes; the parts are six single-(timepoint, channel) conversions; setup writes the manifest.
//              The parts are placed in shuffled order, two different ones at once and one twice at once (a
//              duplicate task). A truncated part fails; monostatus and monofinish then exit 3 and name the
//              missing part; the journal stays open and the part is placed. Placing a part again leaves the
//              file size and its sidecar unchanged. The parts are deleted, then monofinish --verify. Checks:
//              the file equals the reference (imsCompare: every chunk, histogram, attribute and DataSetInfo
//              group; only /Thumbnail is missing), every link is a hard link, the links and attributes of
//              every object are stored in the reference's order (native-order listings match; see nativeOrder
//              for the one exception), a placement after the finish fails, and finishing again reports
//              "already finished".
//   crop run : monoinit + 2 timepoints x 1 channel converted with -B, an image small enough that a single-node
//              conversion of 2 timepoints chunks level 0 differently from the 1-timepoint parts: monofinish
//              re-chunks it, and the file equals the cropped single-node conversion. Refused: a part of another
//              geometry, a uint8 part, a file with two channels.
//   usage: monoTest <parallelimariswriter> <workdir>
// Exit 0 = pass, 1 = fail, 77 = skipped (Windows: the single-file path is not supported there).
#ifdef _WIN32
#include <cstdio>
int main(){ std::printf("monoTest: skipped (the single-file cluster output is not supported on Windows)\n"); return 77; }
#else
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "zarr.h"                // cpp-zarr (also pulls in nlohmann::json)
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims comparison (HDF5 + lz4)

namespace fs = std::filesystem;

static int gFail = 0;
#define CHECK(cond, ...) do{ if(!(cond)){ gFail++; std::printf("  FAIL (line %d): ", __LINE__); std::printf(__VA_ARGS__); std::printf("\n"); } }while(0)

// Odd on every axis: edge chunks at every level; 3 levels (the second is still above the 1 Mi-voxel
// pyramid limit of uint16).
static const uint64_t X = 301, Y = 259, Z = 201;
static const char* VOXEL = "0.108,0.108,0.25";

// A different intensity range per (timepoint, channel), so the multi-timepoint ColorRange and every level's
// histogram trim are exercised; not uniform, so the color-range mode search has something to find.
template<class T> static std::vector<T> makeVolume(uint64_t nx, uint64_t ny, uint64_t nz, int t, int c, uint32_t top){
    std::vector<T> buf(nx * ny * nz);
    const uint32_t range = top ? top : 900 + 1700 * t + 2300 * c;
    for(uint64_t z = 0; z < nz; z++)
        for(uint64_t y = 0; y < ny; y++)
            for(uint64_t x = 0; x < nx; x++){
                uint32_t v = (uint32_t)((x * 7 + y * 13 + z * 17 + (uint64_t)(t * 31 + c * 137)) % range);
                if((x / 16 + y / 8 + z / 4) % 5 == 0) v = v / 3;   // a second, lower population
                buf[x + y * nx + z * nx * ny] = (T)(v + 1);
            }
    return buf;
}
template<class T> static void writeZarr(const fs::path& path, const std::vector<T>& buf, uint64_t nx, uint64_t ny, uint64_t nz, const char* dtype){
    std::vector<uint64_t> shape = {nx, ny, nz}, chunks = {64, 64, 64};
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(1); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype(dtype); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0, 0, 0}, shape);
    if(parallelWriteZarr(Zw, (void*)buf.data(), {0, 0, 0}, shape, shape, sizeof(T) * 8, false, false, false)){
        std::fprintf(stderr, "Zarr write failed: %s\n", Zw.get_errString().c_str()); std::exit(1); }
}

static std::string q(const fs::path& p){ return "\"" + p.string() + "\""; }
// C stdio, not iostreams or std::filesystem copies: on macOS this binary binds part of the C++ runtime
// to the copy bundled (and exported) by libcppZarr, and iostream calls that mix the two copies abort.
static bool readBytes(const fs::path& p, std::string& b){
    FILE* f = std::fopen(p.string().c_str(), "rb"); if(!f) return false;
    b.clear(); char buf[1 << 16]; size_t n;
    while((n = std::fread(buf, 1, sizeof buf, f)) > 0) b.append(buf, n);
    const bool ok = !std::ferror(f); std::fclose(f); return ok;
}
static bool copyFile(const fs::path& from, const fs::path& to){
    std::string b; if(!readBytes(from, b)) return false;
    FILE* f = std::fopen(to.string().c_str(), "wb"); if(!f) return false;
    const bool ok = std::fwrite(b.data(), 1, b.size(), f) == b.size();
    return std::fclose(f) == 0 && ok;
}
// Runs a shell command; its exit code (-1 if it did not exit normally). out: its stdout + stderr.
static int run(const std::string& cmd, std::string* out = nullptr, const fs::path& log = fs::path()){
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    std::string full = cmd;
    if(out) full = "(" + cmd + ") > " + q(log) + " 2>&1";
    int rc = std::system(full.c_str());
    if(out){
        readBytes(log, *out);
        std::printf("%s", out->c_str());
    }
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
}
static uint64_t fileSize(const fs::path& p){ struct stat st; return stat(p.c_str(), &st) == 0 ? (uint64_t)st.st_size : 0; }

// ---- native-order (storage order) listings: the links of every group and the attributes of every object ----
// Compared per object path. Two exceptions, compared as sets: the links of the /DataSet/.../Channel c groups
// (Data, Histogram, Histogram1024), whose storage order depends on how ImarisWriter's writer thread interleaves
// them with the growing chunk indexes (three identical single-node conversions already differ there), and the
// links of /DataSetInfo, whose order in a single-node file depends on the file's allocation history when its
// metadata is written (two inputs give two orders for the same creation order).
struct Listing { std::map<std::string, std::string> attrs, links; std::vector<std::string> notHard; };
static herr_t attrCb(hid_t, const char* name, const H5A_info_t*, void* ud){ ((std::vector<std::string>*)ud)->push_back(name); return 0; }
struct LinkList { std::vector<std::string> names; std::vector<int> types; };
static herr_t linkCb(hid_t, const char* name, const H5L_info2_t* info, void* ud){
    LinkList& l = *(LinkList*)ud; l.names.push_back(name); l.types.push_back((int)info->type); return 0; }
static std::string joined(const std::vector<std::string>& v){ std::string s; for(auto& n : v) s += " " + n; return s; }
static void walkNative(hid_t f, const std::string& path, Listing& w){
    imsutil::Hid o(H5Oopen(f, path.c_str(), H5P_DEFAULT), H5Oclose);
    if(!o.valid()){ w.attrs[path] = "(cannot open)"; return; }
    std::vector<std::string> attrs; hsize_t idx = 0;
    H5Aiterate2(o, H5_INDEX_NAME, H5_ITER_NATIVE, &idx, attrCb, &attrs);
    w.attrs[path] = joined(attrs);
    H5O_info2_t info; H5Oget_info3(o, &info, H5O_INFO_BASIC);
    if(info.type != H5O_TYPE_GROUP) return;
    LinkList l;
    H5Literate2(o, H5_INDEX_NAME, H5_ITER_NATIVE, nullptr, linkCb, &l);
    std::vector<std::string> names;
    for(size_t i = 0; i < l.names.size(); i++){
        if(path == "/" && l.names[i] == "Thumbnail") continue;   // not written by the cluster path yet (B-8)
        names.push_back(l.names[i]);
        const std::string child = (path == "/" ? "/" : path + "/") + l.names[i];
        if(l.types[i] != H5L_TYPE_HARD) w.notHard.push_back(child); else walkNative(f, child, w);
    }
    if((path.rfind("/DataSet/", 0) == 0 && path.find("/Channel ") != std::string::npos) || path == "/DataSetInfo") std::sort(names.begin(), names.end());
    w.links[path] = joined(names);
}
static bool nativeOrder(const fs::path& ref, const fs::path& mono){
    imsutil::QuietErrors quiet;
    imsutil::Hid a = imsutil::openFile(ref), b = imsutil::openFile(mono);
    if(!a.valid() || !b.valid()){ std::printf("  cannot open the files\n"); return false; }
    Listing la, lb; walkNative(a, "/", la); walkNative(b, "/", lb);
    bool ok = true; size_t differ = 0;
    for(auto& n : lb.notHard){ std::printf("  not a hard link: %s\n", n.c_str()); ok = false; }
    auto cmp = [&](const char* what, const std::map<std::string, std::string>& x, const std::map<std::string, std::string>& y){
        std::set<std::string> paths; for(auto& e : x) paths.insert(e.first); for(auto& e : y) paths.insert(e.first);
        for(auto& p : paths){ auto i = x.find(p), j = y.find(p);
            std::string vx = i == x.end() ? "(missing)" : i->second, vy = j == y.end() ? "(missing)" : j->second;
            if(vx != vy){ ok = false; if(differ++ < 10) std::printf("  %s of %s differ:\n    reference:%s\n    mono:     %s\n", what, p.c_str(), vx.c_str(), vy.c_str()); } } };
    cmp("attributes", la.attrs, lb.attrs); cmp("links", la.links, lb.links);
    std::printf("  native order of %zu objects' attributes and %zu groups' links compared: %zu differ%s\n", lb.attrs.size(), lb.links.size(), differ,
                lb.notHard.empty() ? "; all hard links" : "");
    return ok;
}

static bool compareFiles(const fs::path& ref, const fs::path& mono, const char* what){
    std::printf("[compare] %s: reference vs single file\n", what);
    ImsCompareOptions opt; ImsCompareReport report;
    imsCompare(ref, mono, report, opt);
    imsPrintReport(report);
    bool ok = report.identicalExceptThumbnail();
    CHECK(ok, "%s: the single file differs from the single-node conversion", what);
    return ok;
}

int main(int argc, char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if(argc < 3){ std::fprintf(stderr, "usage: %s <parallelimariswriter> <workdir>\n", argv[0]); return 2; }
    const std::string E = q(argv[1]);
    const fs::path work = argv[2];
    fs::remove_all(work); fs::create_directories(work);
    const fs::path log = work / "cmd.log";

    // ---------------- main run: 2 channels x 3 timepoints ----------------
    const int nT = 3, nC = 2; const char* cam[2] = {"CamA", "CamB"};
    fs::path zdir = work / "zarr"; fs::create_directories(zdir);
    for(int c = 0; c < nC; c++) for(int t = 0; t < nT; t++){
        char n[64]; std::snprintf(n, sizeof n, "%s_t%04d.zarr", cam[c], t);
        writeZarr(zdir / n, makeVolume<uint16_t>(X, Y, Z, t, c, 0), X, Y, Z, "<u2");
    }
    std::printf("[reference] one single-node conversion of all %d volumes\n", nT * nC);
    fs::path refDir = work / "ref"; fs::create_directories(refDir);
    if(run(E + " -P CamA,CamB -F " + q(zdir) + " -o " + q(refDir) + " -r zarr -v " + VOXEL) != 0 || !fs::exists(refDir / "output.ims")){
        std::printf("monotest FAILED: reference conversion\n"); return 1; }
    std::printf("[parts] one conversion per (timepoint, channel)\n");
    fs::path parts = work / "parts"; fs::create_directories(parts);
    auto part = [&](int t, int c){ return parts / ("part_t" + std::to_string(t) + "_c" + std::to_string(c) + ".ims"); };
    for(int t = 0; t < nT; t++) for(int c = 0; c < nC; c++){
        char pat[64]; std::snprintf(pat, sizeof pat, "%s_t%04d", cam[c], t);
        if(run(E + " -P " + pat + " -F " + q(zdir) + " -o " + q(parts) + " -n part_t" + std::to_string(t) + "_c" + std::to_string(c) +
               " -r zarr -v " + VOXEL) != 0 || !fs::exists(part(t, c))){ std::printf("monotest FAILED: part conversion\n"); return 1; }
    }
    fs::path keep = work / "keep"; fs::create_directories(keep);
    CHECK(copyFile(part(0, 0), keep / "part_t0_c0.ims"), "cannot copy a part");

    fs::path wd = work / "wd";
    std::printf("[setup]\n");
    CHECK(run(E + " setup " + q(zdir) + " CamA,CamB " + q(wd) + " 128,128,128 " + VOXEL) == 0, "setup failed");
    auto place = [&](int t, int c, const fs::path& p){
        return E + " monoplace " + q(wd) + " " + std::to_string(t) + " " + std::to_string(c) + " " + q(p) + " --threads 3"; };
    const fs::path partial = wd / "output.ims.partial", out = work / "mono.ims";
    std::string o;

    std::printf("[place] shuffled; two different parts at once; one part twice at once\n");
    CHECK(run(place(1, 0, part(1, 0))) == 0, "monoplace t1 c0");
    CHECK(run(place(0, 1, part(0, 1)) + " & p=$!; " + place(2, 0, part(2, 0)) + "; r=$?; wait $p || exit 1; exit $r") == 0, "two concurrent monoplace");
    CHECK(run(place(0, 0, part(0, 0)) + " & p=$!; " + place(0, 0, part(0, 0)) + "; r=$?; wait $p || exit 1; exit $r") == 0, "a duplicate pair of monoplace");
    CHECK(run(place(1, 1, part(1, 1))) == 0, "monoplace t1 c1");

    std::printf("[refused] a truncated part, a timepoint outside the manifest\n");
    fs::path trunc = work / "trunc.ims";
    CHECK(copyFile(part(2, 1), trunc) && ::truncate(trunc.c_str(), (off_t)(fileSize(part(2, 1)) / 2)) == 0, "cannot make a truncated part");
    CHECK(run(place(2, 1, trunc), &o, log) == 1, "a truncated part was placed");
    CHECK(!fs::exists(wd / "mono" / "P_C1_T2.idx"), "the truncated part left a sidecar");
    CHECK(run(place(3, 0, part(2, 0)), &o, log) == 1 && o.find("outside") != std::string::npos, "a timepoint outside the manifest was accepted");

    std::printf("[incomplete] monostatus and monofinish exit 3 and name the missing part\n");
    CHECK(run(E + " monostatus " + q(wd), &o, log) == 3 && o.find("P_C1_T2") != std::string::npos && o.find("5 of 6") != std::string::npos,
          "monostatus with a part missing");
    CHECK(run(E + " monofinish " + q(wd) + " " + q(out), &o, log) == 3 && o.find("P_C1_T2") != std::string::npos, "monofinish with a part missing");
    CHECK(!fs::exists(out) && fs::exists(partial), "an early monofinish touched the files");
    CHECK(run(place(2, 1, part(2, 1))) == 0, "placing the missing part after an early monofinish");

    std::printf("[again] placing a part again changes neither the file size nor its sidecar\n");
    std::string idxBefore, idxAfter; uint64_t sizeBefore = fileSize(partial);
    readBytes(wd / "mono" / "P_C0_T1.idx", idxBefore);
    CHECK(run(place(1, 0, part(1, 0))) == 0, "monoplace again");
    readBytes(wd / "mono" / "P_C0_T1.idx", idxAfter);
    CHECK(fileSize(partial) == sizeBefore && !idxBefore.empty() && idxBefore == idxAfter, "placing again changed the file (%llu -> %llu bytes) or the sidecar",
          (unsigned long long)sizeBefore, (unsigned long long)fileSize(partial));
    CHECK(run(E + " monostatus " + q(wd), &o, log) == 0 && o.find("6 of 6") != std::string::npos, "monostatus when complete");

    std::printf("[finish] parts deleted first\n");
    fs::remove_all(parts);
    CHECK(run(E + " monofinish " + q(wd) + " " + q(out) + " --verify --threads 3") == 0, "monofinish --verify");
    CHECK(fs::exists(out) && !fs::exists(partial), "the file was not renamed to the output");
    if(fs::exists(out)){
        compareFiles(refDir / "output.ims", out, "2 channels x 3 timepoints");
        std::printf("[native order] links and attributes, every object\n");
        CHECK(nativeOrder(refDir / "output.ims", out), "storage order or link types differ from the single-node file");
        ImsCompareReport deep;
        CHECK(imsDeepCheck(out, deep, 2), "deep check of the single file");
    }
    CHECK(run(place(0, 0, keep / "part_t0_c0.ims"), &o, log) == 1 && o.find("finished") != std::string::npos, "a placement after the finish was accepted");
    CHECK(run(E + " monofinish " + q(wd), &o, log) == 0 && o.find("already finished") != std::string::npos, "finishing again");
    CHECK(run(E + " monostatus " + q(wd), &o, log) == 0 && o.find("finished: ") != std::string::npos, "monostatus after the finish");

    // ---------------- crop run: monoinit, -B parts ----------------
    std::printf("[crop] 2 timepoints x 1 channel, -B\n");
    const uint64_t cx = 160, cy = 150, cz = 95;
    const char* BB = "17,23,9,157,143,88";   // 140 x 120 x 79: 2 levels
    fs::path zdir2 = work / "zarr2"; fs::create_directories(zdir2);
    for(int t = 0; t < 2; t++){ char n[64]; std::snprintf(n, sizeof n, "Vol_t%04d.zarr", t);
        writeZarr(zdir2 / n, makeVolume<uint16_t>(cx, cy, cz, t + 2, 1, 0), cx, cy, cz, "<u2"); }
    fs::create_directories(work / "u8");
    writeZarr(work / "u8" / "U8_t0000.zarr", makeVolume<uint8_t>(cx, cy, cz, 0, 0, 200), cx, cy, cz, "|u1");
    fs::path ref2 = work / "ref2", parts2 = work / "parts2", wd2 = work / "wd2", out2 = work / "mono_crop.ims";
    fs::create_directories(ref2); fs::create_directories(parts2);
    bool made = run(E + " -P Vol -F " + q(zdir2) + " -o " + q(ref2) + " -r zarr -v " + VOXEL + " -B " + BB) == 0;
    for(int t = 0; t < 2; t++) made = made && run(E + " -P Vol_t000" + std::to_string(t) + " -F " + q(zdir2) + " -o " + q(parts2) + " -n p" + std::to_string(t) +
                                                 " -r zarr -v " + VOXEL + " -B " + BB) == 0;
    made = made && run(E + " -P U8 -F " + q(work / "u8") + " -o " + q(parts2) + " -n u8 -r zarr -v " + VOXEL + " -B " + BB) == 0;
    CHECK(made, "crop conversions failed");
    CHECK(run(E + " monoinit " + q(wd2) + " 2 1 " + VOXEL + " " + q(out2)) == 0, "monoinit");
    CHECK(run(E + " monoinit " + q(wd2) + " 2 1 " + VOXEL) == 0, "monoinit again with the same values");
    CHECK(run(E + " monoinit " + q(wd2) + " 3 1 " + VOXEL, &o, log) == 1, "monoinit over a different manifest");
    auto place2 = [&](int t, const fs::path& p){ return E + " monoplace " + q(wd2) + " " + std::to_string(t) + " 0 " + q(p); };
    CHECK(run(place2(0, parts2 / "p0.ims")) == 0, "monoplace crop t0");
    CHECK(run(place2(1, keep / "part_t0_c0.ims"), &o, log) == 1 && o.find("layout.json") != std::string::npos, "a part of another geometry was accepted");
    CHECK(run(place2(1, parts2 / "u8.ims"), &o, log) == 1 && o.find("uint16") != std::string::npos, "a uint8 part was accepted");
    CHECK(run(place2(1, refDir / "output.ims"), &o, log) == 1 && o.find("more than one") != std::string::npos, "a file with two channels was accepted as a part");
    CHECK(run(place2(1, parts2 / "p1.ims")) == 0, "monoplace crop t1");
    CHECK(run(E + " monofinish " + q(wd2), &o, log) == 0 && fs::exists(out2) && o.find("levels 0 re-chunked") != std::string::npos,
          "monofinish of the crop run (output from the manifest; level 0 re-chunked)");
    if(fs::exists(out2)){
        compareFiles(ref2 / "output.ims", out2, "cropped, 2 timepoints x 1 channel");
        CHECK(nativeOrder(ref2 / "output.ims", out2), "crop run: storage order differs");
    }

    if(gFail){ std::printf("monotest FAILED (%d checks)\n", gFail); return 1; }
    std::printf("monotest PASSED\n");
    return 0;
}
#endif
