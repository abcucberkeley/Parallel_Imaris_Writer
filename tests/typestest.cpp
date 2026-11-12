// Sample-type test: uint8 and float32 through the single-file cluster output (src/mono.h, src/parallel.h), on
// synthetic data, through the shipped executable. The reference of each case is ONE single-node conversion of all
// its volumes with the same options.
//
//   uint8 series : 2 channels x 3 timepoints, cropped with -B, with metadata options. The B-4 flow (the converter per
//                  (timepoint, channel), monoinit, monoplace, monofinish --verify) and a series plan (plan, every task,
//                  finish --verify) each give a file identical to the reference: every chunk, histogram (256 bins
//                  over 0..255), attribute, ColorRange and /Thumbnail.
//   uint8 tiles  : the same inputs through a tile plan (mono store, several tiles per task) and through setup's mono
//                  store (worker per level, monitor): identical to the reference.
//   float32      : 2 channels x 3 timepoints (series plan) and 1 timepoint x 2 channels (the B-4 flow). The data is
//                  identical to the reference; the histograms are the parts' own (ImarisWriter's adaptive float32
//                  histograms): level 0 identical, levels >= 1 may differ (ImarisWriter's own runs do), and
//                  ColorRange is identical with one timepoint and within the bound of src/mono.h with three. Only
//                  those attributes, histograms and the /Thumbnail (a pixel may differ by 1) may differ.
//   refused      : a float32 or a packs-store uint8 tile plan, a uint32 series plan, setup's packs-store worker on
//                  uint8, a float32 part placed into a uint8 workdir.
//   usage: typesTest <parallelimariswriter> <workdir>
// Exit 0 = pass, 1 = fail, 77 = skipped (Windows: the single-file path is not supported there).
#ifdef _WIN32
#include <cstdio>
int main(){ std::printf("typesTest: skipped (the single-file cluster output is not supported on Windows)\n"); return 77; }
#else
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <type_traits>
#include <vector>
#include <sys/wait.h>

#include "zarr.h"                // cpp-zarr (also pulls in nlohmann::json)
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims comparison (HDF5 + lz4)

namespace fs = std::filesystem;

static int gFail = 0;
#define CHECK(cond, ...) do{ if(!(cond)){ gFail++; std::printf("  FAIL (line %d): ", __LINE__); std::printf(__VA_ARGS__); std::printf("\n"); } }while(0)

// Odd on every axis; the crop is 191 x 165 x 116: uint8 2 levels (1 Mi-voxel pyramid limit), float32 3 (256 Ki).
static const uint64_t X = 203, Y = 181, Z = 125;
static const char* BB = "7,11,5,198,176,121";
static const char* VOXEL = "0.108,0.108,0.25";
static const char* META = " --colors 00FF00,FF00FF --time-interval 1.5 --start-time \"2024-05-06 07:08:09\"";

// A different range per (timepoint, channel), two populations; float32 values with fractions (and some below 1).
template<class T> static std::vector<T> makeVolume(uint64_t nx, uint64_t ny, uint64_t nz, int t, int c){
    std::vector<T> buf(nx * ny * nz);
    const uint32_t range = sizeof(T) == 1 ? 120 + 40 * t + 50 * c : 900 + 1700 * t + 2300 * c;
    for(uint64_t z = 0; z < nz; z++) for(uint64_t y = 0; y < ny; y++) for(uint64_t x = 0; x < nx; x++){
        uint32_t v = (uint32_t)((x * 7 + y * 13 + z * 17 + (uint64_t)(t * 31 + c * 137)) % range);
        if((x / 16 + y / 8 + z / 4) % 5 == 0) v = v / 3;
        double d = v + 1;
        if(std::is_floating_point<T>::value) d = v * 0.37 + 0.25 * (double)((x + 2 * y + 3 * z) % 4);
        buf[x + y * nx + z * nx * ny] = (T)d;
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
// C stdio, not iostreams or std::filesystem copies (macOS: see tests/monotest.cpp).
static bool readBytes(const fs::path& p, std::string& b){
    FILE* f = std::fopen(p.string().c_str(), "rb"); if(!f) return false;
    b.clear(); char buf[1 << 16]; size_t n;
    while((n = std::fread(buf, 1, sizeof buf, f)) > 0) b.append(buf, n);
    const bool ok = !std::ferror(f); std::fclose(f); return ok;
}
// Runs a shell command; its exit code (-1 if it did not exit normally). out: its stdout + stderr.
static int run(const std::string& cmd, std::string* out = nullptr, const fs::path& log = fs::path()){
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    std::string full = cmd;
    if(out) full = "(" + cmd + ") > " + q(log) + " 2>&1";
    int rc = std::system(full.c_str());
    if(out){ readBytes(log, *out); std::printf("%s", out->c_str()); }
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
}
static bool has(const std::string& s, const char* what){ return s.find(what) != std::string::npos; }
static bool compareFiles(const fs::path& ref, const fs::path& out, const char* what){
    std::printf("[compare] %s\n", what);
    ImsCompareOptions opt; ImsCompareReport report;
    imsCompare(ref, out, report, opt);
    imsPrintReport(report);
    const bool ok = report.identical();
    CHECK(ok, "%s: the files differ", what);
    return ok;
}
// The stage names and task counts of a plan.
static bool readStages(const fs::path& wd, std::vector<std::pair<std::string, long> >& stages){
    std::string b; stages.clear();
    if(!readBytes(wd / "manifest.json", b)) return false;
    try{ nlohmann::json j = nlohmann::json::parse(b);
        for(auto& s : j.at("stages")) stages.push_back(std::make_pair(s.at("name").get<std::string>(), s.at("tasks").get<long>())); }
    catch(const std::exception&){ return false; }
    return !stages.empty();
}
// Every task of every stage of a plan, in shuffled order, each a separate process.
static bool runPlan(const std::string& E, const fs::path& wd){
    std::vector<std::pair<std::string, long> > st;
    if(!readStages(wd, st)) return false;
    bool ok = true;
    for(const auto& s : st){
        std::vector<long> order; for(long i = 0; i < s.second; i++) order.push_back(i);
        for(size_t i = 0; i < order.size(); i++) std::swap(order[i], order[(i * 7 + 3) % order.size()]);
        for(long i : order) ok = run(E + " task " + q(wd) + " " + s.first + " " + std::to_string(i) + " --threads 2") == 0 && ok;
    }
    return ok;
}

// ---- float32: what may differ, and the ColorRange bound ----
static int levelOf(const std::string& line){   // the ResolutionLevel a difference line names, -1 for none
    size_t p = line.find("ResolutionLevel "); return p == std::string::npos ? -1 : std::atoi(line.c_str() + p + 16); }
static bool floatPair(const std::string& s, float& a, float& b){ return std::sscanf(s.c_str(), "%f %f", &a, &b) == 2; }
// The reference (a) against the cluster file (b): the data must be identical, and the only differences allowed are
// the histograms of levels >= 1, the ColorRange (one timepoint: identical; more: the bound of src/mono.h) and the
// /Thumbnail (each byte within 1).
static void compareFloat(const fs::path& ref, const fs::path& out, int nT, int nC, const char* what){
    std::printf("[compare float32] %s\n", what);
    ImsCompareOptions opt; ImsCompareReport report;
    imsCompare(ref, out, report, opt);
    imsPrintReport(report);
    CHECK(report.error.empty() && report.datasets > 0, "%s: not compared", what);
    int histHigher = 0, colorRange = 0, other = 0;
    for(const std::string& d : report.differences){
        const bool hist = has(d, "Histogram");
        if(hist && levelOf(d) >= 1) histHigher++;
        else if(has(d, "/DataSetInfo/Channel ") && has(d, "ColorRange")) colorRange++;
        else { other++; std::printf("  not allowed: %s\n", d.c_str()); }
    }
    std::printf("  %d histogram differences at levels >= 1, %d ColorRange differences, %zu /Thumbnail differences\n",
                histHigher, colorRange, report.thumbnail.size());
    CHECK(other == 0, "%s: %d differences other than level >= 1 histograms, ColorRange and /Thumbnail (data or level 0)", what, other);
    for(int c = 0; c < nC; c++){
        const std::string g = "/DataSetInfo/Channel " + std::to_string(c);
        auto ra = imsReadAttribute(ref, g, "ColorRange"), rb = imsReadAttribute(out, g, "ColorRange");
        float a0 = 0, a1 = 0, b0 = 0, b1 = 0;
        CHECK(ra && rb && floatPair(*ra, a0, a1) && floatPair(*rb, b0, b1), "%s: channel %d has no ColorRange", what, c);
        if(!ra || !rb) continue;
        std::printf("  channel %d ColorRange: reference \"%s\", cluster \"%s\"\n", c, ra->c_str(), rb->c_str());
        if(nT == 1){ CHECK(*ra == *rb, "%s: one timepoint, channel %d: ColorRange differs", what, c); continue; }
        float lo = FLT_MAX, hi = -FLT_MAX;   // the channel's value range R over all timepoints (level-0 histograms)
        for(int t = 0; t < nT; t++){
            const std::string lg = "/DataSet/ResolutionLevel 0/TimePoint " + std::to_string(t) + "/Channel " + std::to_string(c);
            auto mn = imsReadAttribute(ref, lg, "HistogramMin"), mx = imsReadAttribute(ref, lg, "HistogramMax");
            if(mn && mx){ lo = std::min(lo, std::strtof(mn->c_str(), NULL)); hi = std::max(hi, std::strtof(mx->c_str(), NULL)); }
        }
        const float R = hi - lo, d = 0.0005f + 0.5f * std::max(std::fabs(lo), std::fabs(hi)) * FLT_EPSILON;
        CHECK(std::fabs(a0 - b0) <= d + R / 1024 + 0.001f, "%s: channel %d ColorRange minimum %g vs %g (bound %g)", what, c, a0, b0, d + R / 1024);
        CHECK(std::fabs(a1 - b1) <= 1.4f * (d + R / 1024) + 1.001f, "%s: channel %d ColorRange maximum %g vs %g (bound %g)", what, c, a1, b1, 1.4f * (d + R / 1024) + 1);
    }
    uint64_t ra = 0, ba = 0, rb = 0, bb = 0; std::vector<uint8_t> ta, tb;
    CHECK(imsReadThumbnail(ref, ra, ba, ta) && imsReadThumbnail(out, rb, bb, tb), "%s: no /Thumbnail", what);
    CHECK(ra == rb && ba == bb && ta.size() == tb.size(), "%s: /Thumbnail shapes differ", what);
    if(ta.size() == tb.size()){ int maxd = 0; size_t n = 0;
        for(size_t i = 0; i < ta.size(); i++){ int e = std::abs((int)ta[i] - (int)tb[i]); if(e){ n++; maxd = std::max(maxd, e); } }
        std::printf("  /Thumbnail: %zu of %zu bytes differ, by at most %d\n", n, ta.size(), maxd);
        if(nT == 1) CHECK(maxd <= 1, "%s: /Thumbnail bytes differ by %d", what, maxd); }
}

int main(int argc, char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if(argc < 3){ std::fprintf(stderr, "usage: %s <parallelimariswriter> <workdir>\n", argv[0]); return 2; }
    const std::string E = q(argv[1]);
    const fs::path work = argv[2];
    fs::remove_all(work); fs::create_directories(work);
    const fs::path log = work / "cmd.log";
    std::string o;
    const int nT = 3, nC = 2; const char* cam[2] = {"CamA", "CamB"};
    const std::string conv = std::string(" -r zarr -v ") + VOXEL + " -B " + BB + META;

    // ---------------- inputs ----------------
    const fs::path z8 = work / "zarr_u8", zf = work / "zarr_f32";
    fs::create_directories(z8); fs::create_directories(zf);
    for(int c = 0; c < nC; c++) for(int t = 0; t < nT; t++){
        char n[64]; std::snprintf(n, sizeof n, "%s_t%04d.zarr", cam[c], t);
        writeZarr(z8 / n, makeVolume<uint8_t>(X, Y, Z, t, c), X, Y, Z, "|u1");
        writeZarr(zf / n, makeVolume<float>(X, Y, Z, t, c), X, Y, Z, "<f4");
    }

    // ---------------- uint8 series ----------------
    std::printf("[uint8 reference] one single-node conversion of all %d volumes\n", nT * nC);
    const fs::path ref8 = work / "ref_u8" / "output.ims";
    CHECK(run(E + " -P CamA,CamB -F " + q(z8) + " -o " + q(work / "ref_u8") + conv) == 0 && fs::exists(ref8), "uint8 reference conversion");
    std::printf("[uint8 B-4 flow] the converter per (timepoint, channel), monoinit, monoplace, monofinish\n");
    const fs::path parts = work / "parts", wdB4 = work / "wd_u8_b4", b4 = work / "u8_b4.ims";
    bool made = run(E + " monoinit " + q(wdB4) + " 3 2 " + VOXEL + " " + q(b4) + " --channel-names CamA,CamB" + META) == 0;
    for(int t = 0; t < nT; t++) for(int c = 0; c < nC; c++){
        char pat[64]; std::snprintf(pat, sizeof pat, "%s_t%04d", cam[c], t);
        const std::string n = "u8_t" + std::to_string(t) + "_c" + std::to_string(c);
        made = made && run(E + " -P " + pat + " -F " + q(z8) + " -o " + q(parts) + " -n " + n + " -r zarr -v " + VOXEL + " -B " + BB) == 0;
        made = made && run(E + " monoplace " + q(wdB4) + " " + std::to_string(t) + " " + std::to_string(c) + " " + q(parts / (n + ".ims"))) == 0;
    }
    made = made && run(E + " monofinish " + q(wdB4) + " --verify") == 0 && fs::exists(b4);
    CHECK(made, "the uint8 B-4 flow failed");
    if(fs::exists(b4)) compareFiles(ref8, b4, "uint8: B-4 flow vs the single-node conversion");
    std::printf("[uint8 series plan]\n");
    const fs::path wd8 = work / "wd_u8", out8 = wd8 / "output.ims";
    CHECK(run(E + " plan " + q(wd8) + " -F " + q(z8) + " -P CamA,CamB" + conv, &o, log) == 0 && has(o, "stage convert: 6 tasks"), "uint8 series plan");
    CHECK(runPlan(E, wd8), "the uint8 series tasks");
    CHECK(run(E + " finish " + q(wd8) + " --verify --threads 3") == 0 && fs::exists(out8), "uint8 series finish --verify");
    if(fs::exists(out8)) compareFiles(ref8, out8, "uint8: series plan vs the single-node conversion");

    // ---------------- uint8 tiles ----------------
    std::printf("[uint8 tile plan] mono store, 4 tiles per task\n");
    const fs::path wdt = work / "wd_u8_tiles", outT = work / "u8_tiles.ims";
    CHECK(run(E + " plan " + q(wdt) + " -F " + q(z8) + " -P CamA,CamB --tiles 96,96,64 --tiles-per-task 4 -o " + q(work) + " -n u8_tiles" + conv, &o, log) == 0 &&
          has(o, "tiles (mono store)"), "uint8 tile plan");
    CHECK(runPlan(E, wdt), "the uint8 tile tasks");
    CHECK(run(E + " finish " + q(wdt) + " --verify") == 0 && fs::exists(outT), "uint8 tile finish --verify");
    if(fs::exists(outT)) compareFiles(ref8, outT, "uint8: tile plan vs the single-node conversion");
    std::printf("[uint8 setup] mono store, worker per level, monitor (uncropped)\n");
    const fs::path refFull = work / "ref_u8_full" / "output.ims", wds = work / "wd_u8_setup", outS = work / "u8_setup.ims";
    CHECK(run(E + " -P CamA,CamB -F " + q(z8) + " -o " + q(work / "ref_u8_full") + " -r zarr -v " + VOXEL + META) == 0, "uncropped uint8 reference");
    made = run(E + " setup " + q(z8) + " CamA,CamB " + q(wds) + " 128,128,128 " + VOXEL + META, &o, log) == 0 && has(o, "single-file");
    std::vector<std::pair<std::string, long> > st;
    {   std::string b; nlohmann::json j;
        made = made && readBytes(wds / "manifest.json", b);
        try{ j = nlohmann::json::parse(b); made = made && j.value("dataType", std::string()) == "uint8";
            int r = 0; for(auto& n : j.at("jobsPerLevel")){ made = made && run(E + " worker " + q(wds) + " " + std::to_string(r) + " 0 " + std::to_string(n.get<long>())) == 0; r++; } }
        catch(const std::exception&){ made = false; } }
    made = made && run(E + " monitor " + q(wds) + " " + q(outS) + " --verify") == 0 && fs::exists(outS);
    CHECK(made, "setup's mono store on uint8");
    if(fs::exists(outS) && fs::exists(refFull)) compareFiles(refFull, outS, "uint8: setup + worker + monitor vs the single-node conversion");

    // ---------------- float32 ----------------
    std::printf("[float32 reference] one single-node conversion of all %d volumes\n", nT * nC);
    const fs::path refF = work / "ref_f32" / "output.ims";
    CHECK(run(E + " -P CamA,CamB -F " + q(zf) + " -o " + q(work / "ref_f32") + conv) == 0 && fs::exists(refF), "float32 reference conversion");
    std::printf("[float32 series plan] 3 timepoints\n");
    const fs::path wdf = work / "wd_f32", outF = wdf / "output.ims";
    CHECK(run(E + " plan " + q(wdf) + " -F " + q(zf) + " -P CamA,CamB" + conv, &o, log) == 0 && has(o, "stage convert: 6 tasks"), "float32 series plan");
    CHECK(runPlan(E, wdf), "the float32 series tasks");
    CHECK(run(E + " finish " + q(wdf) + " --verify --threads 3") == 0 && fs::exists(outF), "float32 series finish --verify");
    if(fs::exists(outF)) compareFloat(refF, outF, nT, nC, "float32, 3 timepoints: series plan vs the single-node conversion");
    std::printf("[float32 B-4 flow] 1 timepoint x 2 channels\n");
    const fs::path ref1 = work / "ref_f32_t0" / "output.ims", wdf1 = work / "wd_f32_t0", outF1 = work / "f32_t0.ims";
    made = run(E + " -P CamA_t0000,CamB_t0000 -F " + q(zf) + " -o " + q(work / "ref_f32_t0") + " -r zarr -v " + VOXEL + " -B " + BB +
               " --channel-names CamA,CamB" + META) == 0;
    made = made && run(E + " monoinit " + q(wdf1) + " 1 2 " + VOXEL + " " + q(outF1) + " --channel-names CamA,CamB" + META) == 0;
    for(int c = 0; c < nC; c++){
        const std::string n = std::string("f32_c") + std::to_string(c);
        made = made && run(E + " -P " + cam[c] + "_t0000 -F " + q(zf) + " -o " + q(parts) + " -n " + n + " -r zarr -v " + VOXEL + " -B " + BB) == 0;
        made = made && run(E + " monoplace " + q(wdf1) + " 0 " + std::to_string(c) + " " + q(parts / (n + ".ims"))) == 0;
    }
    made = made && run(E + " monofinish " + q(wdf1) + " --verify") == 0 && fs::exists(outF1);
    CHECK(made, "the float32 B-4 flow failed");
    if(fs::exists(outF1) && fs::exists(ref1)) compareFloat(ref1, outF1, 1, nC, "float32, 1 timepoint: B-4 flow vs the single-node conversion");

    // ---------------- refused ----------------
    std::printf("[refused] nothing is written\n");
    const fs::path wdx = work / "wd_refused", z32 = work / "zarr_u32";
    fs::create_directories(z32);
    writeZarr(z32 / "U32_t0000.zarr", makeVolume<uint32_t>(X, Y, Z, 0, 0), X, Y, Z, "<u4");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(zf) + " -P CamA -r zarr --tiles 96,96,64", &o, log) == 1 && has(o, "tile plan takes uint8 and uint16"), "a float32 tile plan");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(z8) + " -P CamA -r zarr --tiles 96,96,64 --store packs", &o, log) == 1 && has(o, "packs store"), "a uint8 packs tile plan");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(z32) + " -P U32 -r zarr", &o, log) == 1 && has(o, "uint32"), "a uint32 series plan");
    CHECK(!fs::exists(wdx / "manifest.json"), "a refused plan wrote a manifest");
    const fs::path wdp = work / "wd_u8_packs";
    CHECK(run(E + " setup " + q(z8) + " CamA " + q(wdp) + " 128,128,128 " + VOXEL + " --store packs") == 0 &&
          run(E + " worker " + q(wdp) + " 0 0", &o, log) == 1 && has(o, "packs store"), "setup's packs store took uint8 tiles");
    const fs::path wdm = work / "wd_mixed";
    CHECK(run(E + " monoinit " + q(wdm) + " 2 1 " + VOXEL) == 0 && run(E + " monoplace " + q(wdm) + " 0 0 " + q(parts / "u8_t0_c0.ims")) == 0 &&
          run(E + " monoplace " + q(wdm) + " 1 0 " + q(parts / "f32_c0.ims"), &o, log) == 1 && has(o, "float32") && has(o, "data type"),
          "a float32 part was placed into a uint8 workdir");

    if(gFail){ std::printf("typestest FAILED (%d checks)\n", gFail); return 1; }
    fs::remove_all(work);
    std::printf("typestest PASSED\n");
    return 0;
}
#endif
