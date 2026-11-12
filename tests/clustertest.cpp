// Cluster-plan test: the cluster steps as a plan of independent tasks (plan, task, status, finish; src/parallel.h),
// every task a separate process of the shipped executable, on synthetic data.
//
//   series : 2 channels x 3 timepoints, cropped with -B, with metadata options and -C. The plan's refusals first
//            (a uint8 input, a box that does not fit, inputs of two sizes, a bad metadata option, a second plan in the
//            same workdir); then status/finish (and monostatus/monofinish) of the empty workdir; the tasks in shuffled
//            order, staged in a given folder or wherever the task decides, one by the stage's number, one twice at
//            the same time, and one whose input has a truncated chunk: that one fails alone, status/finish (and
//            monostatus/monofinish) of the partly done workdir name it, it runs once the chunk is back, and a done
//            task is skipped. No staged part is left. finish --verify: the file equals the single-node conversion of
//            the same inputs with the same options and the B-4 flow (the converter per (timepoint, channel), monoinit,
//            monoplace, monofinish): imsCompare of every chunk, histogram, attribute and /Thumbnail. Then a task fails
//            (the file is finished) and finish again reports "already finished".
//   tiles  : the same inputs through a tile plan (--tiles, two tiles per task), mono store: status/finish of the empty
//            and the partly done workdir (and monostatus/monofinish), monoplace refused, a level-1 task before level 0
//            fails; every task in shuffled order level by level; the file equals the single-node conversion. And the
//            packs store: every task, finish merges the packs into a file equal to the same conversion.
//   setup  : which writers a setup workdir (mono store) takes is decided by what is placed first: empty, it is a
//            parts workdir to monostatus and monofinish ("nothing was placed yet"); after one tile worker, a tile one.
//   usage: clusterTest <parallelimariswriter> <workdir>
// Exit 0 = pass, 1 = fail, 77 = skipped (Windows: the single-file path is not supported there).
#ifdef _WIN32
#include <cstdio>
int main(){ std::printf("clusterTest: skipped (the single-file cluster output is not supported on Windows)\n"); return 77; }
#else
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "zarr.h"                // cpp-zarr (also pulls in nlohmann::json)
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims comparison (HDF5 + lz4)

namespace fs = std::filesystem;

static int gFail = 0;
#define CHECK(cond, ...) do{ if(!(cond)){ gFail++; std::printf("  FAIL (line %d): ", __LINE__); std::printf(__VA_ARGS__); std::printf("\n"); } }while(0)

// Odd on every axis; the crop keeps 3 resolution levels (the second above the 1 Mi-voxel pyramid limit of uint16).
static const uint64_t X = 203, Y = 181, Z = 125;
static const char* BB = "7,11,5,198,176,121";    // 191 x 165 x 116
static const char* VOXEL = "0.108,0.108,0.25";
static const char* META = " --colors 00FF00,FF00FF --time-interval 1.5 --start-time \"2024-05-06 07:08:09\"";

template<class T> static std::vector<T> makeVolume(uint64_t nx, uint64_t ny, uint64_t nz, int t, int c){
    std::vector<T> buf(nx * ny * nz);
    const uint32_t range = 900 + 1700 * t + 2300 * c;
    for(uint64_t z = 0; z < nz; z++) for(uint64_t y = 0; y < ny; y++) for(uint64_t x = 0; x < nx; x++){
        uint32_t v = (uint32_t)((x * 7 + y * 13 + z * 17 + (uint64_t)(t * 31 + c * 137)) % range);
        if((x / 16 + y / 8 + z / 4) % 5 == 0) v = v / 3;
        buf[x + y * nx + z * nx * ny] = (T)(v % (sizeof(T) == 1 ? 250 : 65000) + 1);
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
static bool writeBytes(const fs::path& p, const std::string& b){
    FILE* f = std::fopen(p.string().c_str(), "wb"); if(!f) return false;
    const bool ok = std::fwrite(b.data(), 1, b.size(), f) == b.size();
    return std::fclose(f) == 0 && ok;
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
// The staged parts in a folder (piw_part_*).
static size_t stagedParts(const fs::path& dir){
    size_t n = 0; DIR* d = opendir(dir.c_str()); if(!d) return 0; struct dirent* e;
    while((e = readdir(d))) if(!std::strncmp(e->d_name, "piw_part_", 9)){ struct stat st;
        if(stat((dir / e->d_name).c_str(), &st) == 0 && st.st_uid == getuid()) n++; }
    closedir(d); return n;
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

int main(int argc, char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    if(argc < 3){ std::fprintf(stderr, "usage: %s <parallelimariswriter> <workdir>\n", argv[0]); return 2; }
    const std::string E = q(argv[1]);
    const fs::path work = argv[2];
    fs::remove_all(work); fs::create_directories(work);
    const fs::path log = work / "cmd.log";
    std::string o;
    const size_t shmBefore = stagedParts("/dev/shm");

    // ---------------- inputs and the two references ----------------
    const int nT = 3, nC = 2; const char* cam[2] = {"CamA", "CamB"};
    const fs::path zdir = work / "zarr"; fs::create_directories(zdir);
    for(int c = 0; c < nC; c++) for(int t = 0; t < nT; t++){
        char n[64]; std::snprintf(n, sizeof n, "%s_t%04d.zarr", cam[c], t);
        writeZarr(zdir / n, makeVolume<uint16_t>(X, Y, Z, t, c), X, Y, Z, "<u2");
    }
    const std::string conv = std::string(" -r zarr -v ") + VOXEL + " -B " + BB + META;
    std::printf("[reference] one single-node conversion of all %d volumes\n", nT * nC);
    const fs::path refDir = work / "ref", ref = refDir / "output.ims";
    CHECK(run(E + " -P CamA,CamB -F " + q(zdir) + " -o " + q(refDir) + conv) == 0 && fs::exists(ref), "reference conversion");
    std::printf("[B-4 flow] the converter per (timepoint, channel), monoinit, monoplace, monofinish\n");
    const fs::path parts = work / "parts", wdB4 = work / "wd_b4", b4 = work / "b4.ims";
    bool made = run(E + " monoinit " + q(wdB4) + " 3 2 " + VOXEL + " " + q(b4) + " --channel-names CamA,CamB" + META) == 0;
    for(int t = 0; t < nT; t++) for(int c = 0; c < nC; c++){
        char pat[64]; std::snprintf(pat, sizeof pat, "%s_t%04d", cam[c], t);
        const std::string n = "p_t" + std::to_string(t) + "_c" + std::to_string(c);
        made = made && run(E + " -P " + pat + " -F " + q(zdir) + " -o " + q(parts) + " -n " + n + " -r zarr -v " + VOXEL + " -B " + BB) == 0;
        made = made && run(E + " monoplace " + q(wdB4) + " " + std::to_string(t) + " " + std::to_string(c) + " " + q(parts / (n + ".ims"))) == 0;
    }
    made = made && run(E + " monofinish " + q(wdB4)) == 0 && fs::exists(b4);
    CHECK(made, "the B-4 flow failed");
    fs::remove_all(parts);

    // ---------------- series: refusals ----------------
    std::printf("[plan refused] nothing is written\n");
    const fs::path u8 = work / "u8", sizes = work / "sizes", wdx = work / "wd_refused";
    fs::create_directories(u8); fs::create_directories(sizes);
    writeZarr(u8 / "U8_t0000.zarr", makeVolume<uint8_t>(X, Y, Z, 0, 0), X, Y, Z, "|u1");
    writeZarr(sizes / "S_t0000.zarr", makeVolume<uint16_t>(X, Y, Z, 0, 0), X, Y, Z, "<u2");
    writeZarr(sizes / "S_t0001.zarr", makeVolume<uint16_t>(X, Y - 1, Z, 1, 0), X, Y - 1, Z, "<u2");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(u8) + " -P U8 -r zarr", &o, log) == 1 && has(o, "uint16"), "a uint8 series plan");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(zdir) + " -P CamA,CamB -r zarr -B 0,0,0,999,10,10", &o, log) == 1 && has(o, "does not fit"), "a box that does not fit");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(sizes) + " -P S_ -r zarr", &o, log) == 1 && has(o, "same size"), "inputs of two sizes");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(zdir) + " -P CamA,CamB -r zarr --colors nocolor", &o, log) == 1 && has(o, "nocolor"), "a bad metadata option");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(zdir) + " -P CamA,CamB -r zarr --store packs", &o, log) == 1, "--store without --tiles");
    CHECK(run(E + " plan " + q(wdx) + " -F " + q(zdir) + " -P CamA,CamB -r zarr --tiles 64,64,64 -B 0,0,0,10,10,999", &o, log) == 1, "a tile plan whose box does not fit");
    CHECK(!fs::exists(wdx / "manifest.json"), "a refused plan wrote a manifest");

    // ---------------- series: plan, empty workdir ----------------
    const fs::path wd = work / "wd", out = wd / "output.ims", stage = work / "stage";
    std::printf("[series plan]\n");
    const std::string plan = E + " plan " + q(wd) + " -F " + q(zdir) + " -P CamA,CamB -C 64,64,64" + conv;
    CHECK(run(plan, &o, log) == 0 && has(o, "stage convert: 6 tasks"), "series plan");
    CHECK(run(plan, &o, log) == 0, "the same plan again keeps the workdir");
    CHECK(run(E + " plan " + q(wd) + " -F " + q(zdir) + " -P CamA,CamB -r zarr -v 1,1,1", &o, log) == 1 && has(o, "another plan"), "another plan in the same workdir");
    std::printf("[series, empty] status and finish name every task\n");
    CHECK(run(E + " status " + q(wd), &o, log) == 3 && has(o, "0 of 6 tasks done; missing: 0-5"), "status of the empty workdir");
    CHECK(run(E + " finish " + q(wd), &o, log) == 3 && has(o, "6 of 6 tasks are not done") && has(o, "convert 0-5"), "finish of the empty workdir");
    CHECK(run(E + " monostatus " + q(wd), &o, log) == 3 && has(o, "0 of 6 parts placed"), "monostatus of the empty workdir");
    CHECK(run(E + " monofinish " + q(wd), &o, log) == 1 && has(o, "no part was placed yet"), "monofinish of the empty workdir");

    // ---------------- series: the tasks ----------------
    // task 3 = (t=1, c=1): CamB_t0001, with a truncated chunk
    std::vector<fs::path> chunks;
    for(const auto& e : fs::directory_iterator(zdir / "CamB_t0001.zarr")) if(e.path().filename().string()[0] != '.') chunks.push_back(e.path());
    std::sort(chunks.begin(), chunks.end());
    std::string chunk0; CHECK(!chunks.empty() && readBytes(chunks.front(), chunk0) && chunk0.size() > 10, "no chunk to truncate");
    fs::resize_file(chunks.front(), 10);
    auto task = [&](int i, const std::string& extra){ return E + " task " + q(wd) + " convert " + std::to_string(i) + " --threads 2" + extra; };
    std::printf("[series tasks] shuffled, separate processes; task 3's input is truncated\n");
    CHECK(run(task(4, " --stage-dir " + q(stage))) == 0, "task 4 (given stage folder)");
    CHECK(run(task(1, "")) == 0, "task 1 (the task decides where to stage)");
    CHECK(run(task(3, ""), &o, log) == 1 && has(o, "(t=1, c=1)"), "the task with a truncated input did not fail");
    CHECK(!fs::exists(wd / "mono" / "P_C1_T1.idx"), "the failed task left a sidecar");
    CHECK(run(E + " task " + q(wd) + " 0 5 --threads 2 & p=$!; " + task(5, " --stage-dir " + q(stage)) + "; r=$?; wait $p || exit 1; exit $r") == 0,
          "task 5 twice at the same time (one by the stage's number)");
    CHECK(run(task(0, "")) == 0 && run(task(2, " -C 32,32,32")) == 0, "tasks 0 and 2");
    std::printf("[series, partly done] task 3 is named\n");
    CHECK(run(E + " status " + q(wd), &o, log) == 3 && has(o, "5 of 6 tasks done; missing: 3"), "status of the partly done workdir");
    CHECK(run(E + " finish " + q(wd), &o, log) == 3 && has(o, "convert 3"), "finish of the partly done workdir");
    CHECK(run(E + " monostatus " + q(wd), &o, log) == 3 && has(o, "missing P_C1_T1"), "monostatus of the partly done workdir");
    CHECK(run(E + " monofinish " + q(wd), &o, log) == 3 && has(o, "P_C1_T1"), "monofinish of the partly done workdir");
    CHECK(writeBytes(chunks.front(), chunk0), "cannot restore the chunk");
    CHECK(run(task(3, "")) == 0, "task 3 once its input is whole");
    CHECK(run(task(0, ""), &o, log) == 0 && has(o, "already done"), "a done task was not skipped");
    CHECK(stagedParts(stage) == 0 && stagedParts(wd / "stage") == 0 && stagedParts("/dev/shm") == shmBefore, "a staged part was left behind");
    CHECK(run(E + " status " + q(wd), &o, log) == 0 && has(o, "6 of 6 tasks done"), "status when every task is done");
    std::printf("[series finish]\n");
    CHECK(run(E + " finish " + q(wd) + " --verify --threads 3") == 0 && fs::exists(out), "finish --verify");
    if(fs::exists(out)){
        compareFiles(ref, out, "series plan vs the single-node conversion");
        compareFiles(b4, out, "series plan vs the B-4 flow");
    }
    CHECK(run(task(2, ""), &o, log) == 1 && has(o, "finished"), "a task after the finish");
    CHECK(run(E + " finish " + q(wd), &o, log) == 0 && has(o, "already finished"), "finishing again");
    CHECK(run(E + " status " + q(wd), &o, log) == 0 && has(o, "finished: "), "status after the finish");

    // ---------------- tiles: mono store ----------------
    std::printf("[tile plan] mono store, two tiles per task\n");
    const fs::path wdt = work / "wd_tiles", outT = work / "tiles.ims";
    CHECK(run(E + " plan " + q(wdt) + " -F " + q(zdir) + " -P CamA,CamB --tiles 64,64,64 --tiles-per-task 2 -o " + q(work) + " -n tiles" + conv, &o, log) == 0 &&
          has(o, "tiles (mono store)") && has(o, "stage level1"), "tile plan");
    std::vector<std::pair<std::string, long> > st;
    CHECK(readStages(wdt, st) && st.size() >= 2, "the tile plan's stages");
    std::printf("[tiles, empty]\n");
    CHECK(run(E + " status " + q(wdt), &o, log) == 3 && has(o, "stage level0: 0 of"), "status of the empty tile workdir");
    CHECK(run(E + " finish " + q(wdt), &o, log) == 3 && has(o, "level0 0"), "finish of the empty tile workdir");
    CHECK(run(E + " monostatus " + q(wdt), &o, log) == 3 && has(o, "0 of") && has(o, "tiles placed"), "monostatus of the empty tile workdir");
    CHECK(run(E + " monofinish " + q(wdt), &o, log) == 1 && has(o, "no tile was placed yet"), "monofinish of the empty tile workdir");
    CHECK(run(E + " monoplace " + q(wdt) + " 0 0 " + q(ref), &o, log) == 1 && has(o, "tile workers"), "monoplace into a tile workdir");
    CHECK(run(E + " task " + q(wdt) + " level1 0", &o, log) != 0 && has(o, "level 1 needs"), "a level-1 task before level 0");
    auto runStage = [&](const fs::path& w, const std::string& name, long n){
        std::vector<long> order; for(long i = 0; i < n; i++) order.push_back(i);
        for(size_t i = 0; i < order.size(); i++) std::swap(order[i], order[(i * 7 + 3) % order.size()]);   // shuffled
        bool ok = true;
        for(long i : order) ok = run(E + " task " + q(w) + " " + name + " " + std::to_string(i) + " --threads 2") == 0 && ok;
        return ok; };
    CHECK(st.size() >= 2 && runStage(wdt, st[0].first, st[0].second), "the level-0 tasks");
    std::printf("[tiles, partly done]\n");
    CHECK(run(E + " status " + q(wdt), &o, log) == 3 && has(o, "stage level1: 0 of") && !has(o, "stage level0: 0 of"), "status of the partly done tile workdir");
    CHECK(run(E + " finish " + q(wdt), &o, log) == 3 && has(o, "level1 0") && !has(o, "level0 "), "finish of the partly done tile workdir");
    CHECK(run(E + " monostatus " + q(wdt), &o, log) == 3 && has(o, "missing L1_"), "monostatus of the partly done tile workdir");
    CHECK(run(E + " monofinish " + q(wdt), &o, log) == 3 && has(o, "L1_"), "monofinish of the partly done tile workdir");
    for(size_t r = 1; r < st.size(); r++) CHECK(runStage(wdt, st[r].first, st[r].second), "the tasks of %s", st[r].first.c_str());
    CHECK(run(E + " task " + q(wdt) + " level0 0", &o, log) == 0 && has(o, "already done"), "a done tile task was not skipped");
    CHECK(run(E + " finish " + q(wdt) + " --verify") == 0 && fs::exists(outT), "finish of the tile plan");
    if(fs::exists(outT)) compareFiles(ref, outT, "tile plan (mono store) vs the single-node conversion");

    std::printf("[tile plan] packs store\n");
    const fs::path wdp = work / "wd_packs", outP = work / "packs.ims";
    CHECK(run(E + " plan " + q(wdp) + " -F " + q(zdir) + " -P CamA,CamB --tiles 96,64,128 --store packs" + conv, &o, log) == 0 && has(o, "packs store"), "packs plan");
    CHECK(run(E + " finish " + q(wdp) + " " + q(outP), &o, log) == 3, "finish of the empty packs workdir");
    CHECK(readStages(wdp, st), "the packs plan's stages");
    for(size_t r = 0; r < st.size(); r++) CHECK(runStage(wdp, st[r].first, st[r].second), "the packs tasks of %s", st[r].first.c_str());
    CHECK(run(E + " status " + q(wdp), &o, log) == 0, "status of the done packs workdir");
    CHECK(run(E + " finish " + q(wdp) + " " + q(outP), &o, log) == 0 && has(o, "merged") && fs::exists(outP), "finish of the packs plan");
    if(fs::exists(outP)) compareFiles(ref, outP, "tile plan (packs store) vs the single-node conversion");

    // ---------------- setup workdirs: what is placed first decides ----------------
    std::printf("[setup workdir] parts until a tile worker marks it\n");
    const fs::path wds = work / "wd_setup";
    CHECK(run(E + " setup " + q(zdir) + " CamA,CamB " + q(wds) + " 128,128,128 " + VOXEL) == 0, "setup");
    CHECK(run(E + " monostatus " + q(wds), &o, log) == 3 && has(o, "0 of 6 parts placed"), "monostatus of an empty setup workdir");
    CHECK(run(E + " monofinish " + q(wds), &o, log) == 1 && has(o, "nothing was placed yet"), "monofinish of an empty setup workdir");
    CHECK(run(E + " status " + q(wds), &o, log) == 3 && has(o, "series"), "status of an empty setup workdir");
    CHECK(run(E + " worker " + q(wds) + " 0 0") == 0, "one tile worker");
    CHECK(run(E + " monostatus " + q(wds), &o, log) == 3 && has(o, "tiles placed"), "monostatus after one tile worker");
    CHECK(run(E + " status " + q(wds), &o, log) == 3 && has(o, "tiles (mono store)") && has(o, "stage level0"), "status after one tile worker");
    CHECK(run(E + " task " + q(wds) + " level0 0", &o, log) == 0 && has(o, "already done"), "a task of a setup tile workdir");

    if(gFail){ std::printf("clustertest FAILED (%d checks)\n", gFail); return 1; }
    fs::remove_all(work);
    std::printf("clustertest PASSED\n");
    return 0;
}
#endif
