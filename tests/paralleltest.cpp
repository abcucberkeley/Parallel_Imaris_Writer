// Parallel-equivalence test.
//
// The cluster subcommands (setup/worker/monitor) run as many independent
// processes -- exactly how a SLURM array job would -- and must assemble ONE .ims
// whose voxel data is byte-identical to the monolithic whole-volume conversion.
// This generates a deliberately ALL-ODD-dimension volume for TWO channels
// (CamA/CamB, distinct data) as zstd/F-order Zarr (no committed data): odd sizes
// force edge padding on every axis at level 0 and a dropped voxel per axis at each
// downsample, the exact path that first exposed a coarse-level mismatch, and the
// two channels exercise the multi-channel assembly + per-channel color/histogram.
// It converts the Zarrs twice -- (a) monolithically (-P CamA,CamB), (b) via `setup`
// + one `worker` process per (channel, timepoint, level, chunk-index) + `monitor` --
// and compares the two with imsCompare (tests/imsutil.h): the raw compressed Data
// chunks of every channel, the histograms and the attributes, including the
// per-channel DataSetInfo (color, auto-adjusted ColorRange, name) and /Thumbnail. Same
// determinism argument as blocktest.cpp; here the two producers are the parallel
// scatter/gather and the monolithic writer.
//   packs store (setup --store packs): one .pack per tile, merged by monitor. The other
//     pack-based outputs get the same thumbnail: combineone (one channel: equal to a
//     conversion of that channel alone) and link (from the combineone parts: equal to the
//     monolithic one); linkparts writes one too. Then the failure cases.
//   mono store (the default; not on Windows): the workers write their chunks straight into
//     the single file and the coarser levels read them back; monitor indexes them. Also:
//     no .pack is written, every link of the file is a hard link, a duplicate worker run at
//     the same time, restart (a deleted sidecar: monostatus and monitor report the tile, the
//     worker runs again without changing the file), and what fails after the finish.
//   2 timepoints: a smaller volume whose 2-timepoint single-node file lays level 0 out in
//     other chunks than a 1-timepoint one, through both stores (workers given tile counts),
//     and through the mono store again with other channel names and colors (vs a conversion
//     with the same options).
//   Metadata (A-10): every /DataSetInfo attribute of both outputs is checked against the
//     expected values (channel names = the patterns, R/G colors, the first input's modification
//     time), and a second, smaller 2-channel x 2-timepoint run passes the metadata options
//     (names, colors, wavelengths, start time; times from "<n>msecAbs" stamps in the file
//     names) to both the converter and `setup`. Packs store: monitor's output must equal the
//     converter's in data, every attribute and /Thumbnail (coloured with the given colors),
//     combineone's part carries its own timepoint's time and equals a conversion of that
//     (timepoint, channel) alone with the same options, the link file over every combineone
//     part equals the converter's output, and linkparts' link file carries the manifest's
//     names, colors, wavelengths and times. Mono store (not on Windows): monitor's single
//     file equals the converter's output in data, every attribute and /Thumbnail.
// Test file I/O is C stdio: iostreams and std::filesystem copies abort on macOS here (this
// binary mixes its C++ runtime with the copy libcppZarr exports).
//   usage: parallelTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <ctime>
#include <sys/stat.h>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include "zarr.h"                // cpp-zarr (also pulls in nlohmann::json)
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims comparison (HDF5 + lz4)

namespace fs = std::filesystem;

// All odd, and none a multiple of the ~64-128 Imaris chunk sizes: exercises edge
// padding at level 0 and an odd-size downsample (voxel dropped per axis) at level 1.
static const uint64_t X = 149, Y = 129, Z = 101;
// The 2-timepoint volume: a single-node file of 2 timepoints chunks level 0 as 128x128x16
// (1 timepoint: 64x64x128), so the workers must use the 2-timepoint layout.
static const uint64_t X2 = 140, Y2 = 120, Z2 = 79;

// Distinct data per channel (seed) so the two channels can't be confused and their
// histograms / auto color ranges differ.
static std::vector<uint16_t> makeVolume(int seed, uint64_t nx = X, uint64_t ny = Y, uint64_t nz = Z, uint64_t range = 4096){
    std::vector<uint16_t> buf(nx*ny*nz);
    for(uint64_t z=0; z<nz; z++)
      for(uint64_t y=0; y<ny; y++)
        for(uint64_t x=0; x<nx; x++)
          buf[x + y*nx + z*nx*ny] = (uint16_t)((x*7 + y*13 + z*17 + seed*137) % range + 1);
    return buf;
}

static std::string q(const fs::path& p){
    return "\"" + fs::path(p).make_preferred().string() + "\"";
}
// Runs a shell command; its exit code (-1 if it did not exit normally).
static int runCmd(std::string cmd){
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    int rc = std::system(cmd.c_str());
#ifdef _WIN32
    return rc;
#else
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : -1;
#endif
}
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
static void copyFile(const fs::path& from, const fs::path& to, std::error_code& ec){
    ec.clear(); std::string b;
    if(!readBytes(from, b) || !writeBytes(to, b)) ec = std::make_error_code(std::errc::io_error);
}
static void copyTree(const fs::path& from, const fs::path& to, std::error_code& ec){
    fs::create_directories(to, ec); if(ec) return;
    for(fs::recursive_directory_iterator it(from, ec), end; !ec && it != end; it.increment(ec)){
        const fs::path dst = to / fs::relative(it->path(), from, ec); if(ec) return;
        if(it->is_directory(ec)) fs::create_directories(dst, ec); else copyFile(it->path(), dst, ec);
    }
}

static void writeZarr(const fs::path& path, const std::vector<uint16_t>& buf, uint64_t nx = X, uint64_t ny = Y, uint64_t nz = Z){
    std::vector<uint64_t> shape={nx,ny,nz}, chunks={32,32,32};
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0,0,0}, shape);
    if(parallelWriteZarr(Zw, (void*)buf.data(), {0,0,0}, shape, shape, 16, false, false, false)){
        std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); std::exit(1); }
}

// Read setup's JSON manifest: jobsPerLevel[r] = number of workers for level r (run level 0's
// array first, then level 1's, ...); channels/timepoints are logged; store = mono or packs.
static bool readManifest(const fs::path& mf, int& nC, int& nT, std::vector<long>& jobsPerLevel, std::string* store = nullptr){
    std::string b;
    if(!readBytes(mf, b)){ std::printf("  FAIL: no manifest at %s\n", mf.string().c_str()); return false; }
    nlohmann::json j;
    try { j = nlohmann::json::parse(b); } catch(const std::exception& e){ std::printf("  FAIL: bad manifest.json: %s\n", e.what()); return false; }
    nC = j.value("channels", 0); nT = j.value("timepoints", 0);
    if(store) *store = j.value("store", std::string());
    jobsPerLevel.clear();
    if(j.contains("jobsPerLevel")) for(auto& n : j["jobsPerLevel"]) jobsPerLevel.push_back(n.get<long>());
    return nC > 0 && nT > 0 && !jobsPerLevel.empty();
}
// Every worker of every level, in level order, as one process per tile (count = 1) or per
// `count` consecutive tiles.
static bool runWorkers(const std::string& exe, const fs::path& wd, const std::vector<long>& jobsPerLevel, long count = 1){
    for(size_t r=0; r<jobsPerLevel.size(); ++r)              // levels in order: level r+1 reads level r's chunks
        for(long i=0; i<jobsPerLevel[r]; i+=count){
            std::string cmd = q(exe)+" worker "+q(wd)+" "+std::to_string(r)+" "+std::to_string(i);
            if(count > 1) cmd += " "+std::to_string(count);
            if(runCmd(cmd) != 0){ std::printf("  FAIL: worker level %zu index %ld\n", r, i); return false; } }
    return true;
}
static bool compareTo(const fs::path& ref, const fs::path& out, const char* what){
    std::printf("[compare] %s\n", what);
    ImsCompareOptions opt; ImsCompareReport report;
    bool same = imsCompare(ref, out, report, opt);
    imsPrintReport(report);
    if(!same) std::printf("  FAIL: %s differs from the monolithic conversion\n", what);
    return same;
}

// --- Failure cases (B-1): each broken input must give a non-zero exit, and no output file. ---
static void writeZarrU8(const fs::path& path){
    std::vector<uint8_t> buf(X*Y*Z); for(size_t i=0; i<buf.size(); i++) buf[i] = (uint8_t)(i % 251);
    std::vector<uint64_t> shape={X,Y,Z}, chunks={32,32,32};
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u1"); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0,0,0}, shape);
    if(parallelWriteZarr(Zw, (void*)buf.data(), {0,0,0}, shape, shape, 8, false, false, false)){
        std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); std::exit(1); }
}
static void setEnvVar(const char* k, const char* v){
#ifdef _WIN32
    _putenv_s(k, v);
#else
    setenv(k, v, 1);
#endif
}
// The std::filesystem calls in the failure cases use the error_code forms, so a failing call
// is reported and fails the case instead of throwing (an exception aborted the test with no
// message on macOS).
static bool fsOk(const char* what, const std::error_code& ec){
    if(ec){ std::printf("  FAIL: %s: %s\n", what, ec.message().c_str()); return false; } return true; }
static bool expectFail(const char* what, const std::string& cmd, const fs::path& noOutput = fs::path()){
    std::printf("[failure case] %s\n", what);
    int rc = runCmd(cmd);
    if(rc == 0){ std::printf("  FAIL: exit 0\n"); return false; }
    std::error_code ec;
    if(!noOutput.empty() && fs::exists(noOutput, ec)){ std::printf("  FAIL: %s left behind\n", noOutput.string().c_str()); return false; }
    std::printf("  ok (exit %d)\n", rc); return true;
}
// /Thumbnail/Data of b equals a's (shapeOnly: its shape only, e.g. for linkparts, whose parts here are copies of
// one 2-channel file).
static bool sameThumbnail(const fs::path& a, const fs::path& b, const char* what, bool shapeOnly = false){
    uint64_t ra = 0, ca = 0, rb = 0, cb = 0; std::vector<uint8_t> ta, tb;
    bool ga = imsReadThumbnail(a, ra, ca, ta), gb = imsReadThumbnail(b, rb, cb, tb);
    bool ok = ga && gb && ra == rb && ca == cb && (shapeOnly || ta == tb);
    std::printf("  /Thumbnail of %s: %s (%llu x %llu bytes vs %llu x %llu)\n", what, ok ? (shapeOnly ? "same shape" : "identical") : "DIFFERENT",
                (unsigned long long)rb, (unsigned long long)cb, (unsigned long long)ra, (unsigned long long)ca);
    return ok;
}
// wd = the finished packs workdir (manifest + all packs), monoIms = the monolithic 2-channel output.
static bool failureCases(const std::string& exe, const fs::path& work, const fs::path& wd, const fs::path& monoIms){
    const std::string E = q(exe); bool ok = true; std::error_code ec;
    fs::path none = work/"fc_nomanifest"; fs::create_directories(none, ec); ok &= fsOk("create fc_nomanifest", ec);
    ok &= expectFail("worker without a manifest", E+" worker "+q(none)+" 0 0");
    ok &= expectFail("monitor without a manifest", E+" monitor "+q(none)+" "+q(none/"out.ims"), none/"out.ims");
    ok &= expectFail("combineone without a manifest", E+" combineone "+q(none)+" 0 0 "+q(none/"out.ims"), none/"out.ims");
    ok &= expectFail("link without a manifest", E+" link "+q(none)+" "+q(none/"out.ims")+" parts", none/"out.ims");
    ok &= expectFail("linkparts without a manifest", E+" linkparts "+q(none)+" "+q(none/"out.ims")+" parts", none/"out.ims");

    // uint8 data: setup records it (linkparts can take it), the uint16-only tile worker refuses it
    fs::path z8 = work/"fc_zarr_u8"; fs::create_directories(z8, ec); ok &= fsOk("create fc_zarr_u8", ec); writeZarrU8(z8/"CamA_t0000.zarr");
    fs::path wd8 = work/"fc_u8";
    if(runCmd(E+" setup "+q(z8)+" CamA "+q(wd8)+" 64,64,64 1,1,1 --store packs") != 0){ std::printf("  FAIL: setup on uint8 data\n"); ok = false; }
    ok &= expectFail("worker on uint8 data", E+" worker "+q(wd8)+" 0 0");
    ok &= expectFail("setup with an unknown store", E+" setup "+q(z8)+" CamA "+q(work/"fc_store")+" 64,64,64 1,1,1 --store nowhere");

    // chunks/ is a file, so no pack can be created (fails for any user on any platform)
    fs::path wdw = work/"fc_nowrite"; fs::create_directories(wdw, ec); ok &= fsOk("create fc_nowrite", ec);
    copyFile(wd/"manifest.json", wdw/"manifest.json", ec); ok &= fsOk("copy manifest.json", ec);
    ok &= writeBytes(wdw/"chunks", "x");
    ok &= expectFail("worker with an unwritable chunks directory", E+" worker "+q(wdw)+" 0 0");

    ok &= expectFail("monitor writing into a missing directory", E+" monitor "+q(wd)+" "+q(work/"fc_no"/"such"/"out.ims"));

    fs::path wdt = work/"fc_trunc"; copyTree(wd, wdt, ec); ok &= fsOk("copy workdir to fc_trunc", ec);
    fs::path pk = wdt/"chunks"/"L0_C0_T0_0.pack"; const auto pkSize = fs::file_size(pk, ec); ok &= fsOk("size of a pack", ec);
    if(!ec){ fs::resize_file(pk, pkSize - 100, ec); ok &= fsOk("truncate a pack", ec); }
    ok &= expectFail("monitor with a truncated pack", E+" monitor "+q(wdt)+" "+q(work/"fc_trunc.ims"), work/"fc_trunc.ims");

    // one pack missing plus one stale pack: the stale one must not count as the missing one
    fs::path wds = work/"fc_stale"; copyTree(wd, wds, ec); ok &= fsOk("copy workdir to fc_stale", ec);
    fs::remove(wds/"chunks"/"L0_C1_T0_0.pack", ec); ok &= fsOk("remove a pack", ec);
    copyFile(wds/"chunks"/"L0_C0_T0_0.pack", wds/"chunks"/"L0_C0_T0_999.pack", ec); ok &= fsOk("make a stale pack", ec);
    setEnvVar("PIW_MONITOR_TIMEOUT", "2");
    ok &= expectFail("monitor with one pack missing and a stale one (2 s timeout)", E+" monitor "+q(wds)+" "+q(work/"fc_stale.ims"), work/"fc_stale.ims");
    setEnvVar("PIW_MONITOR_TIMEOUT", "");

    // linkparts with a missing part, then with every part present (positive control)
    fs::path lp = work/"fc_link"; fs::create_directories(lp/"parts", ec); ok &= fsOk("create fc_link/parts", ec);
    copyFile(monoIms, lp/"parts"/"part_t0_c0.ims", ec); ok &= fsOk("copy part_t0_c0", ec);
    ok &= expectFail("linkparts with a missing part", E+" linkparts "+q(wd)+" "+q(lp/"link.ims")+" parts", lp/"link.ims");
    copyFile(monoIms, lp/"parts"/"part_t0_c1.ims", ec); ok &= fsOk("copy part_t0_c1", ec);
    if(runCmd(E+" linkparts "+q(wd)+" "+q(lp/"link.ims")+" parts") != 0 || !fs::exists(lp/"link.ims")){
        std::printf("  FAIL: linkparts with every part present\n"); ok = false; }
    else ok &= sameThumbnail(monoIms, lp/"link.ims", "linkparts", true);
    return ok;
}

#ifndef _WIN32
// ---- mono store ----
// Runs a shell command with its stdout + stderr in out (also printed); its exit code.
static int runOut(const std::string& cmd, std::string& out, const fs::path& log){
    int rc = runCmd("(" + cmd + ") > " + q(log) + " 2>&1");
    readBytes(log, out); std::printf("%s", out.c_str());
    return rc;
}
static uint64_t fileSize(const fs::path& p){ std::error_code ec; uint64_t n = fs::file_size(p, ec); return ec ? 0 : n; }
// Every link in the file is a hard link (no external or soft links: the file stands alone); counts the datasets.
struct LinkWalk { int notHard = 0, datasets = 0; };
static herr_t walkCb(hid_t g, const char* name, const H5L_info2_t* info, void* ud){
    LinkWalk& w = *(LinkWalk*)ud;
    if(info->type != H5L_TYPE_HARD){ std::printf("  not a hard link: %s\n", name); w.notHard++; return 0; }
    H5O_info2_t oi;
    if(H5Oget_info_by_name3(g, name, &oi, H5O_INFO_BASIC, H5P_DEFAULT) < 0) return -1;
    if(oi.type == H5O_TYPE_GROUP){ imsutil::Hid c(H5Gopen2(g, name, H5P_DEFAULT), H5Gclose); if(!c.valid()) return -1;
        if(H5Literate2(c, H5_INDEX_NAME, H5_ITER_INC, nullptr, walkCb, ud) < 0) return -1; }
    else if(oi.type == H5O_TYPE_DATASET) w.datasets++;
    return 0;
}
static bool allHardLinks(const fs::path& file){
    imsutil::Hid f = imsutil::openFile(file); LinkWalk w;
    bool ok = f.valid() && H5Literate2(f, H5_INDEX_NAME, H5_ITER_INC, nullptr, walkCb, &w) >= 0 && w.notHard == 0 && w.datasets > 0;
    std::printf("  %s: %d datasets, %s\n", file.filename().string().c_str(), w.datasets, ok ? "every link a hard link" : "NOT self-contained");
    return ok;
}
static bool noPacks(const fs::path& chunks){
    std::error_code ec; int n = 0;
    for(fs::directory_iterator it(chunks, ec), end; !ec && it != end; it.increment(ec)) if(it->path().extension() == ".pack") n++;
    if(ec || n){ std::printf("  FAIL: %d .pack files in %s\n", n, chunks.string().c_str()); return false; }
    return true;
}

// The mono store on the 1-timepoint volume: workers (one tile run twice at once), restart drill, monitor, checks.
static bool monoStore(const std::string& exe, const fs::path& work, const fs::path& zdir, const fs::path& monoIms){
    const std::string E = q(exe); bool ok = true; std::string o; const fs::path log = work/"cmd.log";
    auto check = [&](bool c, const char* what){ if(!c){ std::printf("  FAIL: %s\n", what); ok = false; } return c; };
    const fs::path wd = work/"parm", out = work/"parm.ims", partial = wd/"output.ims.partial", cd = wd/"chunks";
    std::printf("[mono store] setup (the default store)\n");
    if(!check(runCmd(E+" setup "+q(zdir)+" CamA,CamB "+q(wd)+" 64,64,64 1,1,1") == 0, "setup")) return false;
    int nC = 0, nT = 0; std::vector<long> jobs; std::string store;
    if(!check(readManifest(wd/"manifest.json", nC, nT, jobs, &store) && store == "mono", "the manifest's store is not mono")) return false;
    if(!check(runWorkers(exe, wd, jobs), "a worker failed")) return false;
    // tile 0 of level 1, channel 0, twice at the same time (a duplicate task): both succeed
    check(runCmd(E+" worker "+q(wd)+" 1 0 & p=$!; "+E+" worker "+q(wd)+" 1 0; r=$?; wait $p || exit 1; exit $r") == 0, "a duplicate pair of workers");
    check(noPacks(cd), "no packs");
    check(runOut(E+" monostatus "+q(wd), o, log) == 0 && o.find("tiles placed") != std::string::npos, "monostatus of a complete tile workdir");

    std::printf("[mono store] restart: a lost sidecar\n");
    const long perCT = jobs[0] / nC;    // level 0's tiles per channel (1 timepoint)
    const fs::path lost = cd/("L0_C1_T0_0.idx"); std::string idxBefore, idxAfter;
    readBytes(lost, idxBefore); const uint64_t sizeBefore = fileSize(partial);
    std::error_code ec; fs::remove(lost, ec);
    check(runOut(E+" monostatus "+q(wd), o, log) == 3 && o.find("L0_C1_T0_0") != std::string::npos, "monostatus names the lost tile");
    setEnvVar("PIW_MONITOR_TIMEOUT", "2");
    check(runOut(E+" monitor "+q(wd)+" "+q(out), o, log) == 1 && o.find("L0_C1_T0_0.idx") != std::string::npos && !fs::exists(out), "monitor with a lost sidecar");
    setEnvVar("PIW_MONITOR_TIMEOUT", "");
    check(runOut(E+" monofinish "+q(wd)+" "+q(out), o, log) == 3 && o.find("L0_C1_T0_0") != std::string::npos && !fs::exists(out), "monofinish with a lost sidecar");
    check(runCmd(E+" worker "+q(wd)+" 0 "+std::to_string(perCT)) == 0, "re-running the worker of the lost tile");
    readBytes(lost, idxAfter);
    check(!idxBefore.empty() && idxBefore == idxAfter && fileSize(partial) == sizeBefore, "the re-run changed the sidecar or the file size");

    std::printf("[mono store] parts cannot go into a tile workdir\n");
    check(runOut(E+" monoplace "+q(wd)+" 0 0 "+q(monoIms), o, log) == 1 && o.find("tile workers") != std::string::npos, "monoplace into a tile workdir");

    std::printf("[mono store] monitor (index the placed chunks)\n");
    if(!check(runCmd(E+" monitor "+q(wd)+" "+q(out)+" --verify") == 0 && fs::exists(out) && !fs::exists(partial), "monitor produced no .ims")) return false;
    ok &= compareTo(monoIms, out, "monolithic vs mono store");
    check(allHardLinks(out), "a link of the single file is not a hard link");
    ImsCompareReport deep; check(imsDeepCheck(out, deep, 2), "deep check of the single file");

    std::printf("[mono store] after the finish\n");
    check(runOut(E+" worker "+q(wd)+" 0 0", o, log) == 1 && o.find("sealed") != std::string::npos, "a worker after the finish");
    check(runOut(E+" monitor "+q(wd)+" "+q(out), o, log) == 0 && o.find("already finished") != std::string::npos, "monitor again");
    check(runOut(E+" combineone "+q(wd)+" 0 0 "+q(work/"fc_parm.ims"), o, log) == 1 && !fs::exists(work/"fc_parm.ims"), "combineone of a mono-store workdir");
    return ok;
}
#endif

// Both stores on 2 timepoints x 2 channels; workers given several tiles at once.
static bool twoTimepoints(const std::string& exe, const fs::path& work){
    const std::string E = q(exe); bool ok = true;
    fs::path zdir = work/"zarr2"; fs::create_directories(zdir);
    for(int t = 0; t < 2; t++){
        // every (t, c) with its own intensity range: the ColorRange spans both timepoints
        writeZarr(zdir/("CamA_t000"+std::to_string(t)+".zarr"), makeVolume(2*t, X2, Y2, Z2, 3000 + 1700*t), X2, Y2, Z2);
        writeZarr(zdir/("CamB_t000"+std::to_string(t)+".zarr"), makeVolume(2*t+1, X2, Y2, Z2, 5000 + 1700*t), X2, Y2, Z2); }
    fs::path ref = work/"mono2"; fs::create_directories(ref);
    std::printf("[2 timepoints] monolithic\n");
    if(runCmd(q(exe)+" -P CamA,CamB -F "+q(zdir)+" -o "+q(ref)+" -r zarr -v 1,1,1") != 0 || !fs::exists(ref/"output.ims")){
        std::printf("  FAIL: monolithic conversion of 2 timepoints\n"); return false; }
    std::vector<std::string> stores = {"packs"};
#ifndef _WIN32
    stores.push_back("mono");
#endif
    for(const std::string& st : stores){
        const fs::path wd = work/("par2_"+st), out = work/("par2_"+st+".ims");
        std::printf("[2 timepoints] %s store\n", st.c_str());
        int nC = 0, nT = 0; std::vector<long> jobs;
        bool made = runCmd(E+" setup "+q(zdir)+" CamA,CamB "+q(wd)+" 64,64,64 1,1,1 --store "+st) == 0
                    && readManifest(wd/"manifest.json", nC, nT, jobs) && nT == 2 && nC == 2
                    && runWorkers(exe, wd, jobs, 5)
                    && runCmd(E+" monitor "+q(wd)+" "+q(out)) == 0 && fs::exists(out);
        if(!made){ std::printf("  FAIL: 2 timepoints, %s store: setup, a worker or monitor failed\n", st.c_str()); ok = false; continue; }
        ok &= compareTo(ref/"output.ims", out, ("monolithic vs "+st+" store, 2 timepoints x 2 channels").c_str());
    }
#ifndef _WIN32
    // the mono store with other channel names and colors (a hex one whose components are neither 0 nor 1): the
    // whole file, /DataSetInfo and /Thumbnail included, equals a single-node conversion with the same options
    const std::string meta = " --channel-names Membrane,Nuclei --colors magenta,40C0A0";
    const fs::path refm = work/"mono2_meta", wdm = work/"par2_mono_meta", outm = work/"par2_mono_meta.ims";
    fs::create_directories(refm);
    std::printf("[2 timepoints] mono store with channel names and colors\n");
    int nC = 0, nT = 0; std::vector<long> jobs;
    bool made = runCmd(E+" -P CamA,CamB -F "+q(zdir)+" -o "+q(refm)+" -r zarr -v 1,1,1"+meta) == 0 && fs::exists(refm/"output.ims")
                && runCmd(E+" setup "+q(zdir)+" CamA,CamB "+q(wdm)+" 64,64,64 1,1,1 --store mono"+meta) == 0
                && readManifest(wdm/"manifest.json", nC, nT, jobs) && runWorkers(exe, wdm, jobs, 3)
                && runCmd(E+" monitor "+q(wdm)+" "+q(outm)+" --verify") == 0 && fs::exists(outm);
    if(!made){ std::printf("  FAIL: 2 timepoints, mono store with metadata options: a conversion, setup, a worker or monitor failed\n"); ok = false; }
    else ok &= compareTo(refm/"output.ims", outm, "monolithic vs mono store, 2 timepoints x 2 channels, channel names and colors");
#endif
    return ok;
}

// --- metadata (A-10) ---
static std::string localStamp(const fs::path& p){      // modification time, local, via strftime
    struct stat st;
    if(stat(fs::path(p).make_preferred().string().c_str(), &st) != 0) return "<no mtime>";
    const time_t t = st.st_mtime;
    const struct tm* lt = std::localtime(&t);
    char b[64];
    if(!lt || !std::strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", lt)) return "<bad mtime>";
    return b;
}
static bool metaOk(const fs::path& ims, const ImsExpectedMeta& e, const char* what){
    std::vector<std::string> problems;
    bool ok = imsCheckDataSetInfo(ims, e, problems);
    for(size_t i = 0; i < problems.size() && i < 20; ++i) std::printf("    %s\n", problems[i].c_str());
    std::printf("  %s: %s\n", ok ? "ok" : "FAIL", what);
    return ok;
}
// b equals a in data, every attribute and /Thumbnail (imscompare --all).
static bool sameFile(const fs::path& a, const fs::path& b, const char* what){
    ImsCompareOptions opt; ImsCompareReport report;
    const bool same = imsCompare(a, b, report, opt);
    imsPrintReport(report);
    std::printf("  %s: %s\n", same ? "ok" : "FAIL", what);
    return same;
}
static ImsExpectedMeta expectedMeta(uint64_t x, uint64_t y, uint64_t z){
    ImsExpectedMeta e; e.size[0] = x; e.size[1] = y; e.size[2] = z; e.version = PIW_VERSION; return e;
}
static bool runAllWorkers(const std::string& exe, const fs::path& wd){
    int nC = 0, nT = 0; std::vector<long> jobsPerLevel;
    return readManifest(wd/"manifest.json", nC, nT, jobsPerLevel) && runWorkers(exe, wd, jobsPerLevel);
}
// 2 channels x 2 timepoints with the metadata options on both paths (the cluster path through both stores).
static bool metadataCase(const std::string& exe, const fs::path& work){
    std::printf("[metadata] converter and cluster path with the metadata options\n");
    const std::vector<uint64_t> shape = {37, 29, 23};
    fs::path zd = work/"meta_zarr"; fs::create_directories(zd);
    const char* names[2][2] = {{"Scan_Iter_0000_CamA_ch0_0000000msec_0000100000msecAbs.zarr", "Scan_Iter_0001_CamA_ch0_0003500msec_0000103500msecAbs.zarr"},
                               {"Scan_Iter_0000_CamB_ch1_0000010msec_0000100010msecAbs.zarr", "Scan_Iter_0001_CamB_ch1_0003510msec_0000103510msecAbs.zarr"}};
    for(int c = 0; c < 2; ++c) for(int t = 0; t < 2; ++t){
        std::vector<uint16_t> v(shape[0]*shape[1]*shape[2]);
        for(size_t i = 0; i < v.size(); ++i) v[i] = (uint16_t)((i*31 + c*977 + t*313) % 3001 + 1);
        writeZarr(zd/names[c][t], v, shape[0], shape[1], shape[2]);
    }
    const std::string flags = " --channel-names GFP,RFP --colors cyan,FF8000 --emission 525,610 --excitation 488,0"
                              " --start-time \"2026-12-31 23:59:58.500\"";
    fs::path md = work/"meta_mono"; fs::create_directories(md);
    if(runCmd(q(exe)+" -P CamA,CamB -F "+q(zd)+" -o "+q(md)+" -r zarr -v 0.5,0.5,2"+flags) != 0){ std::printf("  FAIL: converter\n"); return false; }
    fs::path wd = work/"meta_par";
    if(runCmd(q(exe)+" setup "+q(zd)+" CamA,CamB "+q(wd)+" 64,64,64 0.5,0.5,2 --store packs"+flags) != 0){ std::printf("  FAIL: setup\n"); return false; }
    if(!runAllWorkers(exe, wd)) return false;
    const fs::path par = work/"meta_par.ims";
    if(runCmd(q(exe)+" monitor "+q(wd)+" "+q(par)) != 0){ std::printf("  FAIL: monitor\n"); return false; }

    ImsExpectedMeta e = expectedMeta(37, 29, 23);
    e.voxel[0] = 0.5f; e.voxel[1] = 0.5f; e.voxel[2] = 2.f;
    e.names = {"GFP", "RFP"};
    e.colors = {"0.000 1.000 1.000", "1.000 0.502 0.000"};
    e.emission = {"525", "610"}; e.excitation = {"488", ""};
    e.times = {"2026-12-31 23:59:58.500", "2027-01-01 00:00:02"};   // CamA's stamps: +3.5 s
    bool ok = metaOk(md/"output.ims", e, "converter: every DataSetInfo attribute");
    ok &= metaOk(par, e, "monitor: every DataSetInfo attribute");
    ok &= sameFile(md/"output.ims", par, "monitor's output equals the converter's (data, every attribute, /Thumbnail)");

    // combineone (t=1, c=1): one channel, one timepoint, with that timepoint's time
    const fs::path one = work/"meta_one.ims";
    if(runCmd(q(exe)+" combineone "+q(wd)+" 1 1 "+q(one)) != 0){ std::printf("  FAIL: combineone\n"); return false; }
    ImsExpectedMeta e1 = e;
    e1.names = {"RFP"}; e1.colors = {e.colors[1]}; e1.emission = {e.emission[1]}; e1.excitation = {e.excitation[1]}; e1.times = {e.times[1]};
    ok &= metaOk(one, e1, "combineone: the channel's and the timepoint's metadata");
    fs::path od = work/"meta_one_ref"; fs::create_directories(od);
    if(runCmd(q(exe)+" -P Scan_Iter_0001_CamB -F "+q(zd)+" -o "+q(od)+" -r zarr -v 0.5,0.5,2"
              " --channel-names RFP --colors FF8000 --emission 610 --start-time \"2027-01-01 00:00:02\"") != 0){
        std::printf("  FAIL: converter of (t=1, c=1) alone\n"); return false; }
    ok &= sameFile(od/"output.ims", one, "combineone's part equals a conversion of that (timepoint, channel) alone with the same options");

    // link over the combineone part of every (t, c): equals the converter's output, /Thumbnail included
    fs::path lk = work/"meta_lk"; { std::error_code ec; fs::create_directories(lk/"parts", ec); }
    for(int t = 0; t < 2; ++t) for(int c = 0; c < 2; ++c)
        if(runCmd(q(exe)+" combineone "+q(wd)+" "+std::to_string(t)+" "+std::to_string(c)+" "+
                  q(lk/"parts"/("part_t"+std::to_string(t)+"_c"+std::to_string(c)+".ims"))) != 0){ std::printf("  FAIL: combineone\n"); return false; }
    if(runCmd(q(exe)+" link "+q(wd)+" "+q(lk/"link.ims")+" parts") != 0){ std::printf("  FAIL: link\n"); return false; }
    ok &= sameFile(md/"output.ims", lk/"link.ims", "link over the combineone parts equals the converter's output");

    // linkparts over parts that are copies of the converter's output: the manifest's metadata
    fs::path lp = work/"meta_link"; std::error_code ec; fs::create_directories(lp/"parts", ec);
    for(int t = 0; t < 2; ++t) for(int c = 0; c < 2; ++c){
        copyFile(md/"output.ims", lp/"parts"/("part_t"+std::to_string(t)+"_c"+std::to_string(c)+".ims"), ec);
        if(!fsOk("copy a part", ec)) return false;
    }
    if(runCmd(q(exe)+" linkparts "+q(wd)+" "+q(lp/"link.ims")+" parts") != 0){ std::printf("  FAIL: linkparts\n"); return false; }
    ok &= metaOk(lp/"link.ims", e, "linkparts: every DataSetInfo attribute");

#ifndef _WIN32
    // the mono store with the same options, its tiles smaller than the volume: monitor's single file equals the
    // converter's output in data, every attribute and /Thumbnail (coloured with the given colors)
    const fs::path wdm = work/"meta_parm", parm = work/"meta_parm.ims";
    if(runCmd(q(exe)+" setup "+q(zd)+" CamA,CamB "+q(wdm)+" 16,16,16 0.5,0.5,2 --store mono"+flags) != 0){ std::printf("  FAIL: setup (mono store)\n"); return false; }
    if(!runAllWorkers(exe, wdm)) return false;
    if(runCmd(q(exe)+" monitor "+q(wdm)+" "+q(parm)+" --verify") != 0 || !fs::exists(parm)){ std::printf("  FAIL: monitor (mono store)\n"); return false; }
    ok &= noPacks(wdm/"chunks");
    ok &= metaOk(parm, e, "mono store: every DataSetInfo attribute");
    ok &= sameFile(md/"output.ims", parm, "the mono store's single file equals the converter's output (data, every attribute, /Thumbnail)");
#endif
    return ok;
}

int main(int argc, char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);   // nothing lost if the test dies
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_paralleltest";
    fs::remove_all(work); fs::create_directories(work);

    // --- Two-channel Zarr input (CamA/CamB, single timepoint, distinct data) ---
    fs::path zdir = work/"zarr"; fs::create_directories(zdir);
    writeZarr(zdir/"CamA_t0000.zarr", makeVolume(0));
    writeZarr(zdir/"CamB_t0000.zarr", makeVolume(1));

    // --- (a) monolithic conversion (-P CamA,CamB) -> mono/output.ims ---
    fs::path monoDir = work/"mono"; fs::create_directories(monoDir);
    std::printf("[monolithic]\n");
    if(runCmd(q(exe)+" -P CamA,CamB -F "+q(zdir)+" -o "+q(monoDir)+" -r zarr -v 1,1,1") != 0
       || !fs::exists(monoDir/"output.ims")){
        std::printf("paralleltest FAILED: monolithic conversion produced no .ims\n"); return 1; }

    // --- (b) parallel scatter/gather, packs store: setup -> workers -> monitor (merge) -> par.ims ---
    fs::path wd = work/"par"; fs::create_directories(wd);
    fs::path parIms = work/"par.ims";
    std::printf("[parallel] setup --store packs\n");
    // small tile (64,64,64) -> multiple spatial tiles per level, exercising sub-division of a volume.
    if(runCmd(q(exe)+" setup "+q(zdir)+" CamA,CamB "+q(wd)+" 64,64,64 1,1,1 --store packs") != 0){
        std::printf("paralleltest FAILED: setup\n"); return 1; }

    int nC = 0, nT = 0; std::vector<long> jobsPerLevel;
    if(!readManifest(wd/"manifest.json", nC, nT, jobsPerLevel)){
        std::printf("paralleltest FAILED: could not read manifest\n"); return 1; }
    std::printf("[parallel] %d channel(s), %d timepoint(s), %zu levels -- workers per level in order\n",
                nC, nT, jobsPerLevel.size());
    if(!runWorkers(exe, wd, jobsPerLevel)){ std::printf("paralleltest FAILED: a worker\n"); return 1; }

    std::printf("[parallel] monitor (assemble)\n");
    if(runCmd(q(exe)+" monitor "+q(wd)+" "+q(parIms)) != 0 || !fs::exists(parIms)){
        std::printf("paralleltest FAILED: monitor produced no .ims\n"); return 1; }

    // --- compare voxel data (all channels), histograms, attributes and the thumbnail ---
    bool same = compareTo(monoDir/"output.ims", parIms, "monolithic vs packs store");

    // --- the thumbnails of combineone (vs a conversion of CamA alone) and link (vs the monolithic output) ---
    std::printf("[thumbnails] combineone, link\n");
    fs::path lk = work/"lk", camA = work/"camA"; fs::create_directories(lk/"parts"); fs::create_directories(camA);
    bool thumbs = runCmd(q(exe)+" -P CamA -F "+q(zdir)+" -o "+q(camA)+" -r zarr -v 1,1,1") == 0;
    for(int c = 0; thumbs && c < nC; c++)
        thumbs = runCmd(q(exe)+" combineone "+q(wd)+" 0 "+std::to_string(c)+" "+q(lk/"parts"/("part_t0_c"+std::to_string(c)+".ims"))) == 0;
    thumbs = thumbs && runCmd(q(exe)+" link "+q(wd)+" "+q(lk/"link.ims")+" parts") == 0;
    if(!thumbs) std::printf("  FAIL: a conversion, combineone or link failed\n");
    else {
        thumbs &= sameThumbnail(camA/"output.ims", lk/"parts"/"part_t0_c0.ims", "combineone t0 c0 (vs CamA alone)");
        thumbs &= sameThumbnail(monoDir/"output.ims", lk/"link.ims", "link (vs monolithic)");
    }
    ImsExpectedMeta meta = expectedMeta(X, Y, Z);
    meta.names = {"CamA", "CamB"};
    meta.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
    meta.times = {localStamp(zdir/"CamA_t0000.zarr")};
    std::printf("[metadata] defaults (first input's modification time %s)\n", meta.times[0].c_str());
    bool metaSame = metaOk(monoDir/"output.ims", meta, "converter: every DataSetInfo attribute");
    metaSame &= metaOk(parIms, meta, "monitor (packs store): every DataSetInfo attribute");
    metaSame &= metadataCase(exe, work);
    bool failOk = failureCases(exe, work, wd, monoDir/"output.ims");
    bool monoOk = true;
#ifndef _WIN32
    monoOk = monoStore(exe, work, zdir, monoDir/"output.ims");
#else
    std::printf("[mono store] skipped (the single-file store is not supported on Windows)\n");
#endif
    bool twoOk = twoTimepoints(exe, work);
    if(same && thumbs && metaSame && failOk && monoOk && twoOk){
        std::printf("paralleltest PASSED (parallel scatter/gather == monolithic, %d channels, both stores)\n", nC); return 0; }
    std::printf("paralleltest FAILED%s%s%s%s%s%s\n", same ? "" : " [packs store]", thumbs ? "" : " [thumbnails]", metaSame ? "" : " [metadata]",
                failOk ? "" : " [failure cases]", monoOk ? "" : " [mono store]", twoOk ? "" : " [2 timepoints]");
    return 1;
}
