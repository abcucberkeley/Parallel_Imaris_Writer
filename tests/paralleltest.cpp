// Parallel-equivalence test.
//
// The cluster subcommands (setup/worker/combine) run as many independent
// processes -- exactly how a SLURM array job would -- and must assemble ONE .ims
// whose voxel data is byte-identical to the monolithic whole-volume conversion.
// This generates a deliberately ALL-ODD-dimension volume for TWO channels
// (CamA/CamB, distinct data) as zstd/F-order Zarr (no committed data): odd sizes
// force edge padding on every axis at level 0 and a dropped voxel per axis at each
// downsample, the exact path that first exposed a coarse-level mismatch, and the
// two channels exercise the multi-channel assembly + per-channel color/histogram.
// It converts the Zarrs twice -- (a) monolithically (-P CamA,CamB), (b) via `setup`
// + one `worker` process per (channel, timepoint, level, chunk-index) + `combine` --
// and compares the two with imsCompare (tests/imsutil.h): the raw compressed Data
// chunks of every channel, the histograms and the attributes, including the
// per-channel DataSetInfo (color, auto-adjusted ColorRange, name). Same determinism argument as
// blocktest.cpp; here the two producers are the parallel scatter/gather and the
// monolithic writer.
//   usage: parallelTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>

#include "zarr.h"                // cpp-zarr (also pulls in nlohmann::json)
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims comparison (HDF5 + lz4)

namespace fs = std::filesystem;

// All odd, and none a multiple of the ~64-128 Imaris chunk sizes: exercises edge
// padding at level 0 and an odd-size downsample (voxel dropped per axis) at level 1.
static const uint64_t X = 149, Y = 129, Z = 101;

// Distinct data per channel (seed) so the two channels can't be confused and their
// histograms / auto color ranges differ.
static std::vector<uint16_t> makeVolume(int seed){
    std::vector<uint16_t> buf(X*Y*Z);
    for(uint64_t z=0; z<Z; z++)
      for(uint64_t y=0; y<Y; y++)
        for(uint64_t x=0; x<X; x++)
          buf[x + y*X + z*X*Y] = (uint16_t)((x*7 + y*13 + z*17 + seed*137) % 4096 + 1);
    return buf;
}

static std::string q(const fs::path& p){
    return "\"" + fs::path(p).make_preferred().string() + "\"";
}
static int runCmd(std::string cmd){
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    return std::system(cmd.c_str());
}

static void writeZarr(const fs::path& path, const std::vector<uint16_t>& buf){
    std::vector<uint64_t> shape={X,Y,Z}, chunks={32,32,32};
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
// array first, then level 1's, ...); channels/timepoints are logged.
static bool readManifest(const fs::path& mf, int& nC, int& nT, std::vector<long>& jobsPerLevel){
    std::ifstream in(mf.string());
    if(!in){ std::printf("  FAIL: no manifest at %s\n", mf.string().c_str()); return false; }
    nlohmann::json j;
    try { in >> j; } catch(const std::exception& e){ std::printf("  FAIL: bad manifest.json: %s\n", e.what()); return false; }
    nC = j.value("channels", 0); nT = j.value("timepoints", 0);
    jobsPerLevel.clear();
    if(j.contains("jobsPerLevel")) for(auto& n : j["jobsPerLevel"]) jobsPerLevel.push_back(n.get<long>());
    return nC > 0 && nT > 0 && !jobsPerLevel.empty();
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
// Plain stream copies: std::filesystem::copy_file / copy aborted this test on macOS (GCC 13).
static void copyFile(const fs::path& from, const fs::path& to, std::error_code& ec){
    ec.clear();
    std::ifstream in(from, std::ios::binary); std::ofstream out(to, std::ios::binary | std::ios::trunc);
    if(!in || !out){ ec = std::make_error_code(std::errc::io_error); return; }
    std::vector<char> buf(1 << 20);
    while(in){ in.read(buf.data(), (std::streamsize)buf.size()); out.write(buf.data(), in.gcount()); }
    if(!out) ec = std::make_error_code(std::errc::io_error);
}
static void copyTree(const fs::path& from, const fs::path& to, std::error_code& ec){
    fs::create_directories(to, ec); if(ec) return;
    for(fs::recursive_directory_iterator it(from, ec), end; !ec && it != end; it.increment(ec)){
        const fs::path dst = to / fs::relative(it->path(), from, ec); if(ec) return;
        if(it->is_directory(ec)) fs::create_directories(dst, ec); else copyFile(it->path(), dst, ec);
    }
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
// wd = the finished parallel workdir (manifest + all packs), monoIms = the monolithic 2-channel output.
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
    if(runCmd(E+" setup "+q(z8)+" CamA "+q(wd8)+" 64,64,64 1,1,1") != 0){ std::printf("  FAIL: setup on uint8 data\n"); ok = false; }
    ok &= expectFail("worker on uint8 data", E+" worker "+q(wd8)+" 0 0");

    // chunks/ is a file, so no pack can be created (fails for any user on any platform)
    fs::path wdw = work/"fc_nowrite"; fs::create_directories(wdw, ec); ok &= fsOk("create fc_nowrite", ec);
    copyFile(wd/"manifest.json", wdw/"manifest.json", ec); ok &= fsOk("copy manifest.json", ec); { std::ofstream f((wdw/"chunks").string()); f << "x"; }
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

    // --- (b) parallel scatter/gather: setup -> workers -> combine -> par.ims ---
    fs::path wd = work/"par"; fs::create_directories(wd);
    fs::path parIms = work/"par.ims";
    std::printf("[parallel] setup\n");
    // small tile (64,64,64) -> multiple spatial tiles per level, exercising sub-division of a volume.
    if(runCmd(q(exe)+" setup "+q(zdir)+" CamA,CamB "+q(wd)+" 64,64,64 1,1,1") != 0){
        std::printf("paralleltest FAILED: setup\n"); return 1; }

    int nC = 0, nT = 0; std::vector<long> jobsPerLevel;
    if(!readManifest(wd/"manifest.json", nC, nT, jobsPerLevel)){
        std::printf("paralleltest FAILED: could not read manifest\n"); return 1; }
    std::printf("[parallel] %d channel(s), %d timepoint(s), %zu levels -- workers per level in order\n",
                nC, nT, jobsPerLevel.size());

    for(size_t r=0; r<jobsPerLevel.size(); ++r)              // levels in order: level r+1 reads level r's chunks
        for(long i=0; i<jobsPerLevel[r]; ++i)
            if(runCmd(q(exe)+" worker "+q(wd)+" "+std::to_string(r)+" "+std::to_string(i)) != 0){
                std::printf("paralleltest FAILED: worker level %zu index %ld\n", r, i); return 1; }

    std::printf("[parallel] monitor (assemble)\n");
    if(runCmd(q(exe)+" monitor "+q(wd)+" "+q(parIms)) != 0 || !fs::exists(parIms)){
        std::printf("paralleltest FAILED: monitor produced no .ims\n"); return 1; }

    // --- compare voxel data (all channels), histograms and attributes; the cluster
    // output has no /Thumbnail yet, so its presence is not compared ---
    std::printf("[compare] monolithic vs parallel:\n");
    ImsCompareOptions opt; opt.thumbnail = false;
    ImsCompareReport report;
    bool same = imsCompare(monoDir/"output.ims", parIms, report, opt);
    imsPrintReport(report);
    bool failOk = failureCases(exe, work, wd, monoDir/"output.ims");
    if(same && failOk){
        std::printf("paralleltest PASSED (parallel scatter/gather == monolithic, %d channels)\n", nC); return 0; }
    std::printf("paralleltest FAILED\n"); return 1;
}
