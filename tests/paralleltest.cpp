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
// Metadata (A-10): every /DataSetInfo attribute of both outputs is checked against the
// expected values (channel names = the patterns, R/G colors, the first input's modification
// time), and a second, smaller 2-channel x 2-timepoint run passes the metadata options
// (names, colors, wavelengths, start time; times from "<n>msecAbs" stamps in the file
// names) to both the converter and `setup`: monitor's output must equal the converter's in
// data and every attribute, combineone's part carries its own timepoint's time, and
// linkparts' link file the manifest's names, colors, wavelengths and times.
//   usage: parallelTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <ctime>
#include <sys/stat.h>

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

static void writeZarr(const fs::path& path, const std::vector<uint16_t>& buf,
                      const std::vector<uint64_t>& shape = {X,Y,Z}){
    std::vector<uint64_t> chunks={32,32,32};
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
static ImsExpectedMeta expectedMeta(uint64_t x, uint64_t y, uint64_t z){
    ImsExpectedMeta e; e.size[0] = x; e.size[1] = y; e.size[2] = z; e.version = PIW_VERSION; return e;
}
static bool runAllWorkers(const std::string& exe, const fs::path& wd){
    int nC = 0, nT = 0; std::vector<long> jobsPerLevel;
    if(!readManifest(wd/"manifest.json", nC, nT, jobsPerLevel)) return false;
    for(size_t r=0; r<jobsPerLevel.size(); ++r)
        for(long i=0; i<jobsPerLevel[r]; ++i)
            if(runCmd(q(exe)+" worker "+q(wd)+" "+std::to_string(r)+" "+std::to_string(i)) != 0){
                std::printf("  FAIL: worker level %zu index %ld\n", r, i); return false; }
    return true;
}
// 2 channels x 2 timepoints with the metadata options on both paths.
static bool metadataCase(const std::string& exe, const fs::path& work){
    std::printf("[metadata] converter and cluster path with the metadata options\n");
    const std::vector<uint64_t> shape = {37, 29, 23};
    fs::path zd = work/"meta_zarr"; fs::create_directories(zd);
    const char* names[2][2] = {{"Scan_Iter_0000_CamA_ch0_0000000msec_0000100000msecAbs.zarr", "Scan_Iter_0001_CamA_ch0_0003500msec_0000103500msecAbs.zarr"},
                               {"Scan_Iter_0000_CamB_ch1_0000010msec_0000100010msecAbs.zarr", "Scan_Iter_0001_CamB_ch1_0003510msec_0000103510msecAbs.zarr"}};
    for(int c = 0; c < 2; ++c) for(int t = 0; t < 2; ++t){
        std::vector<uint16_t> v(shape[0]*shape[1]*shape[2]);
        for(size_t i = 0; i < v.size(); ++i) v[i] = (uint16_t)((i*31 + c*977 + t*313) % 3001 + 1);
        writeZarr(zd/names[c][t], v, shape);
    }
    const std::string flags = " --channel-names GFP,RFP --colors cyan,FF8000 --emission 525,610 --excitation 488,0"
                              " --start-time \"2026-12-31 23:59:58.500\"";
    fs::path md = work/"meta_mono"; fs::create_directories(md);
    if(runCmd(q(exe)+" -P CamA,CamB -F "+q(zd)+" -o "+q(md)+" -r zarr -v 0.5,0.5,2"+flags) != 0){ std::printf("  FAIL: converter\n"); return false; }
    fs::path wd = work/"meta_par";
    if(runCmd(q(exe)+" setup "+q(zd)+" CamA,CamB "+q(wd)+" 64,64,64 0.5,0.5,2"+flags) != 0){ std::printf("  FAIL: setup\n"); return false; }
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
    ImsCompareOptions opt; opt.thumbnail = false;
    ImsCompareReport report;
    const bool same = imsCompare(md/"output.ims", par, report, opt);
    imsPrintReport(report);
    std::printf("  %s: monitor's output equals the converter's (data and every attribute)\n", same ? "ok" : "FAIL");
    ok &= same;

    // combineone (t=1, c=1): one channel, one timepoint, with that timepoint's time
    const fs::path one = work/"meta_one.ims";
    if(runCmd(q(exe)+" combineone "+q(wd)+" 1 1 "+q(one)) != 0){ std::printf("  FAIL: combineone\n"); return false; }
    ImsExpectedMeta e1 = e;
    e1.names = {"RFP"}; e1.colors = {e.colors[1]}; e1.emission = {e.emission[1]}; e1.excitation = {e.excitation[1]}; e1.times = {e.times[1]};
    ok &= metaOk(one, e1, "combineone: the channel's and the timepoint's metadata");

    // linkparts over parts that are copies of the converter's output: the manifest's metadata
    fs::path lp = work/"meta_link"; std::error_code ec; fs::create_directories(lp/"parts", ec);
    for(int t = 0; t < 2; ++t) for(int c = 0; c < 2; ++c){
        copyFile(md/"output.ims", lp/"parts"/("part_t"+std::to_string(t)+"_c"+std::to_string(c)+".ims"), ec);
        if(!fsOk("copy a part", ec)) return false;
    }
    if(runCmd(q(exe)+" linkparts "+q(wd)+" "+q(lp/"link.ims")+" parts") != 0){ std::printf("  FAIL: linkparts\n"); return false; }
    ok &= metaOk(lp/"link.ims", e, "linkparts: every DataSetInfo attribute");
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
    ImsExpectedMeta meta = expectedMeta(X, Y, Z);
    meta.names = {"CamA", "CamB"};
    meta.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
    meta.times = {localStamp(zdir/"CamA_t0000.zarr")};
    std::printf("[metadata] defaults (first input's modification time %s)\n", meta.times[0].c_str());
    bool metaSame = metaOk(monoDir/"output.ims", meta, "converter: every DataSetInfo attribute");
    metaSame &= metaOk(parIms, meta, "monitor: every DataSetInfo attribute");
    metaSame &= metadataCase(exe, work);
    bool failOk = failureCases(exe, work, wd, monoDir/"output.ims");
    if(same && metaSame && failOk){
        std::printf("paralleltest PASSED (parallel scatter/gather == monolithic, %d channels)\n", nC); return 0; }
    std::printf("paralleltest FAILED\n"); return 1;
}
