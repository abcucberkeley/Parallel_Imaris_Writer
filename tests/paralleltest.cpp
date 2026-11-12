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

int main(int argc, char** argv){
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
    if(runCmd(q(exe)+" setup "+q(zdir)+" CamA,CamB "+q(wd)+" 64,64,64") != 0){
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
    if(same){
        std::printf("paralleltest PASSED (parallel scatter/gather == monolithic, %d channels)\n", nC); return 0; }
    std::printf("paralleltest FAILED\n"); return 1;
}
