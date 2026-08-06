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
// and compares the raw compressed Data chunks of every channel plus the per-channel
// DataSetInfo (color, auto-adjusted ColorRange, name). Same determinism argument as
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
#include "hdf5.h"                // core HDF5 (statically linked)

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

// --- Data-chunk comparison (walks the known Imaris layout with the stable H5D chunk
// APIs and compares raw compressed chunk bytes across every resolution/channel). ---
static bool sameData(const fs::path& fa, const fs::path& fb){
    H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){ std::printf("  FAIL: cannot open .ims\n"); return false; }
    bool ok = true; int ndsets = 0; long nchunks = 0;
    for(int r=0; ok; ++r){
        bool foundLevel = false;
        for(int c=0; ok; ++c){
            char path[512];
            std::snprintf(path, sizeof(path),
                "/DataSet/ResolutionLevel %d/TimePoint 0/Channel %d/Data", r, c);
            hid_t da = H5Dopen2(a, path, H5P_DEFAULT);
            if(da < 0) break;                 // no more channels at this resolution level
            foundLevel = true; ++ndsets;
            hid_t db = H5Dopen2(b, path, H5P_DEFAULT);
            if(db < 0){ std::printf("  DIFFERS: %s missing in second file\n", path); ok=false; H5Dclose(da); break; }
            hid_t sa = H5Dget_space(da);
            hsize_t nch = 0; H5Dget_num_chunks(da, sa, &nch);
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
            H5Sclose(sa); H5Dclose(da); H5Dclose(db);
        }
        if(!foundLevel) break;                // no such resolution level -> done
    }
    H5Fclose(a); H5Fclose(b);
    if(ok) std::printf("  identical (%d Data dsets, %ld chunks)\n", ndsets, nchunks);
    return ok && ndsets > 0;
}

// Read an .ims string attribute. They are stored as a 1-D array of H5T_C_S1 (size-1)
// elements (no NUL), so the length is the dataspace element count, not the type size.
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

// Compare the per-channel DataSetInfo (color, auto-adjusted ColorRange, name, ...).
static bool sameChannelInfo(const fs::path& fa, const fs::path& fb){
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){ std::printf("  FAIL: cannot open .ims\n"); return false; }
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
            if(va != vb){ std::printf("  DIFFERS: Channel %d %s: mono=%s par=%s\n", c, k, va.c_str(), vb.c_str()); ok=false; }
        }
        std::printf("  Channel %d: Color=%s ColorRange=%s Name=%s\n", c,
            readStrAttr(gb,"Color").c_str(), readStrAttr(gb,"ColorRange").c_str(), readStrAttr(gb,"Name").c_str());
        H5Gclose(ga); H5Gclose(gb); ++c;
    }
    H5Fclose(a); H5Fclose(b);
    return ok && c > 0;
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

    // --- compare voxel data (all channels) + per-channel DataSetInfo ---
    std::printf("[compare] data chunks (monolithic vs parallel):\n");
    bool dataOk = sameData(monoDir/"output.ims", parIms);
    std::printf("[compare] per-channel DataSetInfo:\n");
    bool metaOk = sameChannelInfo(monoDir/"output.ims", parIms);
    if(dataOk && metaOk){
        std::printf("paralleltest PASSED (parallel scatter/gather == monolithic, %d channels)\n", nC); return 0; }
    std::printf("paralleltest FAILED\n"); return 1;
}
