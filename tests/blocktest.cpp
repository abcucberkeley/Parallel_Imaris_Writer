// Block-processing equivalence test.
//
// The -C block-processing flag must produce byte-identical voxel data to the
// whole-timepoint path (it only bounds how much is read into RAM at a time).
// This generates a deliberately NON-chunk-aligned volume (100x70x200 uint16) as a
// TIFF and a zstd/F-order Zarr (no committed data), runs the shipped executable
// with and without -C on each, and compares the actual voxel data in the two .ims
// files. The Imaris .ims is HDF5 with a custom (LZ4-shuffle) filter, so instead of
// decompressing we compare each Data dataset's RAW compressed chunks by chunk
// coordinate -- the compressor is deterministic, so identical voxels => identical
// chunk bytes, while ignoring non-deterministic file-level metadata (timestamps,
// chunk ordering) that defeats a plain byte compare.
//   usage: blockTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <filesystem>

#include "parallelwritetiff.h"   // cpp-tiff
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "hdf5.h"                // core HDF5 (statically linked)

namespace fs = std::filesystem;

static const uint64_t X = 100, Y = 70, Z = 200;   // none a multiple of the chunk sizes

static std::vector<uint16_t> makeVolume(){
    std::vector<uint16_t> buf(X*Y*Z);
    // value varies in all three axes so any transpose/offset error corrupts it
    for(uint64_t z=0; z<Z; z++)
      for(uint64_t y=0; y<Y; y++)
        for(uint64_t x=0; x<X; x++)
          buf[x + y*X + z*X*Y] = (uint16_t)((x*7 + y*13 + z*17) % 4096 + 1);
    return buf;
}

// Run the executable; blockArg = "" for whole-timepoint, else "x,y,z" for -C.
static bool runExe(const std::string& exe, const std::string& reader,
                   const fs::path& indir, const fs::path& outdir, const std::string& blockArg){
    fs::remove_all(outdir); fs::create_directories(outdir);
    const std::string exeN = fs::path(exe).make_preferred().string();
    const std::string inN  = fs::path(indir).make_preferred().string();
    const std::string outN = fs::path(outdir).make_preferred().string();
    std::string cmd = "\"" + exeN + "\" -P CamA -F \"" + inN + "\" -o \"" + outN +
                      "\" -r " + reader + " -v 1,1,1";
    if(!blockArg.empty()) cmd += " -C " + blockArg;
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::printf("  run [%s%s]: %s\n", reader.c_str(),
                blockArg.empty()?" whole":(" -C "+blockArg).c_str(), cmd.c_str());
    std::fflush(stdout);
    int rc = std::system(cmd.c_str());
    fs::path ims = outdir / "output.ims";
    if(rc != 0 || !fs::exists(ims)){ std::printf("  FAIL: no .ims (rc=%d)\n", rc); return false; }
    return true;
}

// Compare the raw compressed chunks of every Imaris Data dataset in two .ims files.
// Returns true on identical voxel data. Walks the known Imaris layout
// (/DataSet/ResolutionLevel r/TimePoint 0/Channel c/Data) so it needs only the
// stable H5D chunk APIs -- no object-visit call whose signature changes by version.
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

static bool checkCase(const std::string& exe, const std::string& reader,
                      const fs::path& indir, const fs::path& work, const std::string& blockArg){
    fs::path whole = work/(reader+"_whole"), block = work/(reader+"_block");
    if(!runExe(exe, reader, indir, whole, ""))       return false;
    if(!runExe(exe, reader, indir, block, blockArg)) return false;
    std::printf("[%s] whole vs -C %s:\n", reader.c_str(), blockArg.c_str());
    return sameData(whole/"output.ims", block/"output.ims");
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_blocktest";
    fs::remove_all(work); fs::create_directories(work);

    std::vector<uint16_t> buf = makeVolume();

    // TIFF input
    fs::path tdir = work/"tiff"; fs::create_directories(tdir);
    if(writeTiffParallelWrapper(X,Y,Z,(tdir/"CamA_t0000.tif").string().c_str(), buf.data(),
                                16,0,8,"w",false,std::string("none"),1)){
        std::fprintf(stderr,"TIFF write failed\n"); return 1; }

    // Zarr input (zstd, F order, 32^3 chunks so blocking splits every axis)
    fs::path zdir = work/"zarr"; fs::create_directories(zdir);
    {
        std::vector<uint64_t> shape={X,Y,Z}, chunks={32,32,32};
        zarr Zw;
        Zw.set_fileName((zdir/"CamA_t0000.zarr").string());
        Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
        Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
        Zw.write_zarray();
        Zw.set_chunkInfo({0,0,0}, shape);
        if(parallelWriteZarr(Zw, buf.data(), {0,0,0}, shape, shape, 16, false, false, false)){
            std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); return 1; }
    }

    bool okT = checkCase(exe, "tiff", tdir, work, "200,200,78");   // max Z-slab blocking
    bool okZ = checkCase(exe, "zarr", zdir, work, "32,32,32");     // max 3D blocking
    if(okT && okZ){ std::printf("blocktest PASSED (tiff + zarr, block == whole)\n"); return 0; }
    std::printf("blocktest FAILED\n"); return 1;
}
