// Round-trip smoke test for the parallelimariswriter executable.
// Generates a tiny TIFF stack and a tiny zstd/F-order Zarr array on the fly (no
// committed test data), runs the shipped executable on each, and checks that a
// valid Imaris (HDF5) file comes out -- i.e. the binary and its libraries really
// run on this platform. Exits 0 iff both cases pass.
//   usage: roundtripTest <path-to-parallelimariswriter> [workdir]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>

#include "parallelwritetiff.h"   // cpp-tiff
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"

namespace fs = std::filesystem;

static bool isHDF5(const fs::path& p){
    std::ifstream f(p, std::ios::binary);
    unsigned char m[4] = {0};
    if(f) f.read(reinterpret_cast<char*>(m), 4);
    return m[0]==0x89 && m[1]=='H' && m[2]=='D' && m[3]=='F';
}

static bool runCase(const std::string& exe, const std::string& reader,
                    const fs::path& indir, const fs::path& outdir){
    fs::remove_all(outdir); fs::create_directories(outdir);
    std::string cmd = "\"" + exe + "\" -P CamA -F \"" + indir.string() +
                      "\" -o \"" + outdir.string() + "\" -r " + reader + " -v 1,1,1";
    std::printf("[%s] running: %s\n", reader.c_str(), cmd.c_str()); std::fflush(stdout);
    int rc = std::system(cmd.c_str());
    fs::path ims = outdir / "output.ims";
    if(!fs::exists(ims)){ std::printf("[%s] FAIL: no .ims produced (exe rc=%d)\n", reader.c_str(), rc); return false; }
    if(!isHDF5(ims)){ std::printf("[%s] FAIL: output is not valid HDF5\n", reader.c_str()); return false; }
    std::printf("[%s] OK: valid Imaris/HDF5 file\n", reader.c_str());
    return true;
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_roundtrip";
    fs::remove_all(work); fs::create_directories(work);

    const uint64_t X=32, Y=32, Z=4, n=X*Y*Z;
    std::vector<uint16_t> buf(n);
    for(uint64_t i=0;i<n;i++) buf[i]=(uint16_t)((i%4095)+1);

    // TIFF input
    fs::path tdir = work/"tiff"; fs::create_directories(tdir);
    if(writeTiffParallelWrapper(X,Y,Z,(tdir/"CamA_t0000.tif").string().c_str(), buf.data(),
                                16,0,8,"w",false,std::string("none"),1)){
        std::fprintf(stderr,"TIFF write failed\n"); return 1; }

    // Zarr input (zstd, F order)
    fs::path zdir = work/"zarr"; fs::create_directories(zdir);
    {
        std::vector<uint64_t> shape={X,Y,Z}, chunks={X,Y,Z};
        zarr Zw;
        Zw.set_fileName((zdir/"CamA_t0000.zarr").string());
        Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
        Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
        Zw.write_zarray();
        Zw.set_chunkInfo({0,0,0}, shape);
        if(parallelWriteZarr(Zw, buf.data(), {0,0,0}, shape, shape, 16, false, false, false)){
            std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); return 1; }
    }

    bool okT = runCase(exe,"tiff",tdir,work/"out_tiff");
    bool okZ = runCase(exe,"zarr",zdir,work/"out_zarr");
    if(okT && okZ){ std::printf("roundtrip PASSED (tiff + zstd/F zarr)\n"); return 0; }
    std::printf("roundtrip FAILED\n"); return 1;
}
