// Mono scale test: the finisher of a tile workdir (setup's mono store, parallel.h) at any size, without image data.
//
// Generates what the tile workers of a volume leave behind -- the manifest (setup's layout), one claim, histogram
// and sidecar per tile, and the blobs' first and last chunk headers in the single file -- then runs monofinish
// (piw::monoFinish, in this process) and reports its time and the process's peak memory. The chunk sizes are made
// up (150-450 KB, the raw region stays a hole: the file is sparse), except at the thumbnail level of timepoint 0,
// whose chunks are real (encoded test patterns), since the finisher decodes them for /Thumbnail. monofinish's own
// stock-driver check compares every dataset's chunk index with the sidecars. Then the file is checked: it opens,
// holds every dataset with its chunk count, and has a /Thumbnail.
//   usage: monoScaleTest <ignored> <workdir> [X Y Z [cx,cy,cz]] [--timepoints N] [--channels N] [--threads N]
//                        [--claim link|fcntl] [--keep]
//   Without a size (ctest): 700 x 600 x 300 voxels, 128,128,64 tiles, 2 timepoints x 2 channels (four levels, about
//   700 tiles). The workdir is deleted at the end unless --keep (a big run's sparse file has the raw size).
// Exit 0 = pass, 1 = fail, 77 = skipped (Windows: the single-file path is not supported there).
#ifdef _WIN32
#include <cstdio>
int main(){ std::printf("monoScaleTest: skipped (the single-file cluster output is not supported on Windows)\n"); return 77; }
#else
#include "imscore.h"
#include "imsthumb.h"
#include "mono.h"
#include "monoclaim.h"
#include "monoidx.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

namespace fs = std::filesystem;
using imscore::bpSize;

static double now(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static double peakGB(){ struct rusage u; getrusage(RUSAGE_SELF, &u);
#ifdef __APPLE__
    return (double)u.ru_maxrss / 1e9;    // bytes
#else
    return (double)u.ru_maxrss * 1024 / 1e9;   // KiB
#endif
}
static uint64_t mix(uint64_t x){ x ^= x >> 33; x *= 0xff51afd7ed558ccdull; x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ull; x ^= x >> 33; return x; }
static bool pwriteAll(int fd, const void* p, size_t n, uint64_t off){
    const char* c = (const char*)p; size_t put = 0;
    while(put < n){ ssize_t k = pwrite(fd, c + put, n - put, (off_t)(off + put)); if(k < 0 && errno == EINTR) continue; if(k <= 0) return false; put += (size_t)k; }
    return true;
}

int main(int argc, char** argv){
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::vector<std::string> pos; int nT = 2, nC = 2, T = 4; std::string claimMode; bool keep = false;
    for(int i = 1; i < argc; i++){ std::string a = argv[i];
        auto num = [&](int& v){ if(i + 1 >= argc || (v = atoi(argv[++i])) < 1){ std::fprintf(stderr, "%s needs a number >= 1\n", a.c_str()); std::exit(2); } };
        if(a == "--timepoints") num(nT); else if(a == "--channels") num(nC); else if(a == "--threads") num(T);
        else if(a == "--claim" && i + 1 < argc) claimMode = argv[++i];
        else if(a == "--keep") keep = true;
        else pos.push_back(a); }
    if(pos.size() != 2 && pos.size() != 5 && pos.size() != 6){
        std::fprintf(stderr, "usage: %s <ignored> <workdir> [X Y Z [cx,cy,cz]] [--timepoints N] [--channels N] [--threads N] [--claim link|fcntl] [--keep]\n", argv[0]); return 2; }
    bpSize X = 700, Y = 600, Z = 300, cap[3] = {128, 128, 64};
    if(pos.size() >= 5){ X = strtoull(pos[2].c_str(), nullptr, 10); Y = strtoull(pos[3].c_str(), nullptr, 10); Z = strtoull(pos[4].c_str(), nullptr, 10); }
    if(pos.size() == 6 && sscanf(pos[5].c_str(), "%llu,%llu,%llu", (unsigned long long*)&cap[0], (unsigned long long*)&cap[1], (unsigned long long*)&cap[2]) != 3){
        std::fprintf(stderr, "bad tile size %s\n", pos[5].c_str()); return 2; }
    if(!X || !Y || !Z || !cap[0] || !cap[1] || !cap[2]){ std::fprintf(stderr, "sizes must be > 0\n"); return 2; }
    const fs::path work = pos[1], wdp = work / "wd"; const std::string wd = wdp.string(), cd = wd + "/chunks";
    std::error_code ec; fs::remove_all(work, ec); fs::create_directories(work);

    // ---- the manifest, as setup writes it ----
    imscore::Manifest m; m.nC = nC; m.nT = nT; m.X = X; m.Y = Y; m.Z = Z; m.elem = 2; m.store = "mono";
    m.vox[0] = m.vox[1] = 0.108f; m.vox[2] = 0.25f; for(int a = 0; a < 3; a++) m.cap[a] = cap[a];
    m.lev = imscore::makeLevels(X, Y, Z, 2, cap, 1u << 20, (bpSize)nT);
    for(int c = 0; c < nC; c++) for(int t = 0; t < nT; t++) m.zarrs.push_back("synthetic_c" + std::to_string(c) + "_t" + std::to_string(t) + ".zarr");
    {   // the metadata setup resolves, here without input files: default names and colors, a fixed start time
        piw::ConvertOptions mo; mo.startTime = "2026-01-01 00:00:00";
        const std::string me = imsmeta::resolveMetadataFields(mo, (size_t)nC, (size_t)nT, NULL, m.meta);
        if(!me.empty()){ std::printf("monoScaleTest FAILED: %s\n", me.c_str()); return 1; } }
    imscore::Status is = imscore::makeDirs(cd); if(is.ok()) is = imscore::writeManifest(wd, m);
    if(!is.ok()){ std::printf("monoScaleTest FAILED: %s\n", is.message.c_str()); return 1; }
    std::vector<imscore::Vec3> sizes; for(const imscore::Lev& L : m.lev) sizes.push_back(L.size);
    const size_t thumbLevel = imsthumb::thumbnailLevel(sizes);
    struct Tile { int r, t, c; long tile; };
    std::vector<Tile> tiles; uint64_t chunks = 0;
    for(int r = 0; r < (int)m.lev.size(); r++){ const imscore::Lev& L = m.lev[(size_t)r];
        chunks += (uint64_t)L.nch[0] * L.nch[1] * L.nch[2] * nT * nC;
        for(int t = 0; t < nT; t++) for(int c = 0; c < nC; c++) for(long i = 0; i < imscore::tilesPerCT(L); i++){ Tile x = {r, t, c, i}; tiles.push_back(x); } }
    std::printf("monoScaleTest: %llu x %llu x %llu, %d timepoints x %d channels, %zu levels, %zu tiles, %llu chunks (thumbnail level %zu: real chunks)\n",
                (unsigned long long)X, (unsigned long long)Y, (unsigned long long)Z, nT, nC, m.lev.size(), tiles.size(), (unsigned long long)chunks, thumbLevel);

    // ---- what the workers leave: claims, chunk headers (real chunks at the thumbnail level), histograms, sidecars ----
    const double tg = now(); std::string e;
    { MonoClaims first; if(!first.open(imscore::monoClaimsDir(wd), claimMode, &e)){ std::printf("monoScaleTest FAILED: %s\n", e.c_str()); return 1; }
      claimMode = first.mode(); }
    { FILE* f = std::fopen((imscore::monoClaimsDir(wd) + "/tiles").c_str(), "wb"); if(f) std::fclose(f); }
    const int fd = ::open(imscore::monoFilePath(wd).c_str(), O_RDWR | O_CREAT, 0666);
    if(fd < 0){ std::printf("monoScaleTest FAILED: cannot create %s\n", imscore::monoFilePath(wd).c_str()); return 1; }
    std::mutex claimMu;   // fcntl locks are per process: this process's threads take turns
    std::atomic<size_t> next(0), done(0); std::mutex errMu; std::string err;
    auto worker = [&](){
        MonoClaims claims; std::string we;
        if(!claims.open(imscore::monoClaimsDir(wd), claimMode, &we)){ std::lock_guard<std::mutex> g(errMu); err = we; return; }
        std::vector<uint16_t> blk; std::vector<std::vector<uint8_t> > real; std::vector<uint64_t> hist(65536);
        for(;;){ const size_t i = next.fetch_add(1); if(i >= tiles.size()) break; { std::lock_guard<std::mutex> g(errMu); if(!err.empty()) break; }
            const Tile& x = tiles[i]; const imscore::Lev& L = m.lev[(size_t)x.r];
            bpSize cb0[3], cb1[3]; imscore::tileBlockRange(L, x.tile, cb0, cb1);
            const bool isReal = (size_t)x.r == thumbLevel && x.t == 0; const size_t nb = (size_t)(L.block[0] * L.block[1] * L.block[2]);
            MonoIdx ix; ix.owner = imscore::tileOwner(x.r, x.c, x.t, x.tile); ix.timepoint = x.t; ix.channel = x.c;
            MonoIdxLevel g; g.level = (uint32_t)x.r; g.dtype = MONO_U16;
            for(int a = 0; a < 3; a++){ g.size[a] = L.size[a]; g.chunk[a] = (uint32_t)L.block[a]; g.grid[a] = (uint32_t)L.nch[a]; }
            std::fill(hist.begin(), hist.end(), 0); real.clear(); uint64_t total = 0;
            for(bpSize bz = cb0[2]; bz < cb1[2]; bz++) for(bpSize by = cb0[1]; by < cb1[1]; by++) for(bpSize bx = cb0[0]; bx < cb1[0]; bx++){
                MonoChunk ch; memset(&ch, 0, sizeof ch); ch.cx = (uint32_t)bx; ch.cy = (uint32_t)by; ch.cz = (uint32_t)bz; ch.level = (uint16_t)x.r;
                const uint64_t xv = std::min(L.block[0], L.size[0] - bx * L.block[0]), yv = std::min(L.block[1], L.size[1] - by * L.block[1]), zv = std::min(L.block[2], L.size[2] - bz * L.block[2]);
                if(isReal){   // a pattern per channel, zero outside the image (as the workers write it)
                    blk.assign(nb, 0);
                    for(uint64_t z = 0; z < zv; z++) for(uint64_t y = 0; y < yv; y++) for(uint64_t v = 0; v < xv; v++){
                        const uint64_t gx = bx * L.block[0] + v, gy = by * L.block[1] + y, gz = bz * L.block[2] + z;
                        const uint16_t val = (uint16_t)(100 + ((gx / 16 + gy / 16 + gz / 8 + (uint64_t)x.c) % 4) * 700 + (gx + gy) % 50);
                        blk[v + L.block[0] * (y + L.block[1] * z)] = val; hist[val]++; }
                    real.emplace_back();
                    if(!imscore::encodeChunk((const uint8_t*)blk.data(), nb * 2, real.back())){ std::lock_guard<std::mutex> eg(errMu); err = "LZ4 failed"; return; }
                    ch.size = (uint32_t)real.back().size();
                } else {
                    ch.size = (uint32_t)(150000 + mix(((uint64_t)x.r << 56) ^ ((uint64_t)x.t << 40) ^ ((uint64_t)x.c << 36) ^ (bz << 24) ^ (by << 12) ^ bx) % 300000);
                    hist[1000 + (size_t)((x.c * 977 + x.t * 131) % 3000)] += xv * yv * zv;
                }
                ch.offset = total; total += ch.size; ix.chunks.push_back(ch);
            }
            g.nChunks = ix.chunks.size(); ix.levels.push_back(g);
            MonoClaim rec;
            { std::unique_lock<std::mutex> lk(claimMu, std::defer_lock); if(claimMode == "fcntl") lk.lock();
              if(!claims.claim(ix.owner, total, rec, &we)){ std::lock_guard<std::mutex> eg(errMu); err = we; return; } }
            ix.claimOffset = rec.offset; ix.claimSize = rec.size; ix.blobBytes = total;
            bool ok = true; uint8_t h[16];
            for(size_t k = 0; ok && k < ix.chunks.size(); k++){ MonoChunk& ch = ix.chunks[k]; ch.offset += rec.offset;
                if(isReal) ok = pwriteAll(fd, real[k].data(), real[k].size(), ch.offset);
                else if(k == 0 || k + 1 == ix.chunks.size()){   // the headers the finisher checks
                    imscore::wbe(h, nb * 2, 8); imscore::wbe(h + 8, nb * 2, 4); imscore::wbe(h + 12, ch.size - 16, 4); ok = pwriteAll(fd, h, 16, ch.offset); } }
            if(!ok || !mono_write_atomic(imscore::histName(cd, x.r, x.c, x.t, x.tile), hist.data(), imscore::HIST_BYTES, &we) ||
               !mono_idx_write(imscore::tileIdxName(cd, x.r, x.c, x.t, x.tile), ix, &we)){
                std::lock_guard<std::mutex> eg(errMu); err = ok ? we : "cannot write the single file"; return; }
            const size_t n = ++done; if(n % 2000 == 0) std::printf("  %zu of %zu tiles (%.0f s)\n", n, tiles.size(), now() - tg);
        }
    };
    { std::vector<std::thread> th; for(int w = 0; w < T; w++) th.push_back(std::thread(worker)); for(std::thread& x : th) x.join(); }
    close(fd);
    if(!err.empty()){ std::printf("monoScaleTest FAILED: generating the tiles: %s\n", err.c_str()); return 1; }
    const double genSeconds = now() - tg; const double genPeak = peakGB();
    std::printf("generated %zu tiles (%s claims) in %.1f s, peak RSS %.2f GB\n", tiles.size(), claimMode.c_str(), genSeconds, genPeak);

    // ---- the finisher ----
    piw::MonoFinishOptions o; o.workdir = wd; o.outputFile = (work / "scale.ims").string(); o.numThreads = T;
    piw::MonoFinishResult res; const double tf = now();
    piw::Status s = piw::monoFinish(o, &res);
    const double finishSeconds = now() - tf;
    if(!s.ok()){ std::printf("monoScaleTest FAILED: monofinish: %s\n", s.message.c_str()); return 1; }
    std::printf("monofinish: %d levels, %llu chunks, raw region %.3f TB, %.2f GB of HDF5 objects after it: sidecars + seal + thumbnail %.1f s, "
                "HDF5 structure %.1f s, stock-driver check %.1f s, total %.1f s; peak RSS %.2f GB (generator: %.2f GB)\n",
                res.levels, (unsigned long long)res.chunks, (double)(res.rawEnd - MONO_RAW_BEGIN) / 1e12, (double)(res.fileBytes - res.rawEnd) / 1e9,
                res.readSeconds, res.indexSeconds, res.checkSeconds, finishSeconds, peakGB(), genPeak);
    bool ok = res.chunks == chunks;
    if(!ok) std::printf("  FAIL: %llu chunks indexed, %llu expected\n", (unsigned long long)res.chunks, (unsigned long long)chunks);

    // ---- the file: every dataset with its chunk count, and the thumbnail ----
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS); H5Pset_file_locking(fapl, false, true);
    hid_t f = H5Fopen(o.outputFile.c_str(), H5F_ACC_RDONLY, fapl); H5Pclose(fapl);
    if(f < 0){ std::printf("  FAIL: %s does not open\n", o.outputFile.c_str()); return 1; }
    int datasets = 0;
    for(size_t r = 0; r < m.lev.size(); r++) for(int t = 0; t < nT; t++) for(int c = 0; c < nC; c++){
        const std::string dp = "/DataSet/ResolutionLevel " + std::to_string(r) + "/TimePoint " + std::to_string(t) + "/Channel " + std::to_string(c) + "/Data";
        hid_t d = H5Dopen2(f, dp.c_str(), H5P_DEFAULT); hsize_t n = 0; hid_t sp = d >= 0 ? H5Dget_space(d) : -1;
        const imscore::Lev& L = m.lev[r];
        if(d >= 0 && H5Dget_num_chunks(d, sp, &n) >= 0 && n == L.nch[0] * L.nch[1] * L.nch[2]) datasets++;
        else std::printf("  FAIL: %s missing or with %llu chunks\n", dp.c_str(), (unsigned long long)n);
        if(sp >= 0) H5Sclose(sp);
        if(d >= 0) H5Dclose(d);
    }
    imscore::H5Quiet quiet; const bool thumb = H5Lexists(f, "/Thumbnail", H5P_DEFAULT) > 0 && imscore::h5PathExists(f, "/Thumbnail/Data");
    H5Fclose(f);
    const int want = (int)m.lev.size() * nT * nC;
    std::printf("  %d of %d datasets with every chunk, /Thumbnail %s\n", datasets, want, thumb ? "present" : "MISSING");
    ok = ok && datasets == want && thumb;
    if(!keep){ fs::remove_all(work, ec); }
    std::printf("monoScaleTest %s\n", ok ? "PASSED" : "FAILED");
    return ok ? 0 : 1;
}
#endif
