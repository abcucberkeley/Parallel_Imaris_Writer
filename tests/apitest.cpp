// Library-API test.
//
// piw::convert (src/converter.cpp, linked in from the core objects like the executable) must
// write the same .ims as the shipped executable for the same inputs, and must fail with a
// Status -- the process alive and no partial output left -- where the command line used to
// crash. Inputs are generated here (no committed data):
//   - piw::convert against the executable: TIFF and Zarr, whole and with a processing block
//     (-C), and 2 channels x 2 time points: the raw compressed chunks of every Data dataset
//     (all levels, time points, channels) and the per-channel DataSetInfo must be equal
//   - error returns: a .zarr folder without metadata, a truncated chunk, inputs of different
//     sizes (refused before any output), channels with different numbers of files, a pattern
//     without a match; the command line exits 1 (not a crash) for the cases that used to crash
//   - cancel after a few polls; progress never decreasing and ending at 1
//   - two conversions one after the other, two at once from two threads, numThreads (and the
//     caller's OpenMP setting restored), piw::runCli twice in one process
//   - metadata (A-10): every /DataSetInfo attribute with the defaults (names "Channel c", R/G/B,
//     no wavelengths, 1 s apart from the first input's modification time), with the options
//     (names, colors, wavelengths, start time, interval), from "<n>msecAbs" stamps in the file
//     names; the same options as command-line flags give the same file; bad metadata options
//     are refused before any output; imsmeta.h's time and color helpers
//   usage: apiTest <path-to-parallelimariswriter> [workdir]
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#ifndef _WIN32
#include <sys/wait.h>
#endif

#include <sys/stat.h>
#include <ctime>
#include <omp.h>
#include "parallelimariswriter.h"
#include "imsmeta.h"             // src/: the metadata helpers both conversion paths use
#include "parallelwritetiff.h"   // cpp-tiff
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "hdf5.h"                // core HDF5 (statically linked)
#include "imsutil.h"             // imsCheckDataSetInfo, imsCompare

namespace fs = std::filesystem;

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    std::printf("  %s: %s\n", ok ? "ok" : "FAIL", what.c_str());
    std::fflush(stdout);
    if(!ok) ++gFailures;
}

static std::string native(const fs::path& p){ return fs::path(p).make_preferred().string(); }

// --- inputs ---

static std::vector<uint16_t> makeVolume(uint64_t X, uint64_t Y, uint64_t Z, int seed){
    std::vector<uint16_t> buf(X*Y*Z);
    // value varies in all three axes so any transpose/offset error corrupts it
    for(uint64_t z=0; z<Z; z++)
      for(uint64_t y=0; y<Y; y++)
        for(uint64_t x=0; x<X; x++)
          buf[x + y*X + z*X*Y] = (uint16_t)((x*7 + y*13 + z*17 + seed*137) % 4096 + 1);
    return buf;
}

static void writeZarr(const fs::path& path, uint64_t X, uint64_t Y, uint64_t Z, int seed){
    std::vector<uint16_t> buf = makeVolume(X, Y, Z, seed);
    std::vector<uint64_t> shape={X,Y,Z}, chunks={32,32,32};   // 32^3 so -C 32,32,32 splits every axis
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(5); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0,0,0}, shape);
    if(parallelWriteZarr(Zw, buf.data(), {0,0,0}, shape, shape, 16, false, false, false)){
        std::fprintf(stderr,"Zarr write failed: %s\n", Zw.get_errString().c_str()); std::exit(1); }
}

static void writeTiff(const fs::path& path, uint64_t X, uint64_t Y, uint64_t Z, int seed){
    std::vector<uint16_t> buf = makeVolume(X, Y, Z, seed);
    if(writeTiffParallelWrapper(X,Y,Z,path.string().c_str(), buf.data(),16,0,8,"w",false,std::string("none"),1)){
        std::fprintf(stderr,"TIFF write failed\n"); std::exit(1); }
}

// --- the executable ---

static int exitStatus(int rc){
#ifdef _WIN32
    return rc;
#else
    if(rc == -1) return -1;
    if(WIFEXITED(rc)) return WEXITSTATUS(rc);
    return 128 + WTERMSIG(rc);   // died from a signal
#endif
}

// Runs the executable with args, its output going to log; returns its exit status.
static int runExe(const std::string& exe, const std::string& args, const fs::path& log){
    std::string cmd = "\"" + native(exe) + "\" " + args + " > \"" + native(log) + "\" 2>&1";
#ifdef _WIN32
    cmd = "\"" + cmd + "\"";   // cmd.exe needs the whole thing re-quoted (see roundtrip.cpp)
#endif
    std::printf("  $ %s\n", cmd.c_str()); std::fflush(stdout);
    return exitStatus(std::system(cmd.c_str()));
}

static std::string readText(const fs::path& p){
    std::ifstream in(p);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

// --- .ims comparison: raw compressed chunks of every Data dataset (all resolution levels,
// time points and channels) + per-channel DataSetInfo; as tests/paralleltest.cpp ---

static bool sameData(const fs::path& fa, const fs::path& fb){
    H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){
        std::printf("  cannot open %s or %s\n", fa.string().c_str(), fb.string().c_str());
        if(a >= 0) H5Fclose(a);
        if(b >= 0) H5Fclose(b);
        return false;
    }
    bool ok = true; int ndsets = 0; long nchunks = 0;
    for(int r=0; ok; ++r){
        bool foundLevel = false;
        for(int t=0; ok; ++t){
            bool foundTp = false;
            for(int c=0; ok; ++c){
                char path[512];
                std::snprintf(path, sizeof(path), "/DataSet/ResolutionLevel %d/TimePoint %d/Channel %d/Data", r, t, c);
                hid_t da = H5Dopen2(a, path, H5P_DEFAULT);
                if(da < 0) break;                 // no more channels here
                foundLevel = foundTp = true; ++ndsets;
                hid_t db = H5Dopen2(b, path, H5P_DEFAULT);
                if(db < 0){ std::printf("  DIFFERS: %s missing in second file\n", path); ok=false; H5Dclose(da); break; }
                hid_t sa = H5Dget_space(da), sb = H5Dget_space(db);
                hsize_t nch = 0, nchb = 0; H5Dget_num_chunks(da, sa, &nch); H5Dget_num_chunks(db, sb, &nchb);
                if(nch != nchb){ std::printf("  DIFFERS: %s chunk count %llu vs %llu\n", path,
                    (unsigned long long)nch, (unsigned long long)nchb); ok=false; }
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
                H5Sclose(sa); H5Sclose(sb); H5Dclose(da); H5Dclose(db);
            }
            if(!foundTp) break;
        }
        if(!foundLevel) break;
    }
    H5Fclose(a); H5Fclose(b);
    if(ok) std::printf("  identical data (%d Data dsets, %ld chunks)\n", ndsets, nchunks);
    return ok && ndsets > 0;
}

// .ims string attributes are a 1-D array of H5T_C_S1 (size-1) elements (no NUL).
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

static bool sameChannelInfo(const fs::path& fa, const fs::path& fb){
    hid_t a = H5Fopen(fa.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    hid_t b = H5Fopen(fb.string().c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(a < 0 || b < 0){
        if(a >= 0) H5Fclose(a);
        if(b >= 0) H5Fclose(b);
        return false;
    }
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
            if(va != vb){ std::printf("  DIFFERS: Channel %d %s: %s vs %s\n", c, k, va.c_str(), vb.c_str()); ok=false; }
        }
        H5Gclose(ga); H5Gclose(gb); ++c;
    }
    H5Fclose(a); H5Fclose(b);
    return ok && c > 0;
}

static bool sameIms(const fs::path& fa, const fs::path& fb){
    return sameData(fa, fb) && sameChannelInfo(fa, fb);
}

// --- piw::convert with recording callbacks ---

struct Recorder {
    std::mutex mutex;
    std::vector<float> progress;
    std::string text;
    std::atomic<int> polls{0};
    int cancelOnPoll = 0;            // 0: never
};
static void onMessage(const char* text, void* user){
    Recorder* r = (Recorder*)user; std::lock_guard<std::mutex> lock(r->mutex); r->text += text;
}
static void onProgress(float fraction, unsigned long long, void* user){
    Recorder* r = (Recorder*)user; std::lock_guard<std::mutex> lock(r->mutex); r->progress.push_back(fraction);
}
static int onCancel(void* user){
    Recorder* r = (Recorder*)user; int n = ++r->polls; return r->cancelOnPoll > 0 && n >= r->cancelOnPoll;
}

static piw::Callbacks callbacksFor(Recorder& r){
    piw::Callbacks cb;
    cb.message = onMessage; cb.progress = onProgress; cb.cancel = onCancel; cb.user = &r;
    return cb;
}

// names: the channel names (the executable names the channels after their -P patterns).
static piw::ConvertOptions optionsFor(const std::string& reader, const std::vector<std::vector<std::string>>& files,
                                      const fs::path& out, const std::string& processBlock,
                                      const std::vector<std::string>& names = std::vector<std::string>()){
    piw::ConvertOptions o;
    o.reader = reader;
    o.files = files;
    o.channelNames = names;
    o.outputFile = native(out);
    o.voxelSize[0] = o.voxelSize[1] = o.voxelSize[2] = 1.0f;   // the executable runs with -v 1,1,1
    if(!processBlock.empty()){
        o.hasProcessBlock = true;
        std::sscanf(processBlock.c_str(), "%llu,%llu,%llu", (unsigned long long*)&o.processBlock[0],
                    (unsigned long long*)&o.processBlock[1], (unsigned long long*)&o.processBlock[2]);
    }
    return o;
}

static std::vector<std::vector<std::string>> findFiles(const fs::path& dir, const std::vector<std::string>& patterns){
    std::vector<std::vector<std::string>> files;
    piw::Status s = piw::findInputFiles({native(dir)}, patterns, files);
    if(!s.ok()){ std::printf("  findInputFiles failed: %s\n", s.message.c_str()); ++gFailures; }
    return files;
}

static bool progressOk(const Recorder& r){
    if(r.progress.empty()) return false;
    for(size_t i = 1; i < r.progress.size(); ++i) if(r.progress[i] < r.progress[i-1]) return false;
    return r.progress.back() == 1.0f;
}

static size_t countLines(const std::string& text, const std::string& prefix){
    size_t n = 0, pos = 0;
    while((pos = text.find(prefix, pos)) != std::string::npos){ if(pos == 0 || text[pos-1] == '\n') ++n; pos += prefix.size(); }
    return n;
}

struct Case { std::string name, reader; fs::path dir; std::vector<std::string> patterns; std::string processBlock; };

// --- metadata (A-10) ---

// The modification time of p plus aSeconds as "YYYY-MM-DD HH:MM:SS", local time (strftime, not the
// converter's own calendar code).
static std::string localStamp(const fs::path& p, int aSeconds){
    struct stat st;
    if(stat(native(p).c_str(), &st) != 0) return "<no mtime>";
    const time_t t = st.st_mtime + aSeconds;
    const struct tm* lt = std::localtime(&t);
    char b[64];
    if(!lt || !std::strftime(b, sizeof b, "%Y-%m-%d %H:%M:%S", lt)) return "<bad mtime>";
    return b;
}

static ImsExpectedMeta expectedMeta(uint64_t X, uint64_t Y, uint64_t Z){
    ImsExpectedMeta e;
    e.size[0] = X; e.size[1] = Y; e.size[2] = Z;
    e.version = PIW_VERSION;
    return e;
}

static void checkMeta(const fs::path& ims, const ImsExpectedMeta& e, const std::string& what){
    std::vector<std::string> problems;
    const bool ok = imsCheckDataSetInfo(ims, e, problems);
    for(size_t i = 0; i < problems.size() && i < 20; ++i) std::printf("    %s\n", problems[i].c_str());
    check(ok, what + ": every DataSetInfo attribute as expected");
}

static void metadataTests(const std::string& exe, const fs::path& work, const fs::path& mdir){
    std::printf("[metadata: imsmeta.h helpers]\n");
    {
        unsigned jd = 0; unsigned long long ns = 0; int64_t ms = 0;
        check(imsmeta::parseTime("2020-02-05 15:27:04", ms), "parseTime of a plain time");
        imsmeta::julianTime(ms, jd, ns);
        check(jd == 2458885u && ns == 55624ull * 1000000000ull, "julianTime: 2020-02-05 15:27:04 is Julian day 2458885, 55624 s");
        check(imsmeta::timeString(ms) == "2020-02-05 15:27:04", "timeString without milliseconds");
        const char* texts[] = {"2024-02-29 23:59:59.999", "1999-12-31 00:00:00.001", "2026-11-12 08:30:15.250", "1970-01-01 00:00:00", "1969-12-31 23:59:59.500"};
        for(const char* t : texts) check(imsmeta::parseTime(t, ms) && imsmeta::timeString(ms) == t, std::string("parseTime/timeString round trip: ") + t);
        check(imsmeta::parseTime("2026-03-04T05:06", ms) && imsmeta::timeString(ms) == "2026-03-04 05:06:00", "a 'T' separator and no seconds");
        check(imsmeta::parseTime("2026-03-04", ms) && imsmeta::timeString(ms) == "2026-03-04 00:00:00", "a date alone");
        check(imsmeta::parseTime("2026-03-04 05:06:07.1239", ms) && imsmeta::timeString(ms) == "2026-03-04 05:06:07.123", "a fraction beyond milliseconds");
        for(const char* bad : {"2026-02-30 00:00:00", "2025-02-29", "2026-13-01", "2026-01-01 24:00:00", "26-01-01", "2026-01-01 1:00", "2026-01-01 10:00:00.", "2026-01-01x", ""})
            check(!imsmeta::parseTime(bad, ms), std::string("parseTime refuses \"") + bad + "\"");
        int64_t st = 0;
        check(imsmeta::msecAbsStamp("/d/Scan_Iter_0001_CamA_ch0_0000123msec_0012345678msecAbs.zarr", st) && st == 12345678, "msecAbs stamp of a PetaKit5D name");
        check(imsmeta::msecAbsStamp("d\\x_0000042msecAbs_decon.tif", st) && st == 42, "msecAbs stamp with a Windows separator and a suffix");
        check(imsmeta::msecAbsStamp("/d/x_7msecAbs.zarr/", st) && st == 7, "msecAbs stamp of a folder with a trailing separator");
        check(!imsmeta::msecAbsStamp("/d/123msecAbs/CamA_0000123msec.tif", st), "no msecAbs stamp (one in a folder name does not count)");
        check(!imsmeta::msecAbsStamp("/d/CamA_msecAbs.tif", st), "no msecAbs stamp without digits");
        piw::Color c;
        check(imsmeta::parseColor("cyan", c) && c.red == 0 && c.green == 1 && c.blue == 1, "color by name");
        check(imsmeta::parseColor("FF8000", c) && c.red == 1 && c.green == 128.f / 255.f && c.blue == 0, "color as RRGGBB");
        check(imsmeta::parseColor("#0000ff", c) && c.blue == 1 && c.red == 0, "color as #rrggbb");
        check(imsmeta::parseColor("0.25:0.5:0.75", c) && c.red == 0.25f && c.green == 0.5f && c.blue == 0.75f, "color as r:g:b");
        check(!imsmeta::parseColor("purple", c) && !imsmeta::parseColor("FF80", c) && !imsmeta::parseColor("1:2", c), "bad colors refused");
        check(imsmeta::wavelengthString(488.f) == "488" && imsmeta::wavelengthString(561.5f) == "561.5", "wavelength texts");
    }

    // 2 channels x 2 time points (the inputs of the multi cases), 149 x 129 x 101
    const auto files = findFiles(mdir, {"CamA", "CamB"});
    std::printf("[metadata: defaults]\n");
    {
        const fs::path out = work/"meta_default.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.ok(), "convert with the default metadata (" + s.message + ")");
        ImsExpectedMeta e = expectedMeta(149, 129, 101);
        e.names = {"Channel 0", "Channel 1"};
        e.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
        e.times = {localStamp(files[0][0], 0), localStamp(files[0][0], 1)};
        std::printf("  first input's modification time: %s\n", e.times[0].c_str());
        checkMeta(out, e, "defaults");
    }
    std::printf("[metadata: options]\n");
    piw::ConvertOptions opt = optionsFor("zarr", files, work/"meta_options.ims", "");
    opt.channelNames = {"GFP", "mCherry 2"};
    opt.channelColors = {{0.f, 1.f, 1.f}, {0.25f, 0.5f, 0.75f}};
    opt.emissionWavelengths = {525.f, 0.f};
    opt.excitationWavelengths = {488.f, 561.5f};
    opt.startTime = "2026-01-31 23:59:58.250";
    opt.timeInterval = 2.5;
    ImsExpectedMeta eo = expectedMeta(149, 129, 101);
    eo.names = {"GFP", "mCherry 2"};
    eo.colors = {"0.000 1.000 1.000", "0.250 0.500 0.750"};
    eo.emission = {"525", ""};
    eo.excitation = {"488", "561.5"};
    eo.times = {"2026-01-31 23:59:58.250", "2026-02-01 00:00:00.750"};
    {
        Recorder rec;
        const piw::Status s = piw::convert(opt, callbacksFor(rec));
        check(s.ok(), "convert with names, colors, wavelengths, start time and interval (" + s.message + ")");
        checkMeta(opt.outputFile, eo, "options");
    }
    std::printf("[metadata: the same options on the command line]\n");
    {
        const fs::path od = work/"meta_cli"; fs::create_directories(od);
        const std::string args = "-P CamA,CamB -F \"" + native(mdir) + "\" -o \"" + native(od) + "\" -r zarr -v 1,1,1"
            " --channel-names \"GFP,mCherry 2\" --colors cyan,0.25:0.5:0.75 --emission 525,0 --excitation=488,561.5"
            " --start-time \"2026-01-31 23:59:58.250\" --time-interval 2.5";
        const int rc = runExe(exe, args, work/"meta_cli.log");
        check(rc == 0, "executable with the metadata flags (exit " + std::to_string(rc) + ")");
        checkMeta(od/"output.ims", eo, "command line");
        ImsCompareReport report;
        ImsCompareOptions co; co.thumbnail = true;
        const bool same = imsCompare(opt.outputFile, od/"output.ims", report, co);
        imsPrintReport(report);
        check(same, "command line and piw::convert write the same file (data and every attribute)");
    }
    std::printf("[metadata: msecAbs stamps]\n");
    {
        // PetaKit5D-style names; the second channel's stamps are used, the first channel has none
        const fs::path sdir = work/"stamps"; fs::create_directories(sdir);
        const char* a[] = {"CamA_t0000.zarr", "CamA_t0001.zarr", "CamA_t0002.zarr"};
        const char* b[] = {"Scan_Iter_0000_CamB_ch0_0000000msec_0086399000msecAbs.zarr",
                           "Scan_Iter_0001_CamB_ch0_0001250msec_0086400250msecAbs.zarr",
                           "Scan_Iter_0002_CamB_ch0_0061000msec_0086460000msecAbs.zarr"};
        for(int t = 0; t < 3; ++t){ writeZarr(sdir/a[t], 40, 30, 20, t); writeZarr(sdir/b[t], 40, 30, 20, 10 + t); }
        const auto sf = findFiles(sdir, {"CamA", "CamB"});
        piw::ConvertOptions o = optionsFor("zarr", sf, work/"meta_stamps.ims", "", {"CamA", "CamB"});
        o.startTime = "2026-11-12 23:59:59";
        Recorder rec;
        piw::Status s = piw::convert(o, callbacksFor(rec));
        check(s.ok(), "convert with stamps and a start time (" + s.message + ")");
        ImsExpectedMeta e = expectedMeta(40, 30, 20);
        e.names = {"CamA", "CamB"};
        e.colors = {"1.000 0.000 0.000", "0.000 1.000 0.000"};
        e.times = {"2026-11-12 23:59:59", "2026-11-13 00:00:00.250", "2026-11-13 00:01:00"};
        checkMeta(o.outputFile, e, "msecAbs stamps from the start time");
        // without a start time: from the first input's modification time; an interval overrides the stamps
        o.startTime.clear();
        o.outputFile = native(work/"meta_stamps_mtime.ims");
        s = piw::convert(o, callbacksFor(rec));
        e.times = {localStamp(sf[0][0], 0), localStamp(sf[0][0], 1) + ".250", localStamp(sf[0][0], 61)};
        check(s.ok(), "convert with stamps, no start time (" + s.message + ")");
        checkMeta(o.outputFile, e, "msecAbs stamps from the modification time");
        o.timeInterval = 0.5;
        o.outputFile = native(work/"meta_stamps_interval.ims");
        s = piw::convert(o, callbacksFor(rec));
        e.times = {localStamp(sf[0][0], 0), localStamp(sf[0][0], 0) + ".500", localStamp(sf[0][0], 1)};
        check(s.ok(), "convert with stamps and an interval (" + s.message + ")");
        checkMeta(o.outputFile, e, "an interval overrides the stamps");
    }
    std::printf("[metadata: bad options]\n");
    {
        struct Bad { std::string what; piw::ConvertOptions o; };
        std::vector<Bad> bads;
        piw::ConvertOptions base = optionsFor("zarr", files, work/"meta_bad.ims", "");
        Bad k{"three names for two channels", base}; k.o.channelNames = {"a", "b", "c"}; bads.push_back(k);
        k = Bad{"an empty name", base}; k.o.channelNames = {"a", ""}; bads.push_back(k);
        k = Bad{"a color above 1", base}; k.o.channelColors = {{1.f, 0.f, 0.f}, {0.f, 1.5f, 0.f}}; bads.push_back(k);
        k = Bad{"one wavelength for two channels", base}; k.o.emissionWavelengths = {500.f}; bads.push_back(k);
        k = Bad{"a bad start time", base}; k.o.startTime = "2026-02-30 10:00:00"; bads.push_back(k);
        k = Bad{"a negative interval", base}; k.o.timeInterval = -1; bads.push_back(k);
        for(const Bad& b : bads){
            Recorder rec;
            const piw::Status s = piw::convert(b.o, callbacksFor(rec));
            check(s.code == piw::Code::InvalidArgument && !fs::exists(work/"meta_bad.ims") && rec.text.empty(),
                  b.what + ": InvalidArgument before any output (" + s.message + ")");
        }
        const fs::path od = work/"meta_cli_bad"; fs::create_directories(od);
        const std::string common = "-P CamA,CamB -F \"" + native(mdir) + "\" -o \"" + native(od) + "\" -r zarr ";
        for(const char* flags : {"--colors red,purple", "--start-time yesterday", "--time-interval 0", "--channel-names A", "--emission 500,x"}){
            const int rc = runExe(exe, common + flags, work/"meta_cli_bad.log");
            check(rc == 1 && !fs::exists(od/"output.ims"), std::string(flags) + ": exit 1, no output (exit " + std::to_string(rc) + ", " +
                  readText(work/"meta_cli_bad.log").substr(0, 160) + ")");
        }
    }
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = argv[1];
    fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_apitest";
    fs::remove_all(work); fs::create_directories(work);
    std::printf("piw %s, default threads %d\n", piw::version(), piw::defaultThreads());
    check(std::strlen(piw::version()) > 0 && piw::defaultThreads() >= 1, "version() and defaultThreads()");

    // Inputs: none of the sizes is a multiple of the chunk sizes.
    const fs::path tdir = work/"tiff", zdir = work/"zarr", mdir = work/"multi";
    fs::create_directories(tdir); fs::create_directories(zdir); fs::create_directories(mdir);
    writeTiff(tdir/"CamA_t0000.tif", 100, 70, 200, 0);
    writeZarr(zdir/"CamA_t0000.zarr", 100, 70, 200, 0);
    for(int c = 0; c < 2; ++c)
        for(int t = 0; t < 2; ++t)
            writeZarr(mdir/(std::string(c ? "CamB" : "CamA") + "_t000" + std::to_string(t) + ".zarr"), 149, 129, 101, 2*c + t);

    // --- 1. piw::convert writes what the executable writes ---
    const std::vector<Case> cases = {
        {"tiff_whole", "tiff", tdir, {"CamA"}, ""},
        {"tiff_C",     "tiff", tdir, {"CamA"}, "200,200,78"},     // Z-slab blocking
        {"zarr_whole", "zarr", zdir, {"CamA"}, ""},
        {"zarr_C",     "zarr", zdir, {"CamA"}, "32,32,32"},       // 3-D blocking
        {"multi_whole","zarr", mdir, {"CamA","CamB"}, ""},        // 2 channels x 2 time points
        {"multi_C",    "zarr", mdir, {"CamA","CamB"}, "32,32,32"},
    };
    auto exeIms = [&](const Case& k){ return work/("exe_" + k.name)/"output.ims"; };
    for(const Case& k : cases){
        std::printf("[%s] executable vs piw::convert\n", k.name.c_str());
        const fs::path od = work/("exe_" + k.name); fs::create_directories(od);
        std::string pats = k.patterns[0]; for(size_t i = 1; i < k.patterns.size(); ++i) pats += "," + k.patterns[i];
        std::string args = "-P " + pats + " -F \"" + native(k.dir) + "\" -o \"" + native(od) + "\" -r " + k.reader + " -v 1,1,1";
        if(!k.processBlock.empty()) args += " -C " + k.processBlock;
        const int rc = runExe(exe, args, work/("exe_" + k.name + ".log"));
        check(rc == 0 && fs::exists(exeIms(k)), "executable exit 0 and output (rc " + std::to_string(rc) + ")");

        Recorder rec;
        const auto files = findFiles(k.dir, k.patterns);
        const fs::path out = work/("api_" + k.name + ".ims");
        const piw::Status s = piw::convert(optionsFor(k.reader, files, out, k.processBlock, k.patterns), callbacksFor(rec));
        check(s.ok(), "piw::convert ok (" + s.message + ")");
        check(sameIms(exeIms(k), out), "same data chunks and channel info as the executable");
        check(progressOk(rec), "progress never decreases and ends at 1 (" + std::to_string(rec.progress.size()) + " calls)");
        size_t nfiles = 0; for(const auto& ch : files) nfiles += ch.size();
        check(countLines(rec.text, "Current File: ") == nfiles && countLines(rec.text, "Voxel Sizes: 1.000000,1.000000,1.000000") == 1,
              "messages: one Current File line per input, the voxel sizes");
    }
    const Case& zarrWhole = cases[2];
    const Case& tiffWhole = cases[0];
    const Case& zarrC = cases[3];

    // --- 2. errors come back as a Status, without output ---
    std::printf("[errors]\n");
    {
        // a .zarr folder without metadata
        const fs::path bad = work/"bad"; fs::create_directories(bad/"CamA_t0000.zarr");
        const fs::path out = work/"api_bad.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(bad, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::ReadFailed && !fs::exists(out), "zarr without metadata: ReadFailed, no output (" + s.message + ")");
    }
    const fs::path trunc = work/"trunc";
    {
        // a truncated chunk: fails in the first read, after the writer was opened
        fs::create_directories(trunc);
        fs::copy(zdir/"CamA_t0000.zarr", trunc/"CamA_t0000.zarr", fs::copy_options::recursive);
        std::vector<fs::path> chunks;
        for(const auto& e : fs::directory_iterator(trunc/"CamA_t0000.zarr"))
            if(e.is_regular_file() && e.path().filename().string()[0] != '.') chunks.push_back(e.path());
        std::sort(chunks.begin(), chunks.end());
        fs::resize_file(chunks.front(), 10);
        const fs::path out = work/"api_trunc.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(trunc, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::ReadFailed && s.message.rfind("could not read ", 0) == 0 && !fs::exists(out),
              "truncated chunk: ReadFailed, output removed (" + s.message + ")");
    }
    const fs::path sizes = work/"sizes";
    {
        // inputs of different sizes are refused before any output: an existing file stays as it is
        fs::create_directories(sizes);
        writeZarr(sizes/"CamA_t0000.zarr", 100, 70, 200, 0);
        writeZarr(sizes/"CamA_t0001.zarr", 100, 70, 100, 1);
        const fs::path out = work/"api_sizes.ims";
        { std::ofstream(out) << "keep"; }
        Recorder rec;
        const auto files = findFiles(sizes, {"CamA"});
        piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument && readText(out) == "keep" && rec.text.empty(),
              "different sizes: InvalidArgument before any output (" + s.message + ")");
        // ... unless a bounding box fits all of them
        piw::ConvertOptions o = optionsFor("zarr", files, work/"api_sizes_box.ims", "");
        o.hasBoundingBox = true;
        const uint64_t box[6] = {10, 20, 30, 60, 50, 90};
        for(int i = 0; i < 6; ++i) o.boundingBox[i] = box[i];
        s = piw::convert(o, callbacksFor(rec));
        check(s.ok() && fs::exists(work/"api_sizes_box.ims"), "different sizes with a bounding box inside both: ok (" + s.message + ")");
        o.boundingBox[5] = 150;   // beyond the second file
        s = piw::convert(o, callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument, "bounding box outside one input: InvalidArgument (" + s.message + ")");
    }
    {
        // channels with different numbers of files
        auto files = findFiles(mdir, {"CamA", "CamB"});
        files[1].pop_back();
        const fs::path out = work/"api_counts.ims";
        Recorder rec;
        const piw::Status s = piw::convert(optionsFor("zarr", files, out, ""), callbacksFor(rec));
        check(s.code == piw::Code::InvalidArgument && !fs::exists(out), "channels with 2 and 1 files: InvalidArgument (" + s.message + ")");
    }
    {
        // a pattern without a match, and the selection's order (by full path, folders in order)
        std::vector<std::vector<std::string>> files;
        piw::Status s = piw::findInputFiles({native(zdir)}, {"CamZ"}, files);
        check(s.code == piw::Code::NotFound && s.message == "Given pattern \"CamZ\" was not found in any of the provided folders.",
              "pattern without a match: NotFound (" + s.message + ")");
        const fs::path o1 = work/"order", o2 = work/"order2";
        fs::create_directories(o1); fs::create_directories(o2);
        for(const char* n : {"CamA_t0003.tif", "CamA_t0001.tif", "CamA_t0010.tif", "CamA_t0002.tif", "CamA_t0004.txt", "CamB_t0000.tif"})
            std::ofstream(o1/n) << "x";
        std::ofstream(o2/"CamA_t0000.tif") << "x";
        s = piw::findInputFiles({native(o2), native(o1)}, {"CamA"}, files);
        const std::vector<std::string> want = {native(o1/"CamA_t0001.tif"), native(o1/"CamA_t0002.tif"), native(o1/"CamA_t0003.tif"),
                                               native(o1/"CamA_t0010.tif"), native(o2/"CamA_t0000.tif")};
        check(s.ok() && files.size() == 1 && files[0] == want, "two folders: .tif/.zarr files containing the pattern, sorted by path");
    }

    // --- 3. cancel ---
    std::printf("[cancel]\n");
    for(int poll : {1, 3}){
        Recorder rec; rec.cancelOnPoll = poll;
        const fs::path out = work/("api_cancel" + std::to_string(poll) + ".ims");
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(zdir, {"CamA"}), out, ""), callbacksFor(rec));
        check(s.code == piw::Code::Cancelled && rec.polls == poll && !fs::exists(out),
              "cancel on poll " + std::to_string(poll) + ": Cancelled, output removed");
    }

    // --- 4. calls in sequence, at once, with numThreads; runCli twice ---
    std::printf("[calls]\n");
    for(int i = 0; i < 2; ++i){
        Recorder rec;
        const fs::path out = work/("api_seq" + std::to_string(i) + ".ims");
        const piw::Status s = piw::convert(optionsFor("zarr", findFiles(zdir, {"CamA"}), out, "", {"CamA"}), callbacksFor(rec));
        check(s.ok() && sameIms(exeIms(zarrWhole), out), "conversion " + std::to_string(i + 1) + " in sequence");
    }
    {
        piw::Status s1, s2;
        Recorder r1, r2;
        const auto f1 = findFiles(tdir, {"CamA"}), f2 = findFiles(zdir, {"CamA"});
        const fs::path out1 = work/"api_thread1.ims", out2 = work/"api_thread2.ims";
        std::thread t1([&]{ s1 = piw::convert(optionsFor("tiff", f1, out1, "", {"CamA"}), callbacksFor(r1)); });
        std::thread t2([&]{ s2 = piw::convert(optionsFor("zarr", f2, out2, "32,32,32", {"CamA"}), callbacksFor(r2)); });
        t1.join(); t2.join();
        check(s1.ok() && s2.ok() && sameIms(exeIms(tiffWhole), out1) && sameIms(exeIms(zarrC), out2),
              "two conversions at once from two threads");
    }
    {
        const int before = omp_get_max_threads();
        Recorder rec;
        piw::ConvertOptions o = optionsFor("zarr", findFiles(zdir, {"CamA"}), work/"api_threads2.ims", "", {"CamA"});
        o.numThreads = 2;
        const piw::Status s = piw::convert(o, callbacksFor(rec));
        check(s.ok() && sameIms(exeIms(zarrWhole), work/"api_threads2.ims") && omp_get_max_threads() == before,
              "numThreads 2: same file, the caller's OpenMP thread count unchanged");
    }
    for(int i = 0; i < 2; ++i){
        const fs::path od = work/("runcli" + std::to_string(i));
        fs::create_directories(od);
        std::vector<std::string> args = {"parallelimariswriter", "-P", "CamA", "-F", native(zdir), "-o", native(od),
                                         "-r", "zarr", "-v", "1,1,1"};
        std::vector<char*> argvv;
        for(auto& a : args) argvv.push_back(&a[0]);
        argvv.push_back(nullptr);
        const int rc = piw::runCli((int)args.size(), argvv.data());
        check(rc == 0 && sameIms(exeIms(zarrWhole), od/"output.ims"), "piw::runCli call " + std::to_string(i + 1) + " in this process");
    }

    // --- 5. the command line exits 1 (not a crash) where it used to crash ---
    std::printf("[command line]\n");
    {
        const fs::path od = work/"cli_out"; fs::create_directories(od);
        const std::string o = " -o \"" + native(od) + "\"";
        struct CliCase { std::string what, args; };
        const std::vector<CliCase> cliCases = {
            {"no arguments", ""},
            {"no -F", "-P CamA -r zarr" + o},
            {"no -P", "-F \"" + native(zdir) + "\" -r zarr" + o},
            {"unreadable -v", "-P CamA -F \"" + native(zdir) + "\" -r zarr -v 1" + o},
            {"zarr without metadata", "-P CamA -F \"" + native(work/"bad") + "\" -r zarr" + o},
            {"truncated chunk", "-P CamA -F \"" + native(trunc) + "\" -r zarr" + o},
            {"different sizes", "-P CamA -F \"" + native(sizes) + "\" -r zarr" + o},
        };
        int i = 0;
        for(const CliCase& k : cliCases){
            const fs::path log = work/("cli_" + std::to_string(i++) + ".log");
            const int rc = runExe(exe, k.args, log);
            check(rc == 1 && !fs::exists(od/"output.ims"), k.what + ": exit 1, no output (exit " + std::to_string(rc) + ", " +
                  readText(log).substr(0, 160) + ")");
        }
    }

    metadataTests(exe, work, mdir);

    if(gFailures == 0){ std::printf("apitest PASSED\n"); return 0; }
    std::printf("apitest FAILED (%d checks)\n", gFailures);
    return 1;
}
