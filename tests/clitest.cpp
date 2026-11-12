// Command-line test: the converter's options, input discovery and exit codes.
//
// Runs the shipped executable on small generated inputs. Every error case must exit with code 1
// (never 0, never a signal) after one "ERROR: <text>" line on stderr that contains the expected
// text, and leave no output file; every success case must exit 0 with an output whose voxels equal
// the inputs, in the expected time-point order. Covered:
//   - usage: no arguments (usage, exit 1), -h and --help (exit 0), unknown short and long options,
//     missing option values, a stray argument
//   - values: -v, -b, -B, -C, -c, -t out of range or malformed; one trailing comma accepted
//     (-v a,b,c, -b 64,64,64, -B ...,)
//   - discovery: no -F or -P, unknown reader, pattern not found, missing folder, -c / -t that do not
//     match, channels with 3 and 2 files (both orders), a file that matches two patterns, a folder
//     given twice, .tiff and .TIF, a folder holding X.tif and X.zarr (only the reader's files), two
//     folders (2 + 1 files), 12 time points created in shuffled order (ordered by name), '%' in a
//     file and in a folder name, a space in a path, 100 channels
//   - output: a folder nested three levels deep (created), a folder that cannot be created, a
//     timepoints.txt that cannot be written (a warning only)
//   - inputs that used to crash: bad zarr metadata, a corrupt chunk, a TIFF that is not a TIFF
//   - sample types: int16 (Zarr, TIFF), float64 (Zarr, TIFF), an RGB TIFF, a bool Zarr, a 2-D and a
//     4-D Zarr are refused with the type in the message; a second time point of another type with
//     the same bit depth (uint32 after float32, Zarr and TIFF) or an int16 one is refused before
//     any output
//   - bounding boxes: max == min, outside along Z only, outside a TIFF, inside the first time point
//     but not the second (the message names that file)
//   - POSIX only: "~/" with HOME set for the child, an unreadable input folder and an unwritable
//     output folder (skipped when running as root)
// C stdio for file I/O (no iostreams, no std::filesystem copies: see failtest.cpp). The executable
// is started with posix_spawn, or _spawnv on Windows (no shell, so no quoting of '%' or spaces).
//   usage: cliTest <path-to-parallelimariswriter> [workdir]
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

#include "helperfunctions.h"     // cpp-tiff's: its folder comes first
#include "parallelwritetiff.h"   // cpp-tiff
#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims decoding (HDF5 + lz4)

namespace fs = std::filesystem;

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    std::printf("  %s: %s\n", ok ? "ok" : "FAIL", what.c_str());
    std::fflush(stdout);
    if(!ok) ++gFailures;
}

static std::string native(const fs::path& p){ return fs::path(p).make_preferred().string(); }

// --- inputs ---

struct Dims { uint64_t x, y, z; };
static const Dims kSmall = {40, 30, 20};

static std::vector<uint16_t> makeVolume(const Dims& d, int seed){
    std::vector<uint16_t> v(d.x*d.y*d.z);
    for(uint64_t z = 0; z < d.z; z++)
        for(uint64_t y = 0; y < d.y; y++)
            for(uint64_t x = 0; x < d.x; x++)
                v[x + y*d.x + z*d.x*d.y] = (uint16_t)((x*7 + y*13 + z*17 + seed*137) % 4096 + 1);
    return v;
}

// A Zarr of v (dims 0, 1, 2 = X, Y, Z, F order), 16^3 chunks, zstd.
static void writeZarrData(const fs::path& path, const Dims& d, const void* data, const char* dtype, uint64_t bits){
    fs::create_directories(path.parent_path());
    std::vector<uint64_t> shape = {d.x, d.y, d.z}, chunks = {16, 16, 16};
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(1); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype(dtype); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0, 0, 0}, shape);
    if(parallelWriteZarr(Zw, const_cast<void*>(data), {0, 0, 0}, shape, shape, bits, false, false, false)){
        std::fprintf(stderr, "Zarr write of %s failed: %s\n", path.string().c_str(), Zw.get_errString().c_str());
        std::exit(1);
    }
}
// A Zarr of zeros of any shape (2-D, 4-D, ...), 16 per chunk along every dimension.
static void writeZarrShape(const fs::path& path, const std::vector<uint64_t>& shape, const char* dtype, uint64_t bits){
    fs::create_directories(path.parent_path());
    uint64_t n = 1;
    for(uint64_t e : shape) n *= e;
    std::vector<uint8_t> zeros(n*bits/8);
    zarr Zw;
    Zw.set_fileName(path.string());
    Zw.set_cname("zstd"); Zw.set_clevel(1); Zw.set_order("F");
    Zw.set_chunks(std::vector<uint64_t>(shape.size(), 16)); Zw.set_dimension_separator("."); Zw.set_dtype(dtype); Zw.set_shape(shape);
    Zw.write_zarray();
    const std::vector<uint64_t> origin(shape.size(), 0);
    Zw.set_chunkInfo(origin, shape);
    if(parallelWriteZarr(Zw, zeros.data(), origin, shape, shape, bits, false, false, false)){
        std::fprintf(stderr, "Zarr write of %s failed: %s\n", path.string().c_str(), Zw.get_errString().c_str());
        std::exit(1);
    }
}
static void writeZarr(const fs::path& path, const Dims& d, int seed){
    const std::vector<uint16_t> v = makeVolume(d, seed);
    writeZarrData(path, d, v.data(), "<u2", 16);
}

// A TIFF of data: bits per sample, SampleFormat (1 unsigned, 2 signed, 3 float), samples per pixel.
static void writeTiffData(const fs::path& path, const Dims& d, const void* data, uint64_t bits, uint16_t sampleFormat,
                          uint64_t samples = 1){
    fs::create_directories(path.parent_path());
    // The TIFF reader returns (rows, columns, pages) with the row fastest, so the volume's first
    // axis is the TIFF height: a column-major buffer of width d.y and height d.x (see roundtrip.cpp).
    if(writeTiffParallelWrapper(d.y, d.x, d.z, path.string().c_str(), data, bits, 0, 8, "w", true, std::string("none"),
                                sampleFormat, 0, samples)){
        std::fprintf(stderr, "TIFF write of %s failed\n", path.string().c_str());
        std::exit(1);
    }
}
static void writeTiff(const fs::path& path, const Dims& d, int seed){
    const std::vector<uint16_t> v = makeVolume(d, seed);
    writeTiffData(path, d, v.data(), 16, 1);
}

static void writeText(const fs::path& p, const std::string& text){
    fs::create_directories(p.parent_path());
    FILE* f = std::fopen(p.string().c_str(), "wb");
    if(!f){ std::fprintf(stderr, "cannot write %s\n", p.string().c_str()); std::exit(1); }
    std::fwrite(text.data(), 1, text.size(), f);
    std::fclose(f);
}

static std::string readText(const fs::path& p){
    std::string b;
    FILE* f = std::fopen(p.string().c_str(), "rb");
    if(!f) return b;
    char buf[1 << 16]; size_t n;
    while((n = std::fread(buf, 1, sizeof buf, f)) > 0) b.append(buf, n);
    std::fclose(f);
    return b;
}

// --- running the executable ---

struct RunResult {
    int exitCode = -1;      // the exit code; -1 if it did not exit normally
    int signal = 0;         // POSIX: the signal that ended it
    std::string out, err;   // stdout, stderr
};

static std::string gExe;
static fs::path gLogs;
static int gRun = 0;

#ifdef _WIN32
// One argument as the Microsoft C runtime splits a command line.
static std::string quoteArg(const std::string& a){
    if(!a.empty() && a.find_first_of(" \t\"") == std::string::npos) return a;
    std::string r = "\"";
    size_t backslashes = 0;
    for(char c : a){
        if(c == '\\'){ ++backslashes; continue; }
        if(c == '"'){ r.append(backslashes*2 + 1, '\\'); r += '"'; backslashes = 0; continue; }
        r.append(backslashes, '\\'); backslashes = 0; r += c;
    }
    r.append(backslashes*2, '\\');
    return r + "\"";
}
#endif

// Runs the executable with args (stdout and stderr to their own log files). home: HOME for the
// child (POSIX only; empty: unchanged).
static RunResult run(const std::vector<std::string>& args, const std::string& home = std::string()){
    const int n = gRun++;
    const fs::path outLog = gLogs/("run" + std::to_string(n) + ".out"), errLog = gLogs/("run" + std::to_string(n) + ".err");
    std::string cmd = "$";
    for(const std::string& a : args) cmd += " '" + a + "'";
    std::printf("  [run %d]%s %s\n", n, home.empty() ? "" : (" HOME=" + home).c_str(), cmd.c_str());
    std::fflush(stdout);
    RunResult r;
#ifdef _WIN32
    (void)home;
    std::vector<std::string> store = {quoteArg(gExe)};
    for(const std::string& a : args) store.push_back(quoteArg(a));
    std::vector<const char*> argv;
    for(const std::string& a : store) argv.push_back(a.c_str());
    argv.push_back(nullptr);
    std::fflush(stderr);
    const int savedOut = _dup(1), savedErr = _dup(2);
    const int fo = _open(outLog.string().c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
    const int fe = _open(errLog.string().c_str(), _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY, _S_IREAD | _S_IWRITE);
    if(fo >= 0) _dup2(fo, 1);
    if(fe >= 0) _dup2(fe, 2);
    const intptr_t rc = _spawnv(_P_WAIT, gExe.c_str(), argv.data());
    _dup2(savedOut, 1); _dup2(savedErr, 2);
    _close(savedOut); _close(savedErr);
    if(fo >= 0) _close(fo);
    if(fe >= 0) _close(fe);
    r.exitCode = (int)rc;
#else
    std::vector<std::string> store = {gExe};
    store.insert(store.end(), args.begin(), args.end());
    std::vector<char*> argv;
    for(std::string& a : store) argv.push_back(&a[0]);
    argv.push_back(nullptr);
    std::vector<std::string> envStore;
    for(char** e = environ; *e; ++e) if(home.empty() || std::strncmp(*e, "HOME=", 5) != 0) envStore.push_back(*e);
    if(!home.empty()) envStore.push_back("HOME=" + home);
    std::vector<char*> envp;
    for(std::string& e : envStore) envp.push_back(&e[0]);
    envp.push_back(nullptr);
    const std::string outName = outLog.string(), errName = errLog.string();
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 1, outName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_addopen(&fa, 2, errName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    pid_t pid = 0;
    const int e = posix_spawn(&pid, gExe.c_str(), &fa, nullptr, argv.data(), envp.data());
    posix_spawn_file_actions_destroy(&fa);
    if(e != 0){ check(false, std::string("posix_spawn failed: ") + std::strerror(e)); return r; }
    int status = 0;
    while(waitpid(pid, &status, 0) < 0 && errno == EINTR){}
    if(WIFEXITED(status)) r.exitCode = WEXITSTATUS(status);
    else if(WIFSIGNALED(status)) r.signal = WTERMSIG(status);
#endif
    r.out = readText(outLog);
    r.err = readText(errLog);
    return r;
}

static std::string firstLines(const std::string& text){
    std::string t = text.substr(0, 400);
    std::replace(t.begin(), t.end(), '\n', '|');
    return t;
}

static size_t countErrorLines(const std::string& err){
    size_t n = 0;
    for(size_t b = 0; b < err.size();){
        size_t e = err.find('\n', b);
        if(e == std::string::npos) e = err.size();
        if(err.compare(b, 7, "ERROR: ") == 0) ++n;
        b = e + 1;
    }
    return n;
}

// An error case: exit code 1, one ERROR line containing want, and (with noFile) no such file.
static void expectError(const std::string& what, const std::vector<std::string>& args, const std::string& want,
                        const fs::path& noFile = fs::path()){
    const RunResult r = run(args);
    const bool ok = r.exitCode == 1 && r.signal == 0 && countErrorLines(r.err) == 1 &&
                    r.err.find("ERROR: ") != std::string::npos && r.err.find(want) != std::string::npos &&
                    (noFile.empty() || !fs::exists(noFile));
    check(ok, what + ": exit " + std::to_string(r.exitCode) + (r.signal ? " signal " + std::to_string(r.signal) : "") +
              ", stderr \"" + firstLines(r.err) + "\" (want exit 1 and \"" + want + "\")");
}

// The voxels of every time point of channel c equal the volumes made from seeds.
static bool voxelsEqual(const fs::path& ims, int c, const Dims& d, const std::vector<int>& seeds){
    for(size_t t = 0; t < seeds.size(); ++t){
        ImsVolumeInfo info;
        const std::vector<uint8_t> got = imsReadVolume(ims, 0, (int)t, c, info);
        const std::vector<uint16_t> want = makeVolume(d, seeds[t]);
        if(got.size() != want.size()*2 || std::memcmp(got.data(), want.data(), got.size()) != 0){
            std::printf("    channel %d time point %zu differs from its input%s\n", c, t,
                        info.error.empty() ? "" : (" (" + info.error + ")").c_str());
            return false;
        }
    }
    ImsVolumeInfo info;
    if(!imsReadVolume(ims, 0, (int)seeds.size(), c, info).empty()){
        std::printf("    more than %zu time points\n", seeds.size());
        return false;
    }
    return true;
}

// A success case: exit code 0 and outDir/<name>.ims with channel 0's voxels from seeds.
static RunResult expectOk(const std::string& what, const std::vector<std::string>& args, const fs::path& ims,
                          const Dims& d, const std::vector<int>& seeds, const std::string& home = std::string()){
    const RunResult r = run(args, home);
    const bool ran = r.exitCode == 0 && r.signal == 0 && fs::exists(ims);
    check(ran && voxelsEqual(ims, 0, d, seeds), what + ": exit " + std::to_string(r.exitCode) +
          (r.signal ? " signal " + std::to_string(r.signal) : "") + (ran ? "" : ", stderr \"" + firstLines(r.err) + "\""));
    return r;
}

static bool sameData(const fs::path& a, const fs::path& b){
    ImsCompareOptions o;
    o.dataSetInfo = false;   // the names and times differ with the input names
    ImsCompareReport report;
    const bool same = imsCompare(a, b, report, o);
    if(!same){
        for(size_t i = 0; i < report.differences.size() && i < 5; ++i) std::printf("    %s\n", report.differences[i].c_str());
        if(!report.error.empty()) std::printf("    %s\n", report.error.c_str());
    }
    return same;
}

#ifndef _WIN32
static bool isRoot(){ return geteuid() == 0; }
#endif

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    gExe = fs::absolute(argv[1]).string();
    const fs::path work = fs::absolute((argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_clitest");
    fs::remove_all(work);
    gLogs = work/"logs";
    fs::create_directories(gLogs);
    const Dims d = kSmall;

    // Inputs
    const fs::path plain = work/"plain", out = work/"out";
    writeZarr(plain/"CamA_t0000.zarr", d, 0);
    fs::create_directories(out);
    const std::string P = native(plain);
    const fs::path outIms = out/"output.ims";
    auto conv = [&](std::vector<std::string> extra){
        std::vector<std::string> a = {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(out)};
        a.insert(a.end(), extra.begin(), extra.end());
        return a;
    };

    std::printf("[usage]\n");
    {
        const RunResult r = run({});
        check(r.exitCode == 1 && r.err.find("usage:") != std::string::npos && countErrorLines(r.err) == 1,
              "no arguments: usage on stderr, one ERROR line, exit 1 (exit " + std::to_string(r.exitCode) + ")");
        for(const char* h : {"-h", "--help"}){
            const RunResult rh = run({h});
            check(rh.exitCode == 0 && rh.out.find("usage:") != std::string::npos && rh.out.find("--channel-names") != std::string::npos &&
                  rh.err.empty(), std::string(h) + ": usage on stdout, exit 0 (exit " + std::to_string(rh.exitCode) + ")");
        }
        expectError("unknown option -Q", conv({"-Q"}), "Unknown option -Q", outIms);
        expectError("removed option -f", conv({"-f", "x"}), "Unknown option -f", outIms);
        expectError("unknown long option", conv({"--frobnicate", "1"}), "--frobnicate", outIms);
        expectError("--help with a value", conv({"--help=1"}), "--help=1", outIms);
        expectError("-P without a value", {"-r", "zarr", "-F", P, "-o", native(out), "-P"}, "Option -P needs a value", outIms);
        expectError("--colors without a value", conv({"--colors"}), "Option --colors needs a value", outIms);
        expectError("--channel-names=", conv({"--channel-names="}), "--channel-names", outIms);
        expectError("--time-interval x", conv({"--time-interval", "x"}), "--time-interval", outIms);
        expectError("a stray argument", conv({"stray"}), "Unexpected argument \"stray\"", outIms);
    }

    std::printf("[values]\n");
    {
        const std::vector<std::pair<std::vector<std::string>, std::string> > bad = {
            {{"-C", "64"}, "-C"}, {{"-C", "0,64,64"}, "-C"}, {{"-v", "1"}, "-v"}, {{"-v", "0,1,1"}, "-v"},
            {{"-v", "1,-1,1"}, "-v"}, {{"-v", "1,,1"}, "-v"}, {{"-v", "1,1,x"}, "-v"}, {{"-v", "1,1,1,,"}, "-v"},
            {{"-b", "1,2"}, "-b"}, {{"-b", "0,0,0"}, "-b"}, {{"-b", "64.5,64,64"}, "-b"},
            {{"-B", "1,2,3"}, "-B"}, {{"-B", "-1,0,0,5,5,5"}, "-B"}, {{"-B", "0,0,0,5,5,99999999999999999999999"}, "-B"},
            {{"-c", "0"}, "-c"}, {{"-c", "x"}, "-c"}, {{"-t", "-1"}, "-t"}, {{"-t", "1.5"}, "-t"},
        };
        for(const auto& k : bad){
            std::string what = k.first[0] + " " + k.first[1];
            expectError("malformed " + what, conv(k.first), "Could not read " + k.second, outIms);
        }
        expectError("-B that ends before it starts", conv({"-B", "10,0,0,5,5,5"}), "bounding box", outIms);
        expectError("-B larger than the input", conv({"-B", "0,0,0,41,30,20"}), "bounding box", outIms);
        // The MATLAB wrapper's trailing commas
        expectOk("-v 0.1,0.2,0.3, -b 64,64,64, -c 1 -t 1", conv({"-v", "0.1,0.2,0.3,", "-b", "64,64,64,", "-c", "1", "-t", "1"}),
                 outIms, d, {0});
        const std::optional<std::string> ext = imsReadAttribute(outIms, "/DataSetInfo/Image", "ExtMax2");
        check(ext && std::fabs(std::atof(ext->c_str()) - 20*0.3) < 1e-3, "-v 0.1,0.2,0.3,: ExtMax2 = 6 (" + ext.value_or("missing") + ")");
        fs::remove(outIms);
        const RunResult r = run(conv({"-B", "0,0,0,40,30,20,", "-C", "16,16,16,"}));
        check(r.exitCode == 0 && voxelsEqual(outIms, 0, d, {0}), "-B 0,0,0,40,30,20, and -C 16,16,16,: exit 0, same voxels");
        fs::remove(outIms);
    }

    std::printf("[discovery]\n");
    {
        expectError("no -F", {"-r", "zarr", "-P", "CamA", "-o", native(out)}, "No input folder", outIms);
        expectError("no -P", {"-r", "zarr", "-F", P, "-o", native(out)}, "No channel pattern", outIms);
        expectError("-r foo", {"-r", "foo", "-F", P, "-P", "CamA", "-o", native(out)}, "Reader type \"foo\"", outIms);
        expectError("pattern not found", {"-r", "zarr", "-F", P, "-P", "NoSuch", "-o", native(out)},
                    "Given pattern \"NoSuch\" was not found", outIms);
        expectError("missing folder", {"-r", "zarr", "-F", native(work/"does_not_exist"), "-P", "CamA", "-o", native(out)},
                    "does_not_exist\"", outIms);
        expectError("-c 2 with one pattern", conv({"-c", "2"}), "-c 2 was given", outIms);
        expectError("-t 3 with one file", conv({"-t", "3"}), "-t 3 was given", outIms);

        const fs::path u1 = work/"unequal1", u2 = work/"unequal2", o = work/"out_unequal";
        for(int t = 0; t < 3; ++t) writeZarr(u1/("CamA_t000" + std::to_string(t) + ".zarr"), d, t);
        for(int t = 0; t < 2; ++t) writeZarr(u1/("CamB_t000" + std::to_string(t) + ".zarr"), d, t);
        for(int t = 0; t < 2; ++t) writeZarr(u2/("CamA_t000" + std::to_string(t) + ".zarr"), d, t);
        for(int t = 0; t < 3; ++t) writeZarr(u2/("CamB_t000" + std::to_string(t) + ".zarr"), d, t);
        expectError("CamA 3 files, CamB 2", {"-r", "zarr", "-F", native(u1), "-P", "CamA,CamB", "-o", native(o)},
                    "different numbers of files: \"CamA\" 3, \"CamB\" 2", o/"output.ims");
        expectError("CamA 2 files, CamB 3", {"-r", "zarr", "-F", native(u2), "-P", "CamA,CamB", "-o", native(o)},
                    "different numbers of files: \"CamA\" 2, \"CamB\" 3", o/"output.ims");
        expectError("a file matching two patterns", {"-r", "zarr", "-F", native(u1), "-P", "Cam,CamA", "-o", native(o)},
                    "matches both pattern", o/"output.ims");
        expectError("a folder given twice", {"-r", "zarr", "-F", P + "," + P, "-P", "CamA", "-o", native(o)},
                    "was found twice", o/"output.ims");

        // .tiff and .TIF with -r tiff
        const fs::path te = work/"tiffext";
        writeTiff(te/"CamA_t0000.tiff", d, 0);
        writeTiff(te/"CamA_t0001.TIF", d, 1);
        expectOk(".tiff and .TIF", {"-r", "tiff", "-F", native(te), "-P", "CamA", "-o", native(work/"out_tiffext")},
                 work/"out_tiffext"/"output.ims", d, {0, 1});

        // X.tif and X.zarr in one folder: only the reader's files
        const fs::path mix = work/"mixed";
        writeZarr(mix/"CamA_t0000.zarr", d, 3);
        writeTiff(mix/"CamA_t0000.tif", d, 4);
        writeText(mix/"CamA_t0001.txt", "not an input");
        expectOk("X.tif + X.zarr, -r zarr", {"-r", "zarr", "-F", native(mix), "-P", "CamA", "-o", native(work/"out_mix_z")},
                 work/"out_mix_z"/"output.ims", d, {3});
        expectOk("X.tif + X.zarr, -r tiff", {"-r", "tiff", "-F", native(mix), "-P", "CamA", "-o", native(work/"out_mix_t")},
                 work/"out_mix_t"/"output.ims", d, {4});

        // two folders: 2 + 1 files, 3 time points in name order
        const fs::path m1 = work/"multi1", m2 = work/"multi2";
        writeZarr(m1/"CamA_t0000.zarr", d, 10);
        writeZarr(m1/"CamA_t0001.zarr", d, 11);
        writeZarr(m2/"CamA_t0002.zarr", d, 12);
        expectOk("two folders, 2 + 1 files", {"-r", "zarr", "-F", native(m1) + "," + native(m2), "-P", "CamA",
                 "-o", native(work/"out_multi"), "-t", "3"}, work/"out_multi"/"output.ims", d, {10, 11, 12});

        // 12 time points created in shuffled order
        const fs::path sh = work/"shuffled";
        const Dims ds = {16, 12, 8};
        const int order[12] = {7, 2, 11, 0, 5, 9, 3, 10, 1, 8, 6, 4};
        for(int i : order){
            char name[64];
            std::snprintf(name, sizeof name, "CamA_t%04d.zarr", i);
            writeZarr(sh/name, ds, 20 + i);
        }
        std::vector<int> seeds;
        for(int t = 0; t < 12; ++t) seeds.push_back(20 + t);
        expectOk("12 time points created in shuffled order", {"-r", "zarr", "-F", native(sh), "-P", "CamA",
                 "-o", native(work/"out_shuffled")}, work/"out_shuffled"/"output.ims", ds, seeds);

        // '%' in a file name and in a folder name, a space in a path: the same data as the plain name
        const fs::path pct = work/"pct", pctDir = work/"50%s_run", space = work/"with space";
        writeZarr(pct/"CamA_100%done.zarr", d, 0);
        writeZarr(pctDir/"CamA_t0000.zarr", d, 0);
        writeZarr(space/"CamA t0000.zarr", d, 0);
        const fs::path ref = work/"out_plain"/"output.ims";
        expectOk("plain name", {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(work/"out_plain")}, ref, d, {0});
        const struct { const char* what; fs::path in; const char* outName; } named[] = {
            {"CamA_100%done.zarr", pct, "out_pct"}, {"folder 50%s_run", pctDir, "out_pctdir"}, {"a space in the path", space, "out space"}};
        for(const auto& k : named){
            const fs::path ims = work/k.outName/"output.ims";
            expectOk(k.what, {"-r", "zarr", "-F", native(k.in), "-P", "CamA", "-o", native(work/k.outName)}, ims, d, {0});
            check(sameData(ref, ims), std::string(k.what) + ": the same data as the plain name");
        }

        // 100 channels (the names are no longer kept in a fixed buffer)
        const fs::path many = work/"many";
        const Dims dm = {8, 8, 4};
        std::string patterns;
        for(int c = 0; c < 100; ++c){
            char name[32];
            std::snprintf(name, sizeof name, "c%03dx", c);
            writeZarr(many/(std::string(name) + ".zarr"), dm, c);
            patterns += (c ? "," : "") + std::string(name);
        }
        const fs::path mims = work/"out_many"/"output.ims";
        const RunResult r = run({"-r", "zarr", "-F", native(many), "-P", patterns, "-o", native(work/"out_many")});
        const std::optional<std::string> name99 = imsReadAttribute(mims, "/DataSetInfo/Channel 99", "Name");
        check(r.exitCode == 0 && voxelsEqual(mims, 99, dm, {99}) && name99 && *name99 == "c099x",
              "100 channels: exit " + std::to_string(r.exitCode) + ", channel 99 named \"" + name99.value_or("missing") + "\"");
    }

    std::printf("[output folder]\n");
    {
        const fs::path nested = work/"o1"/"o2"/"o3";
        expectOk("output folder nested three levels", {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(nested)},
                 nested/"output.ims", d, {0});
        writeText(work/"a_file", "x");
        expectError("output folder under a file", {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(work/"a_file"/"sub")},
                    "unable to be created");
        const fs::path tp = work/"out_tp";
        fs::create_directories(tp/"timepoints.txt");   // a folder: timepoints.txt cannot be opened
        const RunResult r = expectOk("timepoints.txt cannot be written", {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(tp)},
                                     tp/"output.ims", d, {0});
        check(r.err.find("WARNING: Could not open") != std::string::npos && countErrorLines(r.err) == 0,
              "timepoints.txt cannot be written: a warning (" + firstLines(r.err) + ")");
    }

    std::printf("[inputs that used to crash]\n");
    {
        const fs::path o = work/"out_bad";
        writeText(work/"badzarr"/"CamA_t0000.zarr"/".zarray", "{not json");
        expectError("bad zarr metadata", {"-r", "zarr", "-F", native(work/"badzarr"), "-P", "CamA", "-o", native(o)},
                    "could not read", o/"output.ims");
        const fs::path corrupt = work/"corrupt";
        writeZarr(corrupt/"CamA_t0000.zarr", d, 0);
        writeText(corrupt/"CamA_t0000.zarr"/"0.0.0", "garbage, not a zstd frame");
        expectError("a corrupt chunk", {"-r", "zarr", "-F", native(corrupt), "-P", "CamA", "-o", native(o)},
                    "could not read", o/"output.ims");
        writeText(work/"nottiff"/"CamA_t0000.tif", "not a TIFF file");
        expectError("a .tif that is not a TIFF", {"-r", "tiff", "-F", native(work/"nottiff"), "-P", "CamA", "-o", native(o)},
                    "could not read", o/"output.ims");
    }

    std::printf("[sample types]\n");
    {
        const fs::path o = work/"out_types", oIms = o/"output.ims";
        const uint64_t n = d.x*d.y*d.z;
        auto refused = [&](const std::string& what, const char* reader, const fs::path& in, const std::string& type){
            expectError(what, {"-r", reader, "-F", native(in), "-P", "CamA", "-o", native(o)},
                        "has sample type " + type + "; supported are uint8, uint16, uint32 and float32", oIms);
        };
        std::vector<int16_t> i16(n);
        for(uint64_t i = 0; i < n; ++i) i16[i] = (int16_t)((int64_t)(i % 1000) - 500);
        writeZarrData(work/"i16z"/"CamA_t0000.zarr", d, i16.data(), "<i2", 16);
        refused("int16 Zarr", "zarr", work/"i16z", "int16");
        writeTiffData(work/"i16t"/"CamA_t0000.tif", d, i16.data(), 16, 2);
        refused("int16 TIFF", "tiff", work/"i16t", "int16");
        std::vector<double> f64(n, 1.5);
        writeZarrData(work/"f64"/"CamA_t0000.zarr", d, f64.data(), "<f8", 64);
        refused("float64 Zarr", "zarr", work/"f64", "float64");
        writeTiffData(work/"f64t"/"CamA_t0000.tif", d, f64.data(), 64, 3);
        refused("float64 TIFF", "tiff", work/"f64t", "float64");
        std::vector<uint8_t> rgb(n*3, 7);
        writeTiffData(work/"rgb"/"CamA_t0000.tif", d, rgb.data(), 8, 1, 3);
        refused("RGB TIFF", "tiff", work/"rgb", "uint8 with 3 samples per pixel");
        writeZarrShape(work/"boolz"/"CamA_t0000.zarr", {d.x, d.y, d.z}, "|b1", 8);
        refused("bool Zarr", "zarr", work/"boolz", "bool");
        writeZarrShape(work/"z2d"/"CamA_t0000.zarr", {d.x, d.y}, "<u2", 16);
        expectError("2-D Zarr", {"-r", "zarr", "-F", native(work/"z2d"), "-P", "CamA", "-o", native(o)},
                    "has 2 dimensions; only 3-D arrays are supported", oIms);
        writeZarrShape(work/"z4d"/"CamA_t0000.zarr", {d.x, d.y, d.z, 2}, "<u2", 16);
        expectError("4-D Zarr", {"-r", "zarr", "-F", native(work/"z4d"), "-P", "CamA", "-o", native(o)},
                    "has 4 dimensions; only 3-D arrays are supported", oIms);

        // A second time point of another type: refused before any output, also with the same bit depth
        std::vector<float> f32(n, 2.5f);
        std::vector<uint32_t> u32(n, 70000);
        writeZarrData(work/"mixz"/"CamA_t0000.zarr", d, f32.data(), "<f4", 32);
        writeZarrData(work/"mixz"/"CamA_t0001.zarr", d, u32.data(), "<u4", 32);
        expectError("Zarr uint32 after float32", {"-r", "zarr", "-F", native(work/"mixz"), "-P", "CamA", "-o", native(o)},
                    "CamA_t0001.zarr has sample type uint32 but", oIms);
        writeTiffData(work/"mixt"/"CamA_t0000.tif", d, f32.data(), 32, 3);
        writeTiffData(work/"mixt"/"CamA_t0001.tif", d, u32.data(), 32, 1);
        expectError("TIFF uint32 after float32", {"-r", "tiff", "-F", native(work/"mixt"), "-P", "CamA", "-o", native(o)},
                    "CamA_t0001.tif has sample type uint32 but", oIms);
        writeZarr(work/"mixi"/"CamA_t0000.zarr", d, 0);
        writeZarrData(work/"mixi"/"CamA_t0001.zarr", d, i16.data(), "<i2", 16);
        expectError("Zarr int16 after uint16", {"-r", "zarr", "-F", native(work/"mixi"), "-P", "CamA", "-o", native(o)},
                    "CamA_t0001.zarr has sample type int16 but", oIms);
    }

    std::printf("[bounding boxes]\n");
    {
        expectError("-B with max == min", conv({"-B", "5,0,0,5,30,20"}), "must end after it starts", outIms);
        expectError("-B outside along Z only", conv({"-B", "0,0,10,40,30,21"}), "does not fit in", outIms);
        const fs::path te = work/"boxtiff";
        writeTiff(te/"CamA_t0000.tif", d, 0);
        expectError("TIFF -B outside", {"-r", "tiff", "-F", native(te), "-P", "CamA", "-o", native(out), "-B", "0,0,0,40,31,20"},
                    "does not fit in", outIms);
        // the first time point holds the box, the second does not: the message names the second
        const fs::path bs = work/"boxsizes";
        writeZarr(bs/"CamA_t0000.zarr", d, 0);
        writeZarr(bs/"CamA_t0001.zarr", {40, 30, 10}, 1);
        expectError("-B inside the first time point only", {"-r", "zarr", "-F", native(bs), "-P", "CamA", "-o", native(out),
                    "-B", "0,0,0,20,20,15"}, "does not fit in " + native(bs/"CamA_t0001.zarr") + " (40x30x10)", outIms);
    }

#ifndef _WIN32
    std::printf("[POSIX: home folder, permissions]\n");
    {
        const fs::path home = work/"home";
        writeZarr(home/"in"/"CamA_t0000.zarr", d, 5);
        expectOk("-F ~/in -o ~/out with HOME set", {"-r", "zarr", "-F", "~/in", "-P", "CamA", "-o", "~/out"},
                 home/"out"/"output.ims", d, {5}, native(home));
        expectOk("-o ~ with HOME set", {"-r", "zarr", "-F", "~/in", "-P", "CamA", "-o", "~", "-n", "athome"},
                 home/"athome.ims", d, {5}, native(home));
        // a "~" that is not leading is kept (it used to go through wordexp). TIFF: cpp-zarr itself
        // still runs wordexp on every Zarr path that holds a '~'.
        const fs::path tilde = work/"til~de (x)";
        writeTiff(tilde/"CamA_t0000.tif", d, 6);
        expectOk("a ~ inside a folder name", {"-r", "tiff", "-F", native(tilde), "-P", "CamA", "-o", native(work/"out_tilde")},
                 work/"out_tilde"/"output.ims", d, {6});
        if(isRoot()) std::printf("  skipped: the permission cases (running as root)\n");
        else{
            const fs::path closed = work/"closed";
            writeZarr(closed/"CamA_t0000.zarr", d, 0);
            chmod(closed.string().c_str(), 0);
            expectError("an unreadable input folder", {"-r", "zarr", "-F", native(closed), "-P", "CamA", "-o", native(out)},
                        "Could not open input folder", outIms);
            chmod(closed.string().c_str(), 0755);
            const fs::path ro = work/"readonly";
            fs::create_directories(ro);
            chmod(ro.string().c_str(), 0555);
            expectError("an unwritable output folder", {"-r", "zarr", "-F", P, "-P", "CamA", "-o", native(ro)},
                        "cannot be written to", ro/"output.ims");
            chmod(ro.string().c_str(), 0755);
        }
    }
#endif

    if(gFailures == 0){
        std::printf("clitest PASSED\n");
        fs::remove_all(work);
        return 0;
    }
    std::printf("clitest FAILED (%d checks); logs in %s\n", gFailures, gLogs.string().c_str());
    return 1;
}
