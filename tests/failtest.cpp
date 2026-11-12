// Failure test: a conversion whose output cannot be written must end with an error, not hang.
//
// ImarisWriter writes the .ims in its own threads, and an error there used to be dropped: the
// converter then waited forever (or wrote an incomplete file and exited 0). This runs the shipped
// executable on generated inputs whose output cannot be written, and checks that it exits with
// code 1 and an "ERROR: ... write ..." message within 30 s (a run still going then is killed and
// counted as a hang; PIW_FAILTEST_TIMEOUT=<seconds> changes the limit, e.g. for a sanitizer build):
//   1. a file-size limit (RLIMIT_FSIZE, SIGXFSZ ignored, so write() fails with EFBIG) of 8 MB, far
//      below the output of an incompressible 40 MB volume (random uint16 in an uncompressed Zarr);
//      also with -C 64,64,64 (block processing) and with two timepoints.
//   2. a limit one byte below the size of the complete file, single-threaded (then every run writes
//      the same layout): the last write fails, which may be the flush when the file is closed.
//   3. the output file name is taken by a directory, so ImarisWriter cannot create the file.
// Then the same inputs convert without a limit: exit code 0, and level 0 equals the input.
// POSIX only (fork/exec and setrlimit); not built on Windows.
//   usage: failTest <path-to-parallelimariswriter> [workdir]
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>
#include <filesystem>

#include <fcntl.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "zarr.h"                // cpp-zarr
#include "parallelwritezarr.h"
#include "imsutil.h"             // .ims decoding (HDF5 + lz4)

extern char** environ;

namespace fs = std::filesystem;

static const uint64_t X = 512, Y = 512, Z = 80;           // 40 MiB of uint16
static const rlim_t kLimit = 8ull << 20;                  // file-size limit of case 1
static double gTimeout = 30;                              // seconds a run may take

static int gFailures = 0;
static void check(bool ok, const std::string& what){
    if(!ok){ std::printf("  FAIL: %s\n", what.c_str()); ++gFailures; }
}

// Random (incompressible) voxels; xorshift64 so every platform writes the same data.
static std::vector<uint16_t> makeVolume(uint64_t seed){
    std::vector<uint16_t> v(X*Y*Z);
    uint64_t s = 0x9E3779B97F4A7C15ull * (seed + 1);
    for(auto& e : v){ s ^= s << 13; s ^= s >> 7; s ^= s << 17; e = (uint16_t)(s >> 24); }
    return v;
}

// <dir>/<name>.zarr: F order, uncompressed 128x128x64 chunks.
static bool writeZarr(const fs::path& dir, const std::string& name, const std::vector<uint16_t>& v){
    fs::create_directories(dir);
    std::vector<uint64_t> shape = {X, Y, Z}, chunks = {128, 128, 64};
    zarr Zw;
    Zw.set_fileName((dir/(name + ".zarr")).string());
    Zw.set_cname("none"); Zw.set_order("F");
    Zw.set_chunks(chunks); Zw.set_dimension_separator("."); Zw.set_dtype("<u2"); Zw.set_shape(shape);
    Zw.write_zarray();
    Zw.set_chunkInfo({0, 0, 0}, shape);
    if(parallelWriteZarr(Zw, (void*)v.data(), {0, 0, 0}, shape, shape, 16, false, false, false)){
        std::printf("  FAIL: Zarr write of %s failed: %s\n", name.c_str(), Zw.get_errString().c_str());
        return false;
    }
    return true;
}

// C stdio, not iostreams: on macOS this binary binds part of the C++ runtime to the copy bundled (and
// exported) by libcppZarr, and iostream calls that mix the two copies abort.
static std::string readText(const fs::path& p){
    std::string b;
    FILE* f = std::fopen(p.string().c_str(), "rb");
    if(!f) return b;
    char buf[1 << 16]; size_t n;
    while((n = std::fread(buf, 1, sizeof buf, f)) > 0) b.append(buf, n);
    std::fclose(f);
    return b;
}

struct RunResult {
    int exitCode = -1;      // exit code, or -1 if killed by a signal or by the timeout
    int signal = 0;         // the signal that ended it
    bool timedOut = false;  // still running after gTimeout seconds (killed)
    double seconds = 0;
    std::string output;     // stdout + stderr
};

// Runs <exe> <args> with its output in <log>; fsizeLimit > 0 sets RLIMIT_FSIZE for it (and makes it
// ignore SIGXFSZ), threads > 0 sets OMP_NUM_THREADS. Everything the child needs is prepared before
// fork(), because this process has threads (OpenMP) and the child may only make system calls.
static RunResult run(const std::string& exe, const std::vector<std::string>& args, const fs::path& log,
                     rlim_t fsizeLimit, int threads){
    std::vector<std::string> argStore = {exe};
    argStore.insert(argStore.end(), args.begin(), args.end());
    std::vector<char*> argv;
    for(auto& a : argStore) argv.push_back(const_cast<char*>(a.c_str()));
    argv.push_back(nullptr);
    std::vector<std::string> envStore;
    for(char** e = environ; *e; ++e)
        if(threads <= 0 || std::strncmp(*e, "OMP_NUM_THREADS=", 16) != 0) envStore.push_back(*e);
    if(threads > 0) envStore.push_back("OMP_NUM_THREADS=" + std::to_string(threads));
    std::vector<char*> envp;
    for(auto& e : envStore) envp.push_back(const_cast<char*>(e.c_str()));
    envp.push_back(nullptr);
    const std::string logName = log.string();

    std::string cmd;
    for(auto& a : argStore) cmd += (cmd.empty() ? "" : " ") + a;
    std::printf("  $ %s%s%s\n", fsizeLimit ? ("(file-size limit " + std::to_string(fsizeLimit) + " bytes) ").c_str() : "",
                threads > 0 ? ("OMP_NUM_THREADS=" + std::to_string(threads) + " ").c_str() : "", cmd.c_str());
    std::fflush(stdout);

    RunResult r;
    const auto t0 = std::chrono::steady_clock::now();
    pid_t pid = fork();
    if(pid < 0){ check(false, std::string("fork failed: ") + std::strerror(errno)); return r; }
    if(pid == 0){
#ifdef __linux__
        prctl(PR_SET_PDEATHSIG, SIGKILL);   // never outlive the test (if ctest kills it)
#endif
        int fd = open(logName.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if(fd >= 0){ dup2(fd, 1); dup2(fd, 2); close(fd); }
        if(fsizeLimit){
            signal(SIGXFSZ, SIG_IGN);
            struct rlimit rl;
            if(getrlimit(RLIMIT_FSIZE, &rl) == 0){
                rl.rlim_cur = fsizeLimit;
                setrlimit(RLIMIT_FSIZE, &rl);
            }
        }
        execve(argv[0], argv.data(), envp.data());
        _exit(127);
    }
    int status = 0;
    for(;;){
        pid_t w = waitpid(pid, &status, WNOHANG);
        if(w == pid) break;
        if(w < 0 && errno != EINTR){ check(false, std::string("waitpid failed: ") + std::strerror(errno)); return r; }
        if(std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() > gTimeout){
            kill(pid, SIGKILL);
            waitpid(pid, &status, 0);
            r.timedOut = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    if(!r.timedOut && WIFEXITED(status)) r.exitCode = WEXITSTATUS(status);
    else if(WIFSIGNALED(status)) r.signal = WTERMSIG(status);
    r.output = readText(log);
    return r;
}

// The output's last lines that are not progress lines.
static std::string tail(const std::string& output){
    std::vector<std::string> lines;
    for(size_t b = 0; b < output.size();){
        size_t e = output.find('\n', b);
        if(e == std::string::npos) e = output.size();
        const std::string l = output.substr(b, e - b);
        if(l.rfind("Progress", 0) != 0) lines.push_back(l);
        b = e + 1;
    }
    std::string t;
    for(size_t i = lines.size() > 3 ? lines.size() - 3 : 0; i < lines.size(); i++) t += "    | " + lines[i] + "\n";
    return t;
}

static std::string lower(std::string s){
    for(auto& c : s) c = (char)std::tolower((unsigned char)c);
    return s;
}

static std::vector<std::string> convertArgs(const fs::path& in, const fs::path& out, const std::vector<std::string>& extra){
    std::vector<std::string> a = {"-r", "zarr", "-P", "CamA", "-F", in.string(), "-o", out.string(), "-v", "1,1,1"};
    a.insert(a.end(), extra.begin(), extra.end());
    return a;
}

// A run that cannot write its output: it must exit 1 with an error that says so, within gTimeout.
static void expectWriteFailure(const std::string& what, const std::string& exe, const fs::path& in,
                               const fs::path& out, const std::vector<std::string>& extra,
                               rlim_t fsizeLimit, int threads){
    std::printf("[%s]\n", what.c_str());
    const int before = gFailures;
    RunResult r = run(exe, convertArgs(in, out, extra), out.string() + ".log", fsizeLimit, threads);
    if(r.timedOut) check(false, "still running after " + std::to_string((int)gTimeout) + " s (a hang): killed");
    else if(r.signal) check(false, "killed by signal " + std::to_string(r.signal) + " after " + std::to_string(r.seconds) + " s");
    else check(r.exitCode == 1, "exit code " + std::to_string(r.exitCode) + ", expected 1");
    const std::string o = lower(r.output);
    check(o.find("error:") != std::string::npos && o.find("write") != std::string::npos,
          "no \"ERROR: ... write ...\" message in the output");
    std::printf("  exit %d after %.1f s\n%s", r.exitCode, r.seconds, tail(r.output).c_str());
    if(gFailures == before) std::printf("  OK\n");
}

// A run without a limit: exit 0 and (one timepoint) level 0 equal to the input. Returns the file size.
static uintmax_t expectSuccess(const std::string& what, const std::string& exe, const fs::path& in,
                               const fs::path& out, const std::vector<std::string>& extra, int threads,
                               const std::vector<const std::vector<uint16_t>*>& inputs){
    std::printf("[%s]\n", what.c_str());
    const int before = gFailures;
    fs::remove_all(out); fs::create_directories(out);
    RunResult r = run(exe, convertArgs(in, out, extra), out.string() + ".log", 0, threads);
    check(r.exitCode == 0, "exit code " + std::to_string(r.exitCode) + (r.timedOut ? " (hang: killed)" : "") +
                           (r.signal ? " (signal " + std::to_string(r.signal) + ")" : "") + ", expected 0");
    const fs::path ims = out/"output.ims";
    uintmax_t size = 0;
    if(r.exitCode == 0 && fs::exists(ims)){
        size = fs::file_size(ims);
        for(size_t t = 0; t < inputs.size(); t++){
            ImsVolumeInfo info;
            std::vector<uint8_t> got = imsReadVolume(ims, 0, (int)t, 0, info);
            check(got.size() == inputs[t]->size()*2 && std::memcmp(got.data(), inputs[t]->data(), got.size()) == 0,
                  "timepoint " + std::to_string(t) + ": level 0 differs from the input" +
                  (info.error.empty() ? "" : " (" + info.error + ")"));
        }
    }
    else std::printf("%s", tail(r.output).c_str());
    std::printf("  exit %d after %.1f s, %llu bytes\n", r.exitCode, r.seconds, (unsigned long long)size);
    if(gFailures == before) std::printf("  OK\n");
    return size;
}

int main(int argc, char** argv){
    if(argc < 2){ std::fprintf(stderr, "usage: %s <parallelimariswriter> [workdir]\n", argv[0]); return 2; }
    const std::string exe = fs::absolute(argv[1]).string();
    const fs::path work = (argc > 2) ? fs::path(argv[2]) : fs::temp_directory_path()/"piw_failtest";
    if(const char* t = std::getenv("PIW_FAILTEST_TIMEOUT")) if(std::atof(t) > 0) gTimeout = std::atof(t);
    fs::remove_all(work); fs::create_directories(work);

    const fs::path one = work/"in_one", two = work/"in_two";
    const std::vector<uint16_t> v0 = makeVolume(0), v1 = makeVolume(1);
    if(!writeZarr(one, "CamA_t0000", v0) || !writeZarr(two, "CamA_t0000", v0) || !writeZarr(two, "CamA_t0001", v1))
        return 1;

    // 1. file-size limit far below the output
    expectWriteFailure("8 MB file-size limit", exe, one, work/"out_limit", {}, kLimit, 0);
    expectWriteFailure("8 MB file-size limit, -C 64,64,64", exe, one, work/"out_limit_C", {"-C", "64,64,64"}, kLimit, 0);
    expectWriteFailure("8 MB file-size limit, two timepoints", exe, two, work/"out_limit_2t", {}, kLimit, 0);

    // 2. limit one byte below the complete file; one thread, so that both runs write the same layout
    const uintmax_t full = expectSuccess("no limit, one thread", exe, one, work/"out_full1", {}, 1, {&v0});
    if(full > 1)
        expectWriteFailure("file-size limit of the complete size - 1, one thread", exe, one, work/"out_short",
                           {}, (rlim_t)(full - 1), 1);

    // 3. the output file name is taken by a directory
    {
        const fs::path out = work/"out_isdir";
        fs::create_directories(out/"output.ims");
        expectWriteFailure("output.ims is a directory", exe, one, out, {}, 0, 0);
    }

    // The same inputs without a limit.
    expectSuccess("no limit", exe, one, work/"out_ok", {}, 0, {&v0});
    expectSuccess("no limit, -C 64,64,64", exe, one, work/"out_ok_C", {"-C", "64,64,64"}, 0, {&v0});
    expectSuccess("no limit, two timepoints", exe, two, work/"out_ok_2t", {}, 0, {&v0, &v1});

    if(gFailures == 0){
        std::printf("failtest PASSED\n");
        fs::remove_all(work);
        return 0;
    }
    std::printf("failtest FAILED (%d checks)\n", gFailures);
    return 1;
}
