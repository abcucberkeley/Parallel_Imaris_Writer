// imscompare: compares two .ims files, or checks one, for the tests and for validating
// cluster outputs. A command-line front end over tests/imsutil.h (imsCompare,
// imsDeepCheck); it is a test tool and is not part of the release folder.
//
//   imscompare [options] a.ims b.ims    compare b with a
//   imscompare --deep [options] a.ims   check a alone
//
// By default only the Data datasets are compared: for every /DataSet/ResolutionLevel r/
// TimePoint t/Channel c the dims, chunk dims, type, filters, the set of chunks (by
// coordinate), and each chunk's size, filter mask and compressed bytes. The file layout
// (chunk addresses, object order) does not matter. Chunks are listed in one
// H5Dchunk_iter pass per dataset and their bytes read from the file by --threads
// threads, so the run time is linear in the file size.
//
// Options:
//   --all              also compare the metadata: the attributes of the /DataSet groups
//                      (ImageSizeX/Y/Z, HistogramMin/Max) and of Data, the Histogram and
//                      Histogram1024 datasets, the root's links and attributes, every
//                      /DataSetInfo group's attributes, and /Thumbnail (reported apart)
//   --no-thumbnail     with --all: leave /Thumbnail out
//   --map tA,cA=tB,cB  compare timepoint tA channel cA of a with timepoint tB channel cB
//                      of b, at every resolution level (e.g. one (t,c) of a single-file
//                      cluster output with a standalone part). Repeatable; when given,
//                      only the mapped datasets are compared (with --all also their
//                      group attributes and histograms; never the root, /DataSetInfo or
//                      /Thumbnail, which differ between such files by design)
//   --deep             also decode every chunk of a.ims and check that the chunks
//                      covering the image are all stored, that each decodes to a full
//                      chunk, and that the stored histograms (Histogram, Histogram1024,
//                      HistogramMin/Max) equal the ones rebuilt from the voxels: exactly
//                      for uint8 and uint16, the totals for float32
//   --threads N        threads that read, compare and decode chunks (default 4)
//   --max-lines N      differences printed per part (default 40)
//
// Exit code: 0 identical (and, with --deep, a.ims checks clean); 1 a difference or a
// failed deep check; 2 a usage error or a file that cannot be opened or read;
// 3 identical except /Thumbnail (with --all; nothing else differs and --deep is clean).
// The last line of the output is a one-line summary starting with "imscompare:".
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "imsutil.h"

static void usage(){
    std::fprintf(stderr,
        "usage: imscompare [--all] [--no-thumbnail] [--map tA,cA=tB,cB]... [--deep] [--threads N]\n"
        "                  [--max-lines N] a.ims [b.ims]\n"
        "  Compares the Data chunks of b.ims with a.ims (--all: and every attribute, histogram\n"
        "  and /Thumbnail); --deep also decodes a.ims and checks its histograms; with one file\n"
        "  only --deep runs. Exit: 0 identical, 1 different, 2 error, 3 identical except /Thumbnail.\n");
}

static bool parseInt(const char* s, int& v){
    char* end = nullptr;
    long x = std::strtol(s, &end, 10);
    if(!s[0] || *end || x < 1 || x > 4096) return false;
    v = (int)x;
    return true;
}

static std::string gb(uint64_t bytes){
    char b[32]; std::snprintf(b, sizeof b, "%.2f GB", (double)bytes / 1e9); return b;
}

int main(int argc, char** argv){
    bool all = false, thumbnail = true, deep = false;
    int threads = 4, maxLines = 40;
    std::vector<ImsMapping> map;
    std::vector<std::string> files;
    for(int i = 1; i < argc; ++i){
        const std::string a = argv[i];
        if(a == "--all") all = true;
        else if(a == "--no-thumbnail") thumbnail = false;
        else if(a == "--deep") deep = true;
        else if(a == "--threads" || a == "--max-lines"){
            if(i + 1 >= argc || !parseInt(argv[i + 1], a == "--threads" ? threads : maxLines)){
                std::fprintf(stderr, "imscompare: %s needs a number from 1 to 4096\n", a.c_str()); return 2; }
            ++i;
        } else if(a == "--map"){
            ImsMapping m; char tail = 0;
            if(i + 1 >= argc || std::sscanf(argv[i + 1], "%d,%d=%d,%d%c", &m.tA, &m.cA, &m.tB, &m.cB, &tail) != 4 ||
               m.tA < 0 || m.cA < 0 || m.tB < 0 || m.cB < 0){
                std::fprintf(stderr, "imscompare: --map needs tA,cA=tB,cB (non-negative integers), e.g. --map 3,1=0,0\n"); return 2; }
            map.push_back(m); ++i;
        } else if(a == "-h" || a == "--help"){ usage(); return 0; }
        else if(a.size() > 1 && a[0] == '-'){ std::fprintf(stderr, "imscompare: unknown option %s\n", a.c_str()); usage(); return 2; }
        else files.push_back(a);
    }
    if(files.empty() || files.size() > 2 || (files.size() == 1 && !deep)){ usage(); return 2; }
    for(const std::string& f : files)
        if(!std::filesystem::is_regular_file(f)){ std::fprintf(stderr, "imscompare: %s: no such file\n", f.c_str()); return 2; }

    const auto t0 = std::chrono::steady_clock::now();
    auto seconds = [&]{ return std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(); };
    std::string summary;
    int rc = 0;

    if(files.size() == 2){
        ImsCompareOptions o;
        o.histograms = o.datasetAttributes = o.dataSetInfo = all;
        o.thumbnail = all && thumbnail;
        o.map = map;
        o.threads = threads;
        std::printf("comparing a=%s with b=%s (%s%s, %d threads)\n", files[0].c_str(), files[1].c_str(),
                    all ? "data and metadata" : "data", map.empty() ? "" : ", mapped datasets", threads);
        std::fflush(stdout);
        ImsCompareReport r;
        imsCompare(files[0], files[1], r, o);
        if(!all){
            if(map.empty()) r.notes.clear();   // the per-part "not compared" notes, summed up in one
            r.notes.push_back("only the Data chunks were compared (--all: also the metadata and /Thumbnail)");
        }
        imsPrintReport(r, stdout, (size_t)maxLines);
        const double s = seconds();
        rc = !r.error.empty() ? 2 : r.identical() ? 0 : r.identicalExceptThumbnail() ? 3 : 1;
        const char* verdict[] = {"IDENTICAL", "DIFFERENT", "ERROR", "IDENTICAL except /Thumbnail"};
        char b[256];
        std::snprintf(b, sizeof b, "%s (%d datasets, %llu chunks, %s per file, %.1f s, %.0f MB/s per file)", verdict[rc],
                      r.datasets, (unsigned long long)r.chunks, gb(r.bytes).c_str(), s, s > 0 ? (double)r.bytes / 1e6 / s : 0.0);
        summary = b;
    }
    if(deep){
        const double s0 = seconds();
        std::printf("deep check of %s (%d threads)\n", files[0].c_str(), threads);
        std::fflush(stdout);
        ImsCompareReport r;
        bool clean = imsDeepCheck(files[0], r, threads);
        imsPrintReport(r, stdout, (size_t)maxLines, true);
        const double s = seconds() - s0;
        int drc = !r.error.empty() ? 2 : clean ? 0 : 1;
        // A deep-check failure outranks "identical"; an error outranks everything.
        if(drc == 2 || (drc == 1 && rc != 2)) rc = drc;
        char b[256];
        std::snprintf(b, sizeof b, "DEEP CHECK %s (%d datasets, %llu chunks, %s, %.1f s)", clean ? "CLEAN" : drc == 2 ? "ERROR" : "FAILED",
                      r.datasets, (unsigned long long)r.chunks, gb(r.bytes).c_str(), s);
        summary += (summary.empty() ? "" : "; ") + std::string(b);
    }
    std::printf("imscompare: %s\n", summary.c_str());
    return rc;
}
