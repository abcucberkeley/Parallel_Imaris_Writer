#ifndef PARALLEL_IMARIS_PARALLEL_H
#define PARALLEL_IMARIS_PARALLEL_H

// Cluster/distributed conversion subcommands. Many independent SLURM jobs read a zarr time series
// and produce Imaris .ims output whose voxel data is byte-identical to a monolithic run. Every
// subcommand prints its errors on stderr and exits non-zero on failure (2: a usage error).
//   parallelimariswriter setup      <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--store mono|packs]
//                                   [--option value ...]
//        writes <workdir>/manifest.json; cx,cy,cz = per-worker tile size (voxels); voxel size
//        defaults to 0.108,0.108,0.108 (as the converter); env PIW_CHUNK_BYTES = chunk budget.
//        --store (default: env PIW_STORE, else mono; packs on Windows) = where the workers put the chunks:
//        mono = straight into the one .ims (<workdir>/output.ims.partial until monitor renames it; Linux and
//        macOS), packs = one .pack file per tile for monitor's serial merge, combineone and link.
//        The other options are the converter's metadata options (--channel-names, --colors, --emission,
//        --excitation, --time-interval, --start-time; imsmeta.h), resolved here into the manifest
//        so every output of this workdir carries the converter's metadata
//   parallelimariswriter worker     <workdir> <level> <index> [count]
//        tiles index..index+count-1 (default 1) of one level, one after the other; run level 0's array
//        first, then 1's, ... (level r+1 reads level r's chunks). mono: claims the tile's byte range of the
//        file, writes its chunks there, then chunks/h_L*.hist and chunks/L*.idx (its sidecar: the commit
//        point); packs: chunks/h_L*.hist, then chunks/L*.pack. OMP_NUM_THREADS = threads for encoding and
//        decoding; one timing line per tile
//   parallelimariswriter monitor    <workdir> [out.ims] [--verify] [--threads N]
//        waits for every expected tile (env PIW_MONITOR_TIMEOUT = give up after N seconds), then mono:
//        monofinish <workdir> <out.ims> (src/mono.h: checks the sidecars, builds the HDF5 index over the
//        placed chunks, renames the file); packs: checks every pack and merges them into a new .ims.
//        Default out.ims: <workdir>/output.ims
//   parallelimariswriter combineone <workdir> <t> <c> <part.ims>   -- one (timepoint, channel) from the packs
//   parallelimariswriter link       <workdir> <out.ims> <partsDir>  -- external links to combineone's parts
//   parallelimariswriter linkparts  <workdir> <out.ims> <partsDir>  -- external links to per-(t,c) converter outputs
//   parallelimariswriter layout     <X> <Y> <Z> [elemSize] [timepoints]  -- debug: print the pyramid
// partsDir is relative to out.ims and holds part_t<t>_c<c>.ims. The tile path (worker, monitor) takes uint16 data,
// and uint8 in the single-file store; the packs store (and combineone, link) uint16 only; linkparts takes whatever
// the converter wrote.
// One self-contained .ims from per-(t,c) converter outputs, without links or parts: monoinit, monoplace,
// monofinish, monostatus (src/mono.h); monofinish and monostatus also take a mono-store tile workdir.
//
// The same steps as a plan of independent tasks (the functions below; src/cluster.cpp), for any job system or
// none: plan once, run every task of a stage (each in any process on any node, in any order, each again after a
// failure), the next stage after it, then finish.
//   parallelimariswriter plan   <workdir> -F folders -P patterns [the converter's options: -r -v -b -B -C -c -t -o -n
//                               --channel-names ...] [--tiles X,Y,Z [--store mono|packs] [--tiles-per-task N]
//                               [--chunk-bytes N]]
//        checks every input as the converter does and writes <workdir>/manifest.json with the stages; without
//        --tiles a time series (stage "convert": one task per (timepoint, channel)), with it one enormous volume
//        per (timepoint, channel) cut into tiles (stages "level0", "level1", ...: Zarr, uint8 or uint16)
//   parallelimariswriter task   <workdir> <stage> <index> [--threads N] [--stage-dir DIR] [-C X,Y,Z]
//        one task (stage by name or number); a task that is already done is skipped
//   parallelimariswriter status <workdir>                      -- per stage the tasks not done (exit 3 if any)
//   parallelimariswriter finish <workdir> [out.ims] [--verify] [--threads N]
//        exit 3, naming the tasks, while any is not done; else the output (default: the plan's)
#include "parallelimariswriter.h"
#include "mono.h"
#include <cstdint>
#include <string>
#include <vector>

// True if argv[1] selects a parallel subcommand (vs the default monolithic conversion).
bool isParallelCommand(const char* cmd);
// Runs the selected parallel subcommand; returns its process exit code.
int runParallel(int argc, char** argv);
// task, status and finish (src/cluster.cpp; plan is in src/cli.cpp with the converter's options)
bool isClusterCommand(const char* cmd);
int runCluster(int argc, char** argv);

namespace piw {

// ---- the cluster steps as functions (noexcept; a Status, no printing, no exit; threads as arguments; exported from
// the shared library like parallelimariswriter.h's) ----
//
// Time series ("series"): stage "convert", task i = timepoint i / channels, channel i % channels: converts that one
// (timepoint, channel) with piw::convert into a staging folder, places it into the single file (monoPlace) and
// removes the staged part. clusterFinish = monoFinish. The output equals a single-node conversion of all inputs with
// the same options (data, metadata, /Thumbnail; float32: see monoFinish in src/mono.h). Linux and macOS; uint8,
// uint16 and float32 (uint32: refused, use linkparts).
// Tiled ("tiles"): stage "level<r>" (after "level<r-1>"), task i = tiles i*tilesPerTask .. of that level (setup's
// tile workers, parallel.h); store "mono" writes straight into the single file (Linux and macOS), "packs" one file
// per tile that clusterFinish merges (every platform). Zarr input; uint8 or uint16 (packs: uint16). float32 tiles are
// refused: ImarisWriter's float32 histograms depend on the order of the values (a single float32 volume: a series
// plan of it).

struct ClusterPlanOptions {
    std::string workdir;                     // created if needed; must not hold another plan
    std::string kind = "series";             // "series" or "tiles"
    // The inputs, found as the command line finds them (findInputFiles with convert.reader): folders and channel
    // patterns; or none, and convert.files lists them.
    std::vector<std::string> folders, patterns;
    // The converter's options: reader, files, voxel size, bounding box, block size, process block (series: each
    // task's -C), xOnImarisX (series) and the metadata; channel names default to the patterns. outputFile = the
    // finished file (default <workdir>/output.ims; the single file must be on the workdir's filesystem); numThreads is
    // not used (each task takes its own).
    ConvertOptions convert;
    // tiles only
    uint64_t tileSize[3] = {0, 0, 0};        // voxels per tile along X, Y, Z (each > 0)
    std::string store;                       // "mono" (default; "packs" on Windows) or "packs"
    long tilesPerTask = 1;                   // consecutive tiles of a level per task
    uint64_t chunkBytes = 0;                 // the chunk budget; 0: env PIW_CHUNK_BYTES, else 1 MiB (= the converter's)
};
struct ClusterStage {
    std::string name;                        // "convert", "level0", ...
    long tasks = 0;                          // indices 0 .. tasks-1
    std::string after;                       // the stage whose every task must be done first ("" = none)
};
struct ClusterPlan {
    std::string workdir, kind, store, outputFile;
    int timepoints = 0, channels = 0, levels = 0;
    uint64_t size[3] = {0, 0, 0};            // level 0, Imaris X, Y, Z
    DataType dataType = DataType::UInt16;
    std::vector<ClusterStage> stages;        // in the order they run
};
// Checks the options and every input (sample type, shape, bounding box: as convert() does) before anything is
// written, then writes <workdir>/manifest.json. A workdir that already holds this same plan is kept as it is.
PIW_API Status clusterPlan(const ClusterPlanOptions& options, ClusterPlan* plan = nullptr) noexcept;
// The plan of a workdir (clusterPlan's; setup's and monoinit's workdirs give their stages too).
PIW_API Status clusterReadPlan(const std::string& workdir, ClusterPlan& plan) noexcept;

struct ClusterTaskOptions {
    int numThreads = 0;                      // 0: defaultThreads()
    // series: where the part is written before it is placed. Empty: memory (/dev/shm) when the part fits (raw bytes
    // x 1.15 + 8 GB within 90% of the job's memory, SLURM_MEM_PER_NODE or SLURM_MEM_PER_CPU x SLURM_CPUS_PER_TASK,
    // else the machine's, and within the free space of /dev/shm; Linux), else <workdir>/stage.
    std::string stageDir;
    bool hasProcessBlock = false; uint64_t processBlock[3] = {0, 0, 0};   // series: -C instead of the plan's
    Callbacks callbacks;                     // series: the conversion's messages, progress and cancel
};
struct ClusterTaskResult {
    std::string stage; long index = 0;
    bool skipped = false;                    // it was already done: nothing was changed
    std::vector<std::string> owners;         // what it placed: P_C<c>_T<t>, or its tiles L<r>_C<c>_T<t>_<tile>
    // series
    int timepoint = -1, channel = -1;
    std::string part;                        // the staged part (removed)
    bool memoryStage = false;                // staged in /dev/shm
    double convertSeconds = 0, placeSeconds = 0;
    MonoPlaceResult place;
    // tiles: one timing line per tile
    std::vector<std::string> lines;
    double totalSeconds = 0;
};
// Runs task index of the stage (its name, or its number in the plan). Stages run in order: a task of level r fails
// while the level r-1 tiles it reads are not done. A task that is done is skipped; one that failed or was killed
// is simply run again (also at the same time in two processes: same bytes, same place).
PIW_API Status clusterTask(const std::string& workdir, const std::string& stage, long index,
                           const ClusterTaskOptions& options = ClusterTaskOptions(), ClusterTaskResult* result = nullptr) noexcept;

struct ClusterStageStatus {
    std::string name;
    long tasks = 0;
    std::vector<long> missing;               // task indices not done
};
struct ClusterStatus {
    std::string kind, store;
    std::vector<ClusterStageStatus> stages;
    bool sealed = false;                     // the single file is being finished: no more tasks
    bool finished = false;                   // ... and it was renamed to finishedFile
    std::string finishedFile;
};
// Which tasks are done (their commit points exist). Changes nothing.
PIW_API Status clusterStatus(const std::string& workdir, ClusterStatus& status) noexcept;

struct ClusterFinishOptions {
    bool verify = false;                     // single file: read and decode every chunk again
    int numThreads = 0;                      // 0: defaultThreads()
};
// The output (outputFile, else the plan's): NotFound, listing the missing tasks, while any task is not done; then the
// single file's monoFinish (result: its result), or the packs' merge (result: outputFile, timepoints, channels,
// levels, totalSeconds). Running it again after it finished does nothing (result->alreadyFinished).
PIW_API Status clusterFinish(const std::string& workdir, const std::string& outputFile,
                             const ClusterFinishOptions& options = ClusterFinishOptions(), MonoFinishResult* result = nullptr) noexcept;

namespace detail {
// src/parallel.cpp: tiles index .. index+count-1 of a level (done ones skipped), the packs merge
Status tileTask(const std::string& workdir, int level, long index, long count, int threads, std::vector<std::string>* lines) noexcept;
Status packsFinish(const std::string& workdir, const std::string& outputFile) noexcept;
}

}  // namespace piw

#endif
