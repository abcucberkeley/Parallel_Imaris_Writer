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
// partsDir is relative to out.ims and holds part_t<t>_c<c>.ims. The tile path (worker, monitor,
// combineone, link) takes uint16 data only; linkparts takes whatever the converter wrote.
// One self-contained .ims from per-(t,c) converter outputs, without links or parts: monoinit, monoplace,
// monofinish, monostatus (src/mono.h); monofinish and monostatus also take a mono-store tile workdir.

// True if argv[1] selects a parallel subcommand (vs the default monolithic conversion).
bool isParallelCommand(const char* cmd);
// Runs the selected parallel subcommand; returns its process exit code.
int runParallel(int argc, char** argv);

#endif
