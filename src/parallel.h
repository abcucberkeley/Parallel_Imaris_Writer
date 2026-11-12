#ifndef PARALLEL_IMARIS_PARALLEL_H
#define PARALLEL_IMARIS_PARALLEL_H

// Cluster/distributed conversion subcommands. Many independent SLURM jobs read a zarr time series
// and produce Imaris .ims output whose voxel data is byte-identical to a monolithic run. Every
// subcommand prints its errors on stderr and exits non-zero on failure.
//   parallelimariswriter setup      <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz]
//        writes <workdir>/manifest.json; cx,cy,cz = per-worker tile size (voxels); voxel size
//        defaults to 0.108,0.108,0.108 (as the converter); env PIW_CHUNK_BYTES = chunk budget
//   parallelimariswriter worker     <workdir> <level> <index>
//        one tile at one level -> chunks/L*.pack + h_L*.hist; run level 0's array first, then 1's, ...
//   parallelimariswriter monitor    <workdir> [out.ims]
//        waits for every expected pack (env PIW_MONITOR_TIMEOUT = give up after N seconds), checks
//        them, then merges them into one .ims (default <workdir>/output.ims)
//   parallelimariswriter combineone <workdir> <t> <c> <part.ims>   -- one (timepoint, channel) from the packs
//   parallelimariswriter link       <workdir> <out.ims> <partsDir>  -- external links to combineone's parts
//   parallelimariswriter linkparts  <workdir> <out.ims> <partsDir>  -- external links to per-(t,c) converter outputs
//   parallelimariswriter layout     <X> <Y> <Z> [elemSize]          -- debug: print the pyramid
// partsDir is relative to out.ims and holds part_t<t>_c<c>.ims. The tile path (worker, monitor,
// combineone, link) takes uint16 data only; linkparts takes whatever the converter wrote.
// One self-contained .ims from per-(t,c) converter outputs, without links or parts: monoinit, monoplace,
// monofinish, monostatus (src/mono.h).

// True if argv[1] selects a parallel subcommand (vs the default monolithic conversion).
bool isParallelCommand(const char* cmd);
// Runs the selected parallel subcommand; returns its process exit code.
int runParallel(int argc, char** argv);

#endif
