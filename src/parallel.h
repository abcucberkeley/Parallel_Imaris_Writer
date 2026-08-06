#ifndef PARALLEL_IMARIS_PARALLEL_H
#define PARALLEL_IMARIS_PARALLEL_H

// Cluster/distributed conversion subcommands. Many independent SLURM jobs read a
// zarr time series and produce ONE Imaris .ims, byte-identical to a monolithic run:
//   parallelimariswriter setup   <folder> <pattern> <workdir> [vx,vy,vz]
//   parallelimariswriter worker  <workdir> <timepoint> <level> <index>
//   parallelimariswriter combine <workdir> <out.ims>
//   parallelimariswriter monitor <workdir> [out.ims]     (stateless: combine when all chunks present)

// True if argv[1] selects a parallel subcommand (vs the default monolithic conversion).
bool isParallelCommand(const char* cmd);
// Runs the selected parallel subcommand; returns its process exit code.
int runParallel(int argc, char** argv);

#endif
