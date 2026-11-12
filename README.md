# Parallel_Imaris_Writer
An efficient converter that writes TIFF and Zarr image stacks to the Imaris 5 (.ims) file format, utilizing cpp-tiff, cpp-zarr, the ImarisWriter, and OpenMP. The output is a single multi-resolution .ims file ready for visualization and analysis in Imaris.

## Limitations
1. Only 3D image stacks are supported (single or multiple channels, single or multiple timepoints). Reading inherits the cpp-tiff and cpp-zarr limitations, and color (RGB/RGBA) TIFFs are not supported.
2. 8, 16, and 32 bit data are supported; 64 bit data is not yet supported by the Imaris Converter.
3. Block processing (`-C`) can split a Zarr along all three axes, but a TIFF can only be split along Z (full XY slices) — a limitation of the TIFF format.

## Releases

Prebuilt release folders for Linux, Mac, and Windows are available from the [releases page](https://github.com/abcucberkeley/Parallel_Imaris_Writer/releases): download, unzip, and run the `parallelimariswriter` executable inside.

## CMake

The converter is built with the CMakeLists.txt file.

### Prerequisites
1. All dependencies (cpp-tiff, cpp-zarr, the ImarisWriter, HDF5, LZ4, zlib, and nlohmann/json) are fetched automatically at configure time via CMake FetchContent, so an internet connection is required for the first configure. On Linux and Mac, libuuid is built from the tarball included with cpp-zarr. A few ImarisWriter files are replaced by modified copies kept in `third_party/imariswriter/` (so that a failed write, for example on a full disk, ends the conversion with an error instead of hanging it); its README lists the changes.
2. Currently the only officially supported compiler is gcc on Linux and Mac and MinGW on Windows but others may work. On Mac, use Homebrew gcc (Apple Clang does not support OpenMP).

### Download and Install
````
git clone https://github.com/abcucberkeley/Parallel_Imaris_Writer
cd Parallel_Imaris_Writer
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
cmake --build build --target release
````
The `release` target assembles a self-contained folder, `build/Parallel_Imaris_Writer/`, containing the `parallelimariswriter` executable and its shared libraries, ready to run in place.

## Usage

The converter finds its inputs by matching a file-name pattern inside one or more folders. Each file whose name contains a pattern is treated as a timepoint, and multiple comma-separated patterns are treated as multiple channels. The timepoints of a channel are ordered by name (by full path, folders in name order too), so number them with leading zeros (`t0009`, `t0010`). Only the reader's files are used: `.tif` and `.tiff` (any case) for `tiff`, `.zarr` for `zarr`. Every channel must have the same number of files, and a file may match only one pattern. The result is written as `<output-directory>/<name>.ims`; the inputs are also listed in `<output-directory>/timepoints.txt` (appended to on every run).

### Options
| Flag | Description |
| --- | --- |
| `-r` | Reader: `tiff` or `zarr` (default: `tiff`) |
| `-F` | Comma-separated input folder(s) to search |
| `-P` | Comma-separated file-name pattern(s), one per channel (files whose names contain a pattern are matched) |
| `-o` | Output directory, created (with any missing parent folders) if needed (default: the first input folder) |
| `-n` | Output file name without extension (default: `output`, producing `output.ims`) |
| `-v` | Voxel size in µm (microns) as `X,Y,Z`, each greater than 0 (default: `0.108,0.108,0.108`) |
| `-c` | Number of channels; if given, it must equal the number of patterns |
| `-t` | Number of timepoints; if given, it must equal the number of files per pattern |
| `-b` | Size of the blocks handed to the ImarisWriter as `X,Y,Z`, whole numbers of at least 1 (default: the reader's native chunk size). It does not change the output: the HDF5 chunks are chosen by the ImarisWriter. |
| `-B` | Bounding box to crop the input as `Xmin,Ymin,Zmin,Xmax,Ymax,Zmax`: whole numbers, 0-based, the max excluded |
| `-C` | Processing block size as `X,Y,Z` (whole numbers of at least 1) — the amount of a timepoint held in RAM at once. Zarr is blocked in all three axes; TIFF only in Z. Values larger than the image simply read the whole image. Omit to process a whole timepoint at once (the whole timepoint is then held in RAM). |
| `--channel-names` | Comma-separated channel names (default: the `-P` patterns, e.g. `CamA,CamB`) |
| `--colors` | Comma-separated channel colors: `red`, `green`, `blue`, `cyan`, `magenta`, `yellow`, `white`, `gray`, a hex `RRGGBB`, or `r:g:b` with values from 0 to 1 (default: red, green, blue, red, ...) |
| `--emission` | Comma-separated emission wavelengths in nm, written as `LSMEmissionWavelength` (`0` = none for that channel; default: none) |
| `--excitation` | Comma-separated excitation wavelengths in nm, written as `LSMExcitationWavelength` (`0` = none; default: none) |
| `--time-interval` | Seconds between timepoints |
| `--start-time` | Time of the first timepoint, `"YYYY-MM-DD HH:MM:SS[.mmm]"` (local time) |
| `-h`, `--help` | Print the options and exit |

A list of numbers may end with one comma (`-v 0.108,0.108,0.25,`). A leading `~` or `~/` in `-F` and `-o` is replaced by the home folder (Linux and Mac).

#### Exit codes
`0` when the `.ims` was written. `1` on any error — an unknown option or a missing or malformed value, no arguments at all, an unsupported reader, a missing input folder, a pattern without a match, channels with different numbers of files, an output folder that cannot be created or written, unsupported data (such as 64-bit), or a failed read or write — after one `ERROR: <text>` line on stderr (a partly written output file is removed).

#### Metadata
The channels are named after their patterns and colored red, green, blue, red, ... unless `--channel-names` and `--colors` say otherwise; wavelengths are written only when given. The file's Creator is `parallelimariswriter` with its version. Timepoint `t` is placed at:
1. `start + t × interval` when `--time-interval` is given; otherwise
2. `start + (stamp[t] − stamp[0])` milliseconds when every file of a channel has a `<n>msecAbs` stamp in its name (as PetaKit5D names its files, e.g. `..._0000123msec_0012345678msecAbs.zarr`; the first such channel is used); otherwise
3. `start + t` seconds.

`start` is `--start-time`, or else the modification time (local time, whole seconds) of the first input file — of the first channel's first timepoint, a `.zarr` folder's own modification time for Zarr input. Imaris shows these times and uses their spacing as the frame interval.

### Examples

Convert a folder of TIFF stacks (files containing "CamA") into `output.ims`:
````
parallelimariswriter -r tiff -F /path/to/tiffs -P CamA -o /path/to/out -v 0.108,0.108,0.2
````

Convert Zarr stacks:
````
parallelimariswriter -r zarr -F /path/to/zarrs -P CamA -o /path/to/out -v 0.108,0.108,0.2
````

Two channels (files containing "CamA" and "CamB"):
````
parallelimariswriter -r zarr -F /path/to/data -P CamA,CamB -o /path/to/out
````

Bounded memory for a large image — process 512 x 512 x 256 voxels at a time:
````
parallelimariswriter -r zarr -F /path/to/zarrs -P CamA -o /path/to/out -C 512,512,256
````

## Cluster (distributed) conversion

For datasets too large or too numerous for one machine, the conversion can be split across many independent jobs (for example a SLURM job array) that together produce **one self-contained `.ims` file**: no link file, no `parts/` directory, and the same data, histograms and metadata as converting everything in a single process. The `parallelimariswriter` binary exposes a few extra **subcommands** for this; orchestrate them with whatever scheduler you use.

**How a single file is written in parallel.** Each job compresses its chunks exactly as the single-node converter does and writes the compressed bytes once, straight into the final file, at a byte range it claims in a small journal next to the file (claims never overlap, so jobs need no other coordination). It also leaves a small sidecar index of where each chunk went, plus exact histograms. A short serial `monofinish` step then builds the HDF5 structure in that same file — every chunk index, the histograms, the dataset metadata and the thumbnail — pointing at the chunks already on disk, and checks the result with the stock HDF5 library before renaming the file into place. The output is an ordinary HDF5 1.8-format `.ims` with real chunks.

Requirements and limits: the workdir and the output must be on one shared POSIX file system that every job can write (Linux or macOS; the single-file subcommands are not available on Windows); the single-file path takes uint16 data (other types: use the link option below). Jobs write with `O_DIRECT` and fall back to buffered writes plus a sync where the file system refuses it.

### Time series: one job per (timepoint, channel)  *(recommended)*

When each `(timepoint, channel)` volume can be converted by one node (stream it with `-C`), convert each one independently and move it into the single file:

1. **`setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--channel-names ... --start-time ...]`** — finds the `(channel, timepoint)` → `.zarr` inputs and writes `<workdir>/manifest.json`, with the metadata (channel names, colors, wavelengths, timepoint times) the converter would write for the same inputs and options. It takes the converter's metadata options (see [Metadata](#metadata)); every output made from the workdir carries that metadata. For any other input (TIFF, cropped with `-B`, ...) write the manifest with **`monoinit <workdir> <nT> <nC> <vx,vy,vz> [out.ims]`** instead.
2. For each `(timepoint, channel)` — all at once, e.g. one array task each:
   - run the **normal converter** on that one volume into a part file (`-c 1 -t 1`, same options for every part);
   - **`monoplace <workdir> <t> <c> <part.ims> [--threads N]`** copies the part's compressed chunks into the single file and writes its sidecar (the task's commit point);
   - delete the part.
3. **`monofinish <workdir> [out.ims] [--verify] [--threads N]`** — once every part is placed: builds the index and the metadata and renames the file to `out.ims` (default `<workdir>/output.ims`; it must be on the same file system as the workdir). `--verify` also decodes every chunk of the finished file again and checks it against the parts' histograms (this reads the whole file).

**`monostatus <workdir>`** lists the placed and the missing parts (exit code 3 while any is missing).

Example array task (SLURM; `<...>` are your own values):
````
t=$((SLURM_ARRAY_TASK_ID / 2)); c=$((SLURM_ARRAY_TASK_ID % 2))
stage=/dev/shm/piw_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}
mkdir -p $stage; trap 'rm -rf $stage' EXIT
run(){ "$@" & pid=$!; wait $pid; }; trap 'kill $pid; wait $pid; exit 143' TERM   # see "Staging"
run parallelimariswriter -r zarr -F /path/to/zarrs -P <name of the (t,c) zarr> -c 1 -t 1 -v 0.108,0.108,0.25 -C 2048,2048,512 -o $stage -n part || exit 1
run parallelimariswriter monoplace /path/to/workdir $t $c $stage/part.ims --threads $SLURM_CPUS_PER_TASK
````
and after the whole array (e.g. `--dependency=afterany:<array job>`):
````
parallelimariswriter monofinish /path/to/workdir /path/to/out.ims --verify --threads 16
````

**Staging.** A part only lives until it is placed, so write it to fast node-local memory (`/dev/shm`) when it fits — roughly the raw size of the volume plus a few GB, within the job's memory limit (on Linux files in `/dev/shm` count toward it) — and otherwise to a scratch directory on the shared file system. Remove the stage directory when the task ends, also when it is cancelled (`trap`). SLURM's `scancel` sends `SIGTERM` to the batch shell only, and a shell runs its trap only after the running command has finished, so the converter would go on until it is killed with the whole job, and the trap would never run: run each step in the background and `wait` for it, with a `TERM` trap that stops it (as above). A task killed outright (out of memory, `SIGKILL`) may leave its files in `/dev/shm` until they are removed. Do not stage on slow local disks.

**Restarting.** A failed, cancelled or killed task is simply run again: the same part claims the same byte range and writes the same bytes there, and a part whose sidecar exists can be skipped. `monofinish` refuses (exit code 3) and names the missing parts while any is missing, so run it after the array whatever the tasks' outcome, resubmit exactly the missing `(t,c)` (task index `t*nC + c` above), and run `monofinish` again. Once `monofinish` has started, the journal is sealed and later `monoplace` calls fail instead of changing the file. Until it has renamed the file, `monofinish` can be run again.

**Claims.** By default a claim takes a numbered journal record with `link()`, which is atomic on NFS servers too. Set `PIW_CLAIM=fcntl` in the jobs to use one whole-file `fcntl` lock instead (only where `fcntl` locks work across nodes; it is refused on Linux mounts whose locks are node-local). A run records the mechanism its first job used, and every later job uses that one.

**Keeping the parts instead (link option).** To keep one file per `(timepoint, channel)`, convert the parts into `<workdir>/parts/part_t{t}_c{c}.ims` and run **`linkparts <workdir> <out.ims> parts`**: it writes a small `.ims` of **HDF5 external links** into the parts that Imaris opens as one dataset. The output is then a directory — the `<out.ims>` link file plus its `parts/` (relative links) — that must stay together. It takes every bit depth the converter writes (the `/Thumbnail` only for uint16). An existing link + parts output can be turned into one file later: `monoinit`, `monoplace` on each part, then `monofinish`.

### Single huge volume: spatial tiling

When even one `(timepoint, channel)` is too large for a node, tile it spatially. Work is divided by `(channel, timepoint, spatial tile)` at every pyramid level; because each coarser level downsamples the one below it, the levels run **in order**.

1. **`setup ... <cx,cy,cz> ...`** — plans into `manifest.json`; `jobsPerLevel[]` gives the worker count per level. Output chunk size comes from the `PIW_CHUNK_BYTES` env var (bytes, default 1 MiB, which matches the single-node converter's chunking).
2. **`worker <workdir> <level> <index>`** — builds one tile of one level (`index` runs `0..jobsPerLevel[level]-1`). Level 0 reads the Zarr; coarser levels downsample the finer level's tiles, so **level r+1 must run after level r** (e.g. via `--dependency=afterok`). Writes atomically (temp file + rename); safe to re-run.
3. Assemble the tiles into the `.ims`, either as one file or in parallel:
   - **`monitor <workdir> [out.ims]`** — one HDF5 writer reads all tiles and writes a single self-contained `.ims` (one write stream: the slowest step for large data).
   - or **`combineone <workdir> <t> <c> <out.ims>`** per `(timepoint, channel)` into its own `.ims`, then **`link <workdir> <out.ims> <parts>`** to stitch them with external links (same as `linkparts`, but sourced from the tiled chunk files).

**Memory:** the binding constraint is the **coarse levels**, not level 0 — a level>0 worker loads the finer level's region (up to ~8× its own tile) in order to downsample it, needing several times the level-0 footprint. On ~5000×22000×350 data a 2048³ tile fits in ~21 GB at every level; 3072³ OOMs level 1 even at 48 GB.

### Notes

- **Throughput.** The parallel steps are limited by the shared file system's write bandwidth into one file; `monofinish` is serial but writes only metadata (minutes even for millions of chunks). Splitting one HDF5 array across processes with MPI-IO does **not** help (HDF5's collective compressed writes are slower than a single writer).
- **Chunk size.** Keep the converter's default chunking for the per-(timepoint, channel) path; for the tiled `monitor` path, `PIW_CHUNK_BYTES=8388608` (8 MiB) speeds the single-writer combine at some cost to Imaris interactive-read speed, and changes the chunking from the single-node converter's.
- **Thumbnail and metadata.** All cluster outputs carry the same `/Thumbnail` preview image as the single-node converter. The single-file output's `ColorRange` comes from the histograms of every timepoint, as in a single-node conversion; the `linkparts` output takes each channel's range from its timepoint-0 part. The normal single-process converter (above) handles all supported readers and bit depths.

## Reference

Please cite our software if you find it useful in your work:

Xiongtao Ruan, Matthew Mueller, Gaoxiang Liu, Frederik Görlitz, Tian-Ming Fu, Daniel E. Milkie, Joshua L. Lillvis, Alexander Kuhn, Chu Yi Aaron Herr, Wilmene Hercule, Marc Nienhaus, Alison N. Killilea, Eric Betzig, Srigokul Upadhyayula. Image processing tools for petabyte-scale light sheet microscopy data. Nature Methods (2024). https://doi.org/10.1038/s41592-024-02475-4
