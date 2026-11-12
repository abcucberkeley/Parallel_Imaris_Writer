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
1. All dependencies (cpp-tiff, cpp-zarr, the ImarisWriter, HDF5, LZ4, zlib, and nlohmann/json) are fetched automatically at configure time via CMake FetchContent, so an internet connection is required for the first configure. On Linux and Mac, libuuid is built from the tarball included with cpp-zarr.
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

The converter finds its inputs by matching a file-name pattern inside one or more folders. Each file whose name contains a pattern is treated as a timepoint, and multiple comma-separated patterns are treated as multiple channels. The result is written as `<output-directory>/<name>.ims`.

### Options
| Flag | Description |
| --- | --- |
| `-r` | Reader: `tiff` or `zarr` (default: `tiff`) |
| `-F` | Comma-separated input folder(s) to search |
| `-P` | Comma-separated file-name pattern(s), one per channel (files whose names contain a pattern are matched) |
| `-o` | Output directory (default: the first input folder) |
| `-n` | Output file name without extension (default: `output`, producing `output.ims`) |
| `-v` | Voxel size in µm (microns) as `X,Y,Z` (default: `0.108,0.108,0.108`) |
| `-c` | Number of channels (default: inferred from the number of patterns) |
| `-t` | Number of timepoints (default: inferred from the matching files) |
| `-b` | Imaris output block (chunk) size as `X,Y,Z` (default: the reader's native chunk size) |
| `-B` | Bounding box to crop the input as `Xmin,Ymin,Zmin,Xmax,Ymax,Zmax` |
| `-C` | Processing block size as `X,Y,Z` — the amount of a timepoint held in RAM at once. Zarr is blocked in all three axes; TIFF only in Z. Values larger than the image simply read the whole image. Omit to process a whole timepoint at once. |

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

For datasets too large or too numerous for one machine, the conversion can be split across many independent jobs (for example a SLURM job array) that together produce one Imaris dataset. The `parallelimariswriter` binary exposes a few extra **subcommands** for this; orchestrate them with whatever scheduler you use. This path takes **Zarr** input (one `.zarr` per channel per timepoint).

Why it needs structure: an `.ims` is a single HDF5 container with one shared chunk index per array, so multiple processes cannot write into one `.ims` concurrently without corrupting it. There are two ways to work within that, chosen by the shape of the data.

### Approach 1 — one job per (timepoint, channel)  *(recommended for time series)*

When each individual `(timepoint, channel)` volume fits in one node's memory (stream it with `-C`), convert each one independently and then stitch the results together:

1. **`setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz]`** — enumerates the `(channel, timepoint)` → `.zarr` mapping into `<workdir>/manifest.json`.
2. For each `(timepoint, channel)`, run the **normal converter** on that single `.zarr` into `<parts>/part_t{t}_c{c}.ims` (add `-C` to bound memory). Every one is independent, so run them all at once.
3. **`linkparts <workdir> <out.ims> <parts>`** — writes a tiny `.ims` of **HDF5 external links** into the parts; Imaris follows them and opens the whole set as one dataset.

The compressed data is written **once**, with no shared writer and no separate merge step. The output is a directory — the `<out.ims>` link file plus its `parts/` — so keep them together (links are stored as relative paths). Data-identical to converting all timepoints in a single process.

### Approach 2 — spatial tiling  *(for a single volume too big for one node)*

When even one `(timepoint, channel)` is too large for a node, tile it spatially. Work is divided by `(channel, timepoint, spatial tile)` at every pyramid level; because each coarser level downsamples the one below it, the levels run **in order**.

1. **`setup ... <cx,cy,cz> ...`** — plans into `manifest.json`; `jobsPerLevel[]` gives the worker count per level. Output chunk size comes from the `PIW_CHUNK_BYTES` env var (bytes, default 1 MiB).
2. **`worker <workdir> <level> <index>`** — builds one tile of one level (`index` runs `0..jobsPerLevel[level]-1`). Level 0 reads the Zarr; coarser levels downsample the finer level's tiles, so **level r+1 must run after level r** (e.g. via `--dependency=afterok`). Writes atomically (temp file + rename); safe to re-run.
3. Assemble the tiles into the `.ims`, either as one file or in parallel:
   - **`monitor <workdir> [out.ims]`** — one HDF5 writer reads all tiles and writes a single self-contained `.ims`.
   - or **`combineone <workdir> <t> <c> <out.ims>`** per `(timepoint, channel)` into its own `.ims`, then **`link <workdir> <out.ims> <parts>`** to stitch them with external links (same as `linkparts`, but sourced from the tiled chunk files).

**Memory:** the binding constraint is the **coarse levels**, not level 0 — a level>0 worker loads the finer level's region (up to ~8× its own tile) in order to downsample it, needing several times the level-0 footprint. On ~5000×22000×350 data a 2048³ tile fits in ~21 GB at every level; 3072³ OOMs level 1 even at 48 GB.

### Notes

- **Parallelize at the (timepoint, channel) level, not within one array.** A single self-contained `.ims` (`monitor`) is written by one HDF5 stream — the throughput ceiling for that path. Splitting one array across processes with MPI-IO does **not** help (HDF5's collective compressed-write coordination is worse than a single writer). External links (`link` / `linkparts`) instead give many independent writers with no shared index, so the merge parallelizes across the cluster and is then limited only by aggregate storage bandwidth — turning a multi-hour single-writer combine into a parallel step of minutes on a large multi-timepoint dataset.
- **Chunk size.** For the single-writer (`monitor`) path, fewer/bigger chunks cut per-chunk overhead — `PIW_CHUNK_BYTES=8388608` (8 MiB) noticeably speeds the combine, at some cost to Imaris interactive-read speed. The per-(timepoint,channel) approaches use the converter's default chunking.
- **Scope & output.** The `/Thumbnail` preview image is not generated (Imaris does not need it to open the file). External-link output is a directory that must stay together (the link `.ims` plus its `parts/`). The normal single-process converter (above) handles all supported readers and bit depths.

## Reference

Please cite our software if you find it useful in your work:

Xiongtao Ruan, Matthew Mueller, Gaoxiang Liu, Frederik Görlitz, Tian-Ming Fu, Daniel E. Milkie, Joshua L. Lillvis, Alexander Kuhn, Chu Yi Aaron Herr, Wilmene Hercule, Marc Nienhaus, Alison N. Killilea, Eric Betzig, Srigokul Upadhyayula. Image processing tools for petabyte-scale light sheet microscopy data. Nature Methods (2024). https://doi.org/10.1038/s41592-024-02475-4
