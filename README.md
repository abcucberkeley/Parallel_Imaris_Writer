# Parallel_Imaris_Writer
[![Tests](https://github.com/abcucberkeley/Parallel_Imaris_Writer/actions/workflows/tests.yml/badge.svg)](https://github.com/abcucberkeley/Parallel_Imaris_Writer/actions/workflows/tests.yml)

An efficient converter that writes TIFF and Zarr image stacks to the Imaris 5 (.ims) file format, utilizing cpp-tiff, cpp-zarr, the ImarisWriter, and OpenMP. The output is a single multi-resolution .ims file ready for visualization and analysis in Imaris.

## Limitations
1. Only 3D image stacks are supported (single or multiple channels, single or multiple timepoints). Reading inherits the cpp-tiff and cpp-zarr limitations, and color (RGB/RGBA) TIFFs are not supported.
2. Supported sample types: unsigned 8-, 16- and 32-bit integers (`uint8`, `uint16`, `uint32`) and 32-bit floats (`float32`), one sample per pixel. The type is the file's own (a TIFF's BitsPerSample and SampleFormat, a Zarr's dtype) and the `.ims` holds the same type. Signed integers, 16- and 64-bit floats, 64-bit integers, booleans, complex numbers and TIFFs with several samples per pixel are refused with an error before any output is written, and every input must have the type of the first.
3. Block processing (`-C`) can split a Zarr along all three axes, but a TIFF can only be split along Z (full XY slices) — a limitation of the TIFF format.

## Releases

Prebuilt release folders for Linux, Mac, and Windows are available from the [releases page](https://github.com/abcucberkeley/Parallel_Imaris_Writer/releases): download, unzip, and run the `parallelimariswriter` executable inside.

## Python

A Python package, `parallel_imaris_writer`, writes NumPy arrays to `.ims` files, converts TIFF and Zarr files as the command line does, reads `.ims` files back, and runs the [cluster steps](#cluster-steps-from-python). It is the same C++ code as the command line, and with the same inputs and options its files equal the command line's.

### Prerequisites

#### Python
1. Python version 3.9-3.14 (3.14 free-threaded too on Linux and Mac)
2. NumPy (installed with the package)

#### OS
Linux (x86_64): distributions with glibc 2.28 or newer (manylinux_2_28)

Mac Apple Silicon (M1, M2, etc.): wheels built by GitHub Actions need macOS 15 or newer, because the OpenMP runtime bundled into the wheel (Homebrew's gcc@13 libgomp on the build machine) sets the wheel's macOS version; wheels built on a macOS 13 machine install on macOS 13 and newer

Windows (x86_64): Windows 10 or newer

### Installation

The package is not on PyPI yet. Until it is, install a wheel built by the Wheels workflow, or build one from source.

#### A wheel from the Wheels workflow
The [Wheels workflow](https://github.com/abcucberkeley/Parallel_Imaris_Writer/actions/workflows/wheels.yml) (run by hand, or on a `v*` tag) builds and tests wheels for every supported Python. Open a run, download the artifact of your system (`wheels-Linux`, `wheels-macOS` or `wheels-Windows`), unzip it, and install the wheel of your Python, e.g.
````
pip install parallel_imaris_writer-<version>-cp312-cp312-<platform>.whl
````

#### Build from source
The wheel is built with CMake (fetched by pip on Linux and Mac); the dependencies are downloaded at the first build, as for the [CMake](#cmake) build. Compilers: gcc on Linux; Homebrew's gcc-13 on Mac (`brew install gcc@13`); on Windows MSYS2's UCRT64 environment (`pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-libgomp mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-ninja`) with its `ucrt64\bin` folder first on PATH. The simplest way is cibuildwheel, as the Wheels workflow runs it (on Linux it needs Docker or Podman; set `CIBW_BUILD`, e.g. `cp312-*`, to build fewer wheels):
````
git clone https://github.com/abcucberkeley/Parallel_Imaris_Writer
cd Parallel_Imaris_Writer
pip install cibuildwheel
cibuildwheel python --output-dir wheelhouse
pip install wheelhouse/parallel_imaris_writer-<version>-cp312-cp312-<platform>.whl
````
Or with pip in `python/`: `pip wheel . --no-deps -w dist` (on Mac with `CC=gcc-13 CXX=g++-13`), then bundle the library into the wheel with `auditwheel repair` (Linux), `delocate-wheel` (Mac) or `delvewheel repair` (Windows), pointing them at `python/build/install/lib` (`python/build/install/bin` on Windows), and install the repaired wheel. `python/tools/build_manylinux_wheels.sh` does all of this for Linux inside a manylinux container. A plain `pip install .` does not bundle the library into the package.

#### From PyPI (once it is there)
````
pip install parallel-imaris-writer
````

The package also installs the command line as the `parallelimariswriter` command (the same as `python -m parallel_imaris_writer`).

### Conventions

**Types.** uint8, uint16, uint32 (checked with Imaris' own reader) and float32. Arrays of any other type raise `TypeError` (convert them with `astype` first); input files of another type raise `ValueError` before anything is written.

**Axes.** `order` names the axes of arrays and of every per-axis value; it does not ask for a memory layout (any layout is read in place and means the same). By default **Imaris X is the image's y**, Imaris Y its x and Imaris Z its z, as in every file the command line and PetaKit5D's MATLAB wrapper write; `x_on_imaris_x=True` (on `write_ims`, `convert_to_ims`, `convert_dataset` and `cluster_plan`) puts the image's x on Imaris X instead. Read such a file with `x_on_imaris_x=True` too (`read_ims`, `ims_info`).

| | `order='C'` (default) | `order='F'` |
| --- | --- | --- |
| 3D array | (z, y, x) | (y, x, z) |
| 4D array | (c, z, y, x) | (y, x, z, c) |
| 5D array | (t, c, z, y, x) | (y, x, z, c, t) |
| `voxel_size`, `block_size`, `process_block_size`, `tile_size`, `start_coords`, `end_coords` | (z, y, x) | (y, x, z) |
| `input_bbox` (0-based, end excluded) | (z0, y0, x0, z1, y1, x1) | (y0, x0, z0, y1, x1, z1) |
| Imaris X, Y, Z | y, x, z | y, x, z |
| Imaris X, Y, Z with `x_on_imaris_x=True` | x, y, z | x, y, z |
| what it matches | NumPy (z, y, x) stacks, e.g. `cpptiff.read_tiff` | MATLAB arrays |

A 2D array is one (y, x) plane in either order. `voxel_size` and the other per-axis values can also be a mapping with the keys `'x'`, `'y'` and `'z'`.

**The axes of input files** (`input_axes` of `convert_to_ims`, `convert_dataset` and `cluster_plan`): `'yxz'` is MATLAB's layout, which every TIFF has (rows, columns, pages) and PetaKit5D's Zarr files have; `'zyx'` is a Zarr of a (z, y, x) array. By default it is `'yxz'` for TIFF files and is decided from each Zarr's metadata: a Zarr v2 of order `F`, or a Zarr v3 whose transpose codec reverses the axes, is MATLAB's (y, x, z); order `C` is (z, y, x). Pitfall: `cppzarr.write_zarr` writes Zarr v2 files in F order by default, so a (z, y, x) NumPy array saved with its defaults is read as (y, x, z): pass `input_axes='zyx'`, or write it with `order='C'`. The command line has no such option: it takes every input as (y, x, z).

**Behaviour.**
- Calls in one process run one at a time (a call from a second thread waits for the first); to convert several files at once, use processes (below).
- The GIL is released while a call runs, and Ctrl-C stops it with `KeyboardInterrupt` (but not `run_cli`, and a tile `cluster_task` finishes its tiles first). `num_threads` sets the threads of that call only (default: `OMP_NUM_THREADS`, or every CPU the process may use).
- A file appears under its name only when it is complete: it is written as `<stem>_<8 hex digits>.ims` next to it and renamed at the end, replacing an existing file. A failed or stopped call leaves nothing; if only the final rename fails (on Windows, for example, while the old file is open in Imaris), the complete file is kept under its temporary name and the `OSError` names it. (The command line does the same with `<name>.ims.partial`: see its Usage section below.)
- `progress(fraction, bytes_written)` is called on the calling thread at most about ten times a second and once with 1.0 at the end; an exception it raises stops the call.

### Usage

#### Write an array
````
import numpy as np
import parallel_imaris_writer as piw
im = np.random.randint(0, 4096, size=(64, 512, 512), dtype=np.uint16)   # (z, y, x)
piw.write_ims('volume.ims', im, voxel_size=(0.25, 0.108, 0.108))   # (dz, dy, dx) in microns
````

#### Several channels and time points
````
ch0 = np.random.randint(0, 4096, size=(64, 512, 512), dtype=np.uint16)
ch1 = np.random.randint(0, 4096, size=(64, 512, 512), dtype=np.uint16)
# A list of channels, or a (c, z, y, x) array
piw.write_ims('channels.ims', [ch0, ch1], channel_names=['CamA', 'CamB'], channel_colors=[(0, 1, 0), (1, 0, 1)])
# data[timepoint][channel], or a (t, c, z, y, x) array
piw.write_ims('series.ims', [[ch0, ch1], [ch1, ch0]], time_interval=2.5, start_time='2026-01-01 12:00:00')
piw.write_ims('series5d.ims', np.stack([np.stack([ch0, ch1]), np.stack([ch1, ch0])]), time_interval=2.5,
              start_time='2026-01-01 12:00:00')
````

#### MATLAB's (y, x, z) layout
````
yxz = np.transpose(im, (1, 2, 0))   # (y, x, z), as MATLAB holds it
piw.write_ims('matlab.ims', yxz, order='F', voxel_size=(0.108, 0.108, 0.25))   # (dy, dx, dz)
# the same image as volume.ims: the same data (and, given the same start_time, the same file)
````

#### Put x on Imaris X
````
piw.write_ims('x_on_x.ims', im, x_on_imaris_x=True)
back = piw.read_ims('x_on_x.ims', x_on_imaris_x=True)   # (z, y, x) again
````

#### Convert TIFF or Zarr files
The inputs are found as the command line's `-F` and `-P` find them (each pattern one channel, its files the time points in name order), or given as a list. The reader comes from the file names unless `reader='tiff'` or `'zarr'` is given.
````
# Every file in data/ whose name contains CamA (channel 0) or CamB (channel 1)
piw.convert_to_ims('converted.ims', folders='data', channel_patterns=['CamA', 'CamB'],
                   voxel_size=(0.25, 0.108, 0.108))
# The same inputs as a list: files[channel][timepoint]
files = piw.find_input_files('data', ['CamA', 'CamB'])
piw.convert_to_ims('listed.ims', files=files, voxel_size=(0.25, 0.108, 0.108))
# Crop every input, 0-based with the end excluded: (z0, y0, x0, z1, y1, x1)
piw.convert_to_ims('cropped.ims', files=files, input_bbox=(0, 100, 100, 32, 300, 300))
````

Zarr files of (z, y, x) arrays:
````
import cppzarr
cppzarr.write_zarr('zyx_c.zarr', im, order='C')   # C order: read as (z, y, x)
piw.convert_to_ims('zyx_c.ims', files='zyx_c.zarr')
cppzarr.write_zarr('zyx_f.zarr', im)              # F order by default: say what the axes are
piw.convert_to_ims('zyx_f.ims', files='zyx_f.zarr', input_axes='zyx')
````

As PetaKit5D's MATLAB dataset wrapper: one `.ims` per dataset folder, in its `imaris` folder, named after the first input file (an existing one is kept unless `overwrite=True`); it returns the paths:
````
outputs = piw.convert_dataset('data', channel_patterns=['CamA', 'CamB'], zarr_file=True,
                              voxel_size=(0.25, 0.108, 0.108))
````

#### Bounded memory, threads and progress
````
def report(fraction, bytes_written):
    print(f'{fraction:.0%}, {bytes_written / 1e6:.1f} MB written')

# Hold at most 32 x 256 x 256 voxels of a time point at once (TIFF: whole planes), with 8 threads
piw.convert_to_ims('bounded.ims', folders='data', channel_patterns=['CamA', 'CamB'],
                   process_block_size=(32, 256, 256), num_threads=8, progress=report)
````

#### Several conversions at once
Calls in one process run one at a time, so convert several files at once in processes, each with its share of the CPUs:
````
from concurrent.futures import ProcessPoolExecutor
import parallel_imaris_writer as piw

def convert(pattern):
    piw.convert_to_ims(f'{pattern}.ims', folders='data', channel_patterns=pattern, num_threads=4)

if __name__ == '__main__':
    with ProcessPoolExecutor(2) as pool:
        list(pool.map(convert, ['CamA', 'CamB']))
````

#### Read back and compare
````
whole = piw.read_ims('volume.ims')                                          # (z, y, x)
region = piw.read_ims('volume.ims', start_coords=(0, 0, 0), end_coords=(16, 128, 128))
coarse = piw.read_ims('series.ims', resolution_level=1, timepoint=1, channel=0)
print(np.array_equal(whole, im))                                           # True

info = piw.ims_info('volume.ims')   # shape, dtype, channels, timepoints, resolution_levels, voxel_size, ...
print(info['shape'], info['dtype'], info['resolution_levels'], info['voxel_size'])

# Two files hold the same data when the CRCs of their datasets are equal
a = piw.ims_info('volume.ims', checksums=True)
b = piw.ims_info('matlab.ims', checksums=True)
print([d['crc'] for d in a['datasets']] == [d['crc'] for d in b['datasets']])   # True
````
`ims_info` also returns every attribute of the file (`info['attributes']`); with the same `start_time` two such files have the same attributes too.

#### The command line
````
import subprocess, sys
subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-r', 'zarr', '-F', 'data', '-P', 'CamA,CamB',
                '-o', 'out', '-n', 'cli'], check=True)
# or in this process: the exit code
code = piw.run_cli(['verify', 'out/cli.ims'])
````

#### Cluster steps from Python
The [cluster steps](#plan-tasks-finish-any-job-system) are one function each: `cluster_plan` plans the conversion as stages of independent tasks, `cluster_task` runs one task (a single-node call: run each in a job of your own job system, in any process on any node, each stage after the one before), `cluster_status` says which tasks are not done, and `cluster_finish` writes the file. The functions never talk to Slurm or any other scheduler. A failed task raises and is simply run again; Ctrl-C stops a time-series task between blocks (a tile task finishes its tiles first). The time-series plan (the default) runs on Linux and Mac.
````
plan = piw.cluster_plan('workdir', folders='data', channel_patterns=['CamA', 'CamB'], file_name='cluster.ims',
                        voxel_size=(0.25, 0.108, 0.108))
for stage in plan['stages']:            # in order: each stage after the one before
    for index in range(stage['tasks']):     # in your job system: one job (or array task) each
        piw.cluster_task('workdir', stage['name'], index, num_threads=8)
print(piw.cluster_status('workdir')['missing'])   # 0
piw.cluster_finish('workdir')
````
`cluster.ims` equals `convert_to_ims` of the same inputs and options. With `tile_size` the plan cuts each volume into tiles instead (Zarr input in MATLAB's layout, uint8 or uint16), in stages `level0`, `level1`, ...

## CMake

The converter is built with the CMakeLists.txt file.

### Prerequisites
1. All dependencies (cpp-tiff, cpp-zarr, the ImarisWriter, HDF5, LZ4, zlib, and nlohmann/json) are fetched automatically at configure time via CMake FetchContent, so an internet connection is required for the first configure. On Linux and Mac, libuuid is built from the tarball included with cpp-zarr. A few ImarisWriter files are replaced by modified copies kept in `third_party/imariswriter/` (so that a failed write, for example on a full disk, ends the conversion with an error instead of hanging it, and a conversion can be stopped safely at any point); its README lists the changes.
2. Currently the only officially supported compiler is gcc on Linux and Mac and MinGW on Windows but others may work. On Mac, use Homebrew gcc (Apple Clang does not support OpenMP).

### Download and Install
````
git clone https://github.com/abcucberkeley/Parallel_Imaris_Writer
cd Parallel_Imaris_Writer
cmake -S . -B build
cmake --build build -j
````
The build type is `Release` unless `-DCMAKE_BUILD_TYPE` says otherwise, and `-DCMAKE_CXX_FLAGS=...` / `-DCMAKE_C_FLAGS=...` reach everything that is built, the fetched dependencies included.

Every build also assembles the self-contained folder that is shipped, `build/Parallel_Imaris_Writer/` (the `release` target, part of the default build): the `parallelimariswriter` executable, `libcppTiff` and `libcppZarr`, and on Linux and Mac the OpenMP runtime (`libgomp`), ready to run in place and to be copied anywhere. `cmake --install build --prefix <dir>` installs the same files into `<dir>/bin`. In that folder:
- the executable links its own C++ runtime, LZ4 and zlib statically (on Linux it uses the system's `libgcc_s`), and the `libcppTiff` and `libcppZarr` built with it keep theirs private (the `runtimeIsolation` test checks this);
- the binaries find each other through an RPATH of exactly `$ORIGIN` (`@loader_path` on Mac), whatever library folders the compiler added at link time;
- in a `Release` (or `MinSizeRel`) build they are stripped (no static symbol table or debug information; the dynamic symbols stay), and no file records the source or build folder: everything is compiled with `-ffile-prefix-map` (the `releaseFolder` test checks both).

All C and C++ code, the dependencies included, is compiled with `-ffp-contract=off`, so that arm64 and x86-64 builds compute the same display ranges and thumbnails.

### Tests
Configure with `-DPIW_BUILD_TESTS=ON`, build, then run `ctest --test-dir build --output-on-failure`; the tests generate their own input data and run the release folder's files. For testing only, `-DPIW_SANITIZE=address` (AddressSanitizer and LeakSanitizer) or `-DPIW_SANITIZE=thread` (ThreadSanitizer) builds everything, the fetched dependencies included, with that sanitizer; use a fresh build directory and `-DCMAKE_BUILD_TYPE=RelWithDebInfo`. ThreadSanitizer reports races inside OpenMP parallel regions that are not real (GCC's OpenMP runtime is not instrumented, so its barriers are invisible to it).

## Usage

The converter finds its inputs by matching a file-name pattern inside one or more folders. Each file whose name contains a pattern is treated as a timepoint, and multiple comma-separated patterns are treated as multiple channels. The timepoints of a channel are ordered by name (by full path, folders in name order too), so number them with leading zeros (`t0009`, `t0010`). Only the reader's files are used: `.tif` and `.tiff` (any case) for `tiff`, `.zarr` for `zarr`. Every channel must have the same number of files, and a file may match only one pattern. The result is written as `<output-directory>/<name>.ims`; the inputs are also listed in `<output-directory>/timepoints.txt` (appended to on every run).

**A file named `<name>.ims` is always complete.** The converter writes `<name>.ims.partial` next to it, checks the closed file (as `verify` does, below) and only then renames it to `<name>.ims`, replacing an existing file of that name. After any error the partial file is removed and an existing `<name>.ims` is left as it was (except when only the final rename fails, e.g. because the old `<name>.ims` is open in another program on Windows: then the complete file is kept as `<name>.ims.partial` and the error says so); a run that is killed (or loses its node) leaves only `<name>.ims.partial`, which the next run overwrites. Scripts that wait for the output can therefore wait for `<name>.ims` to appear, and `--no-overwrite` skips outputs that are already done (a file written by an earlier version of the converter may be incomplete; `verify` tells). The library (`piw::convert`, `piw::writeVolumes`) and the cluster tasks write their files the same way.

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
| `-B` | Bounding box to crop the input as `Xmin,Ymin,Zmin,Xmax,Ymax,Zmax`: whole numbers, 0-based, the max excluded, for both readers. Along every axis `0 ≤ min < max ≤ size` of every input; the inputs may then differ in size (without `-B` they must all have the same size). |
| `-C` | Processing block size as `X,Y,Z` (whole numbers of at least 1) — the amount of a timepoint held in RAM at once. Zarr is blocked in all three axes; TIFF only in Z. Values larger than the image simply read the whole image. Omit to process a whole timepoint at once (the whole timepoint is then held in RAM). |
| `--channel-names` | Comma-separated channel names (default: the `-P` patterns, e.g. `CamA,CamB`) |
| `--colors` | Comma-separated channel colors: `red`, `green`, `blue`, `cyan`, `magenta`, `yellow`, `white`, `gray`, a hex `RRGGBB`, or `r:g:b` with values from 0 to 1 (default: red, green, blue, red, ...) |
| `--emission` | Comma-separated emission wavelengths in nm, written as `LSMEmissionWavelength` (`0` = none for that channel; default: none) |
| `--excitation` | Comma-separated excitation wavelengths in nm, written as `LSMExcitationWavelength` (`0` = none; default: none) |
| `--time-interval` | Seconds between timepoints |
| `--start-time` | Time of the first timepoint, `"YYYY-MM-DD HH:MM:SS[.mmm]"` (local time) |
| `--no-overwrite` | If `<name>.ims` exists, print a line and exit 0 without converting (such a file is complete, see above) |
| `--no-verify` | Skip the check of the written file before it is renamed (the check reads only metadata: well under a second for a 43 GB file) |
| `-h`, `--help` | Print the options and exit |

A list of numbers may end with one comma (`-v 0.108,0.108,0.25,`). A leading `~` or `~/` in `-F` and `-o` is replaced by the home folder (Linux and Mac).

#### Checking a file: `verify`
`parallelimariswriter verify <file.ims> [<file.ims> ...]` checks that each file is a complete `.ims`, from its metadata alone (no image data is read, so it takes seconds even for a very large file): `/DataSetInfo/Image` `X`, `Y`, `Z` give the image size, `/DataSetInfo/TimeInfo` `FileTimePoints` the timepoints and the `/DataSetInfo/Channel` groups the channels; every resolution level of the ImarisWriter pyramid for that size must be there with exactly those timepoints and channels, and every `Data` dataset must hold every chunk its `ImageSizeX/Y/Z` and chunk size imply. It prints one line per complete file and, for any other, one `ERROR:` line that names the first missing or wrong attribute or dataset; the exit code is `0` when every file is complete, else `1`. It works on the converter's files, on the single files of `finish`, `monofinish` and `monitor`, and on the link files of `link` and `linkparts` (with their parts). It does not decode chunks: `monofinish --verify` (below) does that for the single file.

#### Exit codes
`0` when the `.ims` was written (or, with `--no-overwrite`, exists). `1` on any error — an unknown option or a missing or malformed value, no arguments at all, an unsupported reader, a missing input folder, a pattern without a match, channels with different numbers of files, an output folder that cannot be created or written, an unsupported sample type (see Limitations), inputs of different types or sizes, a bounding box that does not fit an input, or a failed read or write — after one `ERROR: <text>` line on stderr (the partly written `<name>.ims.partial` is removed, and an existing `<name>.ims` is left as it was).

#### Metadata
The channels are named after their patterns and colored red, green, blue, red, ... unless `--channel-names` and `--colors` say otherwise; wavelengths are written only when given. The file's Creator is `parallelimariswriter` with its version. Timepoint `t` is placed at:
1. `start + t × interval` when `--time-interval` is given; otherwise
2. `start + (stamp[t] − stamp[0])` milliseconds when every file of a channel has a `<n>msecAbs` stamp in its name (as PetaKit5D names its files, e.g. `..._0000123msec_0012345678msecAbs.zarr`; the first such channel is used); otherwise
3. `start + t` seconds.

`start` is `--start-time`, or else the modification time (local time, whole seconds) of the first input file — of the first channel's first timepoint, a `.zarr` folder's own modification time for Zarr input. Imaris shows these times and uses their spacing as the frame interval.

#### Writing and timing
On Linux the output is handed to the operating system for writing while the conversion runs (a helper thread asks for the writeback of the file's written pages every 500 ms), so closing the file at the end does not wait for tens of gigabytes still held in memory. The ImarisWriter may hold up to 2 GB of blocks that are compressed and wait to be written, so that a slow write does not stop the compression threads at once. Neither changes the file. With the environment variable `PIW_TIMING=1` the converter prints one line on stderr when the file is written: the seconds spent reading the inputs, handing the blocks to the ImarisWriter, finishing (histograms, metadata and the close of the file) and checking the closed file, e.g. `Timing: read 12.31 s, feed 30.02 s, finish and close 0.35 s, verify 0.02 s`. The library (`src/parallelimariswriter.h`) reports the same numbers through `Callbacks::timing`.

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

Requirements and limits: the workdir and the output must be on one shared POSIX file system that every job can write (Linux or macOS; the single-file subcommands are not available on Windows); the single-file path takes uint8, uint16 and float32 data, the tile workers uint8 and uint16 (uint32, and float32 tiles: use the link option below). uint8 and uint16 files equal a single-node conversion in every byte that `imscompare` checks. float32 files hold the same data, but their histograms are those of each `(timepoint, channel)`'s own conversion: ImarisWriter's float32 histograms are adaptive (the bins follow the order the values arrive in; its own runs can differ by a bin at levels >= 1), so they cannot be summed or rebuilt exactly. The default display range (`ColorRange`) is the same as a single-node conversion's for one timepoint; for several it is computed from the stored histograms, whose ranges are kept to 3 decimals, and can then differ slightly (at most about 1/700 of the channel's value range plus 1; see `src/mono.h`). Jobs write with `O_DIRECT` and fall back to buffered writes plus a sync where the file system refuses it.

### Plan, tasks, finish (any job system)

The steps below are also available as one **plan** of independent **tasks**, which any job system (or a plain loop) can run; the same steps are library functions (`piw::clusterPlan`, `clusterTask`, `clusterStatus`, `clusterFinish` in `src/parallel.h`):

1. **`plan <workdir> -F <folders> -P <patterns> [the converter's options]`** — finds and checks every input exactly as the converter does (reader, `-v`, `-b`, `-B`, `-C`, `-c`, `-t`, the [metadata](#metadata) options; `-o`/`-n` name the finished file, default `<workdir>/output.ims`) before anything is written, and writes `<workdir>/manifest.json` with its stages. Without `--tiles` it plans a time series: stage `convert`, one task per `(timepoint, channel)` (task `t*nC + c`). With **`--tiles X,Y,Z`** it plans one tiled volume per `(timepoint, channel)` (Zarr; uint8 or uint16, the packs store uint16 only): stages `level0`, `level1`, ..., each after the one before, with `--store mono|packs` (as `setup`'s), `--tiles-per-task N` and `--chunk-bytes N`.
2. **`task <workdir> <stage> <index> [--threads N] [--stage-dir DIR] [-C X,Y,Z]`** — runs one task (the stage by name or number): a time-series task converts its volume, stages the part in `/dev/shm` when it fits (raw size × 1.15 + 8 GB within 90% of the job's memory and of the free `/dev/shm`) or else in `<workdir>/stage`, places it and removes it; a tile task runs its tiles' workers. A task that is done is skipped and a failed one is simply run again. A `SIGTERM` that reaches the task stops its conversion between blocks and removes the part (`scancel` signals only the batch shell: see Staging below); a part that a killed task left in `/dev/shm` is removed by the next task that stages there.
3. **`status <workdir>`** — per stage, the tasks not done, as `--array` ranges (exit code 3 while any is missing).
4. **`finish <workdir> [out.ims] [--verify] [--threads N]`** — exit code 3, naming the missing tasks, until every task is done; then `monofinish` (or, for the packs store, the merge).

Example (SLURM; `<...>` are your own values):
````
parallelimariswriter plan /path/to/workdir -F /path/to/zarrs -P CamA,CamB -r zarr -v 0.108,0.108,0.25 -C 2048,2048,512
sbatch --array=0-<tasks-1> -c 8 --wrap 'parallelimariswriter task /path/to/workdir convert $SLURM_ARRAY_TASK_ID --threads 8'
sbatch --dependency=afterany:<array job> -c 16 --wrap 'parallelimariswriter finish /path/to/workdir --verify --threads 16'
````

### Time series: one job per (timepoint, channel)  *(recommended)*

When each `(timepoint, channel)` volume can be converted by one node (stream it with `-C`), convert each one independently and move it into the single file:

1. **`setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--channel-names ... --start-time ...]`** — finds the `(channel, timepoint)` → `.zarr` inputs and writes `<workdir>/manifest.json`, with the metadata (channel names, colors, wavelengths, timepoint times) the converter would write for the same inputs and options. It takes the converter's metadata options (see [Metadata](#metadata)); every output made from the workdir carries that metadata. For any other input (TIFF, cropped with `-B`, ...) write the manifest with **`monoinit <workdir> <nT> <nC> <vx,vy,vz> [out.ims] [--channel-names ... --start-time ...]`** instead; it takes the same metadata options, but it has no input files to take names and times from: the channels are named `Channel 0`, `Channel 1`, ... unless `--channel-names` is given, and the timepoints are 1 s (or `--time-interval`) apart from `--start-time`, or else from the time `monoinit` runs (give `--start-time` to get the times a single-node conversion writes).
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

When even one `(timepoint, channel)` is too large for a node, tile it spatially (Zarr input). Work is divided by `(channel, timepoint, spatial tile)` at every pyramid level; because each coarser level downsamples the one below it, the levels run **in order**. By default the tile workers write their chunks straight into the single file, as `monoplace` does (setup's **mono store**).

1. **`setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--store mono|packs] [--channel-names ... --start-time ...]`** — plans tiles of `cx,cy,cz` voxels into `manifest.json` (`jobsPerLevel[]` gives the worker count per level), with the chunks laid out as a single-node conversion of all the timepoints lays them out and the metadata as above. Output chunk size comes from the `PIW_CHUNK_BYTES` env var (bytes, default 1 MiB, which matches the single-node converter's chunking). `--store` (default: the `PIW_STORE` env var, else `mono`) chooses where the workers put the chunks: `mono` into the single file, `packs` into one `.pack` file per tile (see below). On Windows the default is `packs`, and `mono` is refused.
2. **`worker <workdir> <level> <index> [count]`** — builds tiles `index` to `index+count-1` (default: one tile) of one level; `index` runs `0..jobsPerLevel[level]-1`, and `count` keeps the number of jobs for a huge volume within the scheduler's limits. Level 0 reads the Zarr; coarser levels read the finer level's chunks back and average them down, so **level r+1 must run after level r** (e.g. via `--dependency=afterok`). In the mono store a worker claims its tile's byte range of the file, writes the chunks there, then the tile's histogram and its sidecar `chunks/L<r>_C<c>_T<t>_<tile>.idx` (the tile's commit point). `OMP_NUM_THREADS` sets the threads for encoding and decoding, and each tile prints one timing line. Safe to re-run: the tile claims the same range again and writes the same bytes.
3. **`monitor <workdir> [out.ims] [--verify] [--threads N]`** — waits until every tile is done (`PIW_MONITOR_TIMEOUT=<seconds>` gives up after that long and names the missing tiles), then runs `monofinish` on the workdir with these options: it checks every sidecar, builds the index, the histograms (summed from the tiles'), the metadata and the thumbnail over the chunks already in the file, and renames it to `out.ims` (default `<workdir>/output.ims`, on the workdir's file system). **`monostatus <workdir>`** lists the placed and the missing tiles; `monofinish <workdir> [out.ims]` can also be run on its own.

Example (SLURM; `<...>` are your own values; one array per level, each after the one before):
````
parallelimariswriter setup /path/to/zarrs CamA /path/to/workdir 2048,2048,512 0.108,0.108,0.25
sbatch --array=0-<jobsPerLevel[0]-1> -c 8 --wrap 'PIW_CLAIM=fcntl OMP_NUM_THREADS=8 parallelimariswriter worker /path/to/workdir 0 $SLURM_ARRAY_TASK_ID'
sbatch --dependency=afterok:<level 0 array> --array=0-<jobsPerLevel[1]-1> -c 8 --wrap '... worker /path/to/workdir 1 $SLURM_ARRAY_TASK_ID'
...
sbatch --dependency=afterok:<last level's array> -c 16 --wrap 'parallelimariswriter monitor /path/to/workdir /path/to/out.ims --verify --threads 16'
````

**Restarting.** Run the worker of a failed tile again, then the levels after it; `monitor` and `monostatus` name a missing tile `L<r>_C<c>_T<t>_<tile>` (its index at level `r` is `(c*nT + t)*n + tile`, with `n = jobsPerLevel[r] / (nC*nT)` tiles per volume). Once the finish has started, the journal is sealed and later workers fail instead of changing the file. A workdir takes either tiles or parts (`monoplace`), never both: a `setup` workdir becomes a tile workdir with its first worker and takes parts until then (a `plan` workdir is one or the other from the start).

**Claims at scale.** A huge volume has thousands of tiles, and many workers claim at once: there `PIW_CLAIM=fcntl` (see Claims above; only where `fcntl` locks work across nodes) is faster than the default `link()` journal, whose claims retry under contention.

**Memory.** A level>0 worker averages the finer level's chunks straight into its own tile, so every level needs about what level 0 needs: the tile, its compressed chunks, and read buffers of up to 64 MB per thread. On ~5000×22000×350 data a 2048,2048,512 tile peaks at about 6 GB at every level. (Only a block layout whose averaging boxes cross chunk boundaries, with an odd block size along a halved axis, first assembles the finer region under the tile, up to 8× its size.)

**Packs store (`setup --store packs`).** The workers write one `.pack` file per tile (atomically: temp file + rename) instead of writing into the single file, and the packs are assembled afterwards:
- **`monitor <workdir> [out.ims]`** — one HDF5 writer reads all packs and writes a single self-contained `.ims` (one write stream: the slowest step for large data);
- or **`combineone <workdir> <t> <c> <out.ims>`** per `(timepoint, channel)` into its own `.ims`, then **`link <workdir> <out.ims> <parts>`** to stitch them with external links (same as `linkparts`, but sourced from the packs). `combineone` and `link` refuse a mono-store workdir.

### Notes

- **Throughput.** The parallel steps are limited by the shared file system's write bandwidth into one file; `monofinish` is serial but writes only metadata (minutes even for millions of chunks). Splitting one HDF5 array across processes with MPI-IO does **not** help (HDF5's collective compressed writes are slower than a single writer).
- **Chunk size.** Keep the default chunking: it matches the single-node converter's. Only the packs store's `monitor` (one writer) gets faster with fewer, bigger chunks — `PIW_CHUNK_BYTES=8388608` (8 MiB) — at some cost to Imaris' interactive-read speed, and its output is then chunked differently from a single-node conversion.
- **Thumbnail and metadata.** All cluster outputs carry the same `/Thumbnail` preview image as the single-node converter, coloured with the channel colors, and the metadata (names, colors, wavelengths, times) that `setup` or `monoinit` resolved into the manifest. The single-file output's `ColorRange` comes from the histograms of every timepoint, as in a single-node conversion; the `linkparts` output takes each channel's range from its timepoint-0 part. The normal single-process converter (above) handles all supported readers and bit depths.

## Reference

Please cite our software if you find it useful in your work:

Xiongtao Ruan, Matthew Mueller, Gaoxiang Liu, Frederik Görlitz, Tian-Ming Fu, Daniel E. Milkie, Joshua L. Lillvis, Alexander Kuhn, Chu Yi Aaron Herr, Wilmene Hercule, Marc Nienhaus, Alison N. Killilea, Eric Betzig, Srigokul Upadhyayula. Image processing tools for petabyte-scale light sheet microscopy data. Nature Methods (2024). https://doi.org/10.1038/s41592-024-02475-4
