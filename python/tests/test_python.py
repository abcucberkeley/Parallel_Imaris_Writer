"""Tests of the parallel-imaris-writer Python package, run against each built wheel.

    python tests/test_python.py [section ...]      (from any folder; default: every section)

Sections:
  cli         the command line in this process (run_cli twice, -C, python -m parallel_imaris_writer,
              an output it cannot write) and the file conversion call (pybind11_convert with its input
              selection, messages, progress, the opt-in x-on-Imaris-X mapping, errors, Ctrl-C), on
              small Zarr v2 volumes written here with NumPy alone; the version against CMakeLists.txt
  convert     convert_to_ims and find_input_files on Zarr ((y, x, z) in F order, (z, y, x) in C
              order) and TIFF inputs written here: the file of write_ims of the same array, also with
              process_block_size, block_size, input_bbox (the array's crop), order='F', input_axes and
              x_on_imaris_x (read back with it too); two channels and three time points by patterns in
              name order; the command line with the same options; compressed inputs from cpp-zarr and
              cpp-tiff when installed; errors (no match, no metadata, a truncated chunk, a box outside,
              inputs of different sizes, mixed orders, ...) leave nothing behind
  dataset     convert_dataset as PetaKit5D's MATLAB dataset wrapper: the file and path of its command
              line, overwrite, result_dir_name, TIFF datasets, a missing match (None, a warning), a
              list of datasets and one in two folders, inputs of different sizes refused before any
              output unless input_bbox lies inside all of them
  roundtrip   write_ims then read_ims and ims_info: uint8, uint16, uint32 and float32, order 'C' and
              'F', 2-D to 5-D arrays and lists, odd sizes, every resolution level, regions, out=
  layout      the memory layout never changes the file: F-contiguous, strided, reversed, other byte
              order, unaligned and read-only arrays give the CRCs of the C-contiguous copy; (y, x, z)
              with order='F' equals (z, y, x); lists equal 4-D/5-D arrays; block_size and num_threads
              do not change the file; the same memory read as order='F' gives another file
  golden      CRCs of one small 2-channel uint16 file, computed on Linux: the same on every platform
  threads     num_threads changes no default thread count; other Python threads run during a write
              (the GIL is released); two threads writing at once
  progress    progress values never decrease and end at 1.0; a callback that raises stops the write
              and leaves no file; Ctrl-C (simulated) likewise; verbose prints the messages
  metadata    channel names and colors, wavelengths, start time and interval in ims_info
  errors      unsupported types, empty arrays, wrong arguments, unwritable folders: exceptions, the
              process alive, nothing left behind
  h5py        with h5py and hdf5plugin installed: every level read through h5py equals read_ims
  importorder with scipy, cpp-zarr and cpp-tiff installed: this package imported and used after
              numpy, scipy, cppzarr and cpptiff were used, and before (fresh processes)
  fork        (Linux, macOS) after a write, a write in a child made by os.fork() ends within 60 s
  sigint      (Linux, macOS) SIGINT 1 s into a long write ends it with KeyboardInterrupt within 5 s,
              leaving no file
  cluster     (Linux, macOS) the cluster steps on two channels and two time points of (y, x, z) Zarrs:
              cluster_plan, every task in a Python process of its own (one with a truncated chunk
              fails alone and runs once the chunk is whole), cluster_status, cluster_finish: the file
              of convert_to_ims of the same inputs and options, and of the same steps through the
              command line; (z, y, x) Zarrs, also with x_on_imaris_x; Ctrl-C stops a task, leaving
              nothing staged; tile plans (single file: every task a process, also through the command
              line; another chunk_bytes; the packs store); errors

Only NumPy is needed; h5py + hdf5plugin, scipy, cpp-zarr and cpp-tiff are used when installed (a
line says when a part is skipped). Exits non-zero on any failure.
"""
import _thread
import datetime
import importlib.util
import io
import json
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import warnings

import numpy as np
import parallel_imaris_writer as piw
from parallel_imaris_writer.parallel_imaris_writer import (pybind11_convert, pybind11_default_threads,
                                                           pybind11_find_input_files)

# h5py and hdf5plugin (LZ4, filter 32004) read the files back, when they are installed
try:
    import h5py
    import hdf5plugin  # noqa: F401
except ImportError:
    h5py = None

HDF5_SIGNATURE = b'\x89HDF\r\n\x1a\n'
POSIX = os.name == 'posix'
START = '2026-01-02 03:04:05'           # a fixed start time: the files' attributes are then the same
TYPES = (np.uint8, np.uint16, np.uint32, np.float32)

failures = []


def check(cond, what):
    print(('ok    ' if cond else 'FAILED ') + what, flush=True)
    if not cond:
        failures.append(what)


def skip(what):
    print('skip  ' + what, flush=True)


def raises(exception, call, what):
    try:
        call()
    except exception as e:
        check(True, f'{what}: {type(e).__name__}: {e}')
        return
    except BaseException as e:
        check(False, f'{what}: {type(e).__name__} instead of {exception.__name__}: {e}')
        return
    check(False, f'{what}: no {exception.__name__}')


def leftovers(folder, keep=()):
    # Files in folder other than keep (a failed write must leave nothing, not even its temporary file)
    return sorted(set(os.listdir(folder)) - set(keep))


def sample(shape, dtype, seed=0):
    # Values that change along every axis (uint8: 0-250, uint16: up to 65520, uint32: up to 100002,
    # float32: 0 to 1000.5 in halves)
    n = int(np.prod(shape))
    v = (np.arange(n, dtype=np.uint64) * np.uint64(2654435761) + np.uint64(seed * 97)) % np.uint64(65521)
    if dtype == np.uint8:
        v = v % np.uint64(251)
    elif dtype == np.uint32:
        v = v * np.uint64(1543) % np.uint64(100003)
    elif dtype == np.float32:
        return ((v % np.uint64(2002)).astype(np.float32) / np.float32(2)).reshape(shape)
    return v.astype(dtype).reshape(shape)


def smooth(shape, seed=0):
    # A compressible uint16 (z, y, x) volume
    z, y, x = np.meshgrid(*(np.arange(n) for n in shape), indexing='ij')
    return ((z * 17 + y * 7 + x * 13 + seed * 137) % 4096 + 1).astype(np.uint16)


def crcs(path, dtype=np.uint16):
    # The CRCs of every dataset (data and histograms) and of the thumbnail. uint32/float32 levels
    # above 0 have histograms that vary from run to run (ImarisWriter's adaptive 32-bit
    # histograms depend on the resample threads' order): their histograms are left out.
    info = piw.ims_info(path, checksums=True)
    vary = np.dtype(dtype) in (np.dtype(np.uint32), np.dtype(np.float32))
    return [(d['resolution_level'], d['timepoint'], d['channel'], d['shape'], d['chunks'], d['crc'],
             None if vary and d['resolution_level'] > 0 else d['histogram_crc']) for d in info['datasets']] + \
        [('thumbnail', info['thumbnail_crc'])]


VARYING_32BIT = re.compile(r'^/DataSet/ResolutionLevel [1-9][0-9]*/.*/(HistogramMin|HistogramMax)$')


def attributes(path, dtype=np.uint16):
    # Every attribute (the 32-bit types' histogram limits above level 0 left out, as in crcs)
    a = piw.ims_info(path)['attributes']
    if np.dtype(dtype) in (np.dtype(np.uint32), np.dtype(np.float32)):
        a = {k: v for k, v in a.items() if not VARYING_32BIT.match(k)}
    return a


def same_file(a, b, dtype=np.uint16):
    return crcs(a, dtype) == crcs(b, dtype) and attributes(a, dtype) == attributes(b, dtype)


# ---- cli: the command line and the file conversion call (C-4's smoke test)

ZSHAPE = (101, 83, 47)   # dims 0, 1, 2 = Imaris X, Y, Z (y, x, z); odd sizes pad every axis
ZCHUNKS = (64, 64, 32)


def zvolume(seed, shape=ZSHAPE):
    y, x, z = np.meshgrid(*(np.arange(n) for n in shape), indexing='ij')
    return ((y * 7 + x * 13 + z * 17 + seed * 137) % 4096 + 1).astype(np.uint16)


def write_zarr(path, data, chunks=ZCHUNKS, order='F'):
    # An uncompressed Zarr v2 array, in F order (MATLAB's layout) by default, chunk files named i.j.k
    os.makedirs(path)
    meta = {'zarr_format': 2, 'shape': list(data.shape), 'chunks': list(chunks), 'dtype': data.dtype.str,
            'order': order, 'compressor': None, 'fill_value': 0, 'filters': None, 'dimension_separator': '.'}
    with open(os.path.join(path, '.zarray'), 'w') as f:
        json.dump(meta, f)
    counts = [-(-n // c) for n, c in zip(data.shape, chunks)]
    for index in np.ndindex(*counts):
        chunk = np.zeros(chunks, data.dtype)   # edge chunks are stored whole
        region = tuple(slice(i * c, min((i + 1) * c, n)) for i, c, n in zip(index, chunks, data.shape))
        part = data[region]
        chunk[tuple(slice(0, s) for s in part.shape)] = part
        with open(os.path.join(path, '.'.join(map(str, index))), 'wb') as f:
            f.write(chunk.tobytes(order=order))


def write_tiff(path, image):
    # A multi-page uncompressed little-endian TIFF of a (y, x, z) image, as PetaKit5D stores one:
    # page k holds image[:, :, k], its rows y and its columns x
    ny, nx, nz = image.shape
    data = image.astype(image.dtype.newbyteorder('<'))
    pages = [np.ascontiguousarray(data[:, :, k]).tobytes() for k in range(nz)]
    fmt = {'u': 1, 'i': 2, 'f': 3}[image.dtype.kind]
    entries = 11
    ifd_size = 2 + entries * 12 + 4
    first_ifd = 8 + sum(len(p) for p in pages)
    out = bytearray(b'II*\x00' + struct.pack('<I', first_ifd))
    offsets = []
    for p in pages:
        offsets.append(len(out))
        out += p
    for k, p in enumerate(pages):
        nxt = first_ifd + (k + 1) * ifd_size if k + 1 < nz else 0
        tags = [(256, 4, nx), (257, 4, ny), (258, 3, image.dtype.itemsize * 8), (259, 3, 1), (262, 3, 1),
                (273, 4, offsets[k]), (277, 3, 1), (278, 4, ny), (279, 4, len(p)), (284, 3, 1), (339, 3, fmt)]
        out += struct.pack('<H', entries)
        for tag, kind, value in tags:
            out += struct.pack('<HHI', tag, kind, 1) + (struct.pack('<HH', value, 0) if kind == 3 else
                                                         struct.pack('<I', value))
        out += struct.pack('<I', nxt)
    with open(path, 'wb') as f:
        f.write(out)


def is_hdf5(path):
    if not os.path.isfile(path):
        return False
    with open(path, 'rb') as f:
        return f.read(8) == HDF5_SIGNATURE


def ims_matches(path, volumes, x_on_imaris_x=False):
    # Every channel's level 0 equals its (y, x, z) volume: Imaris (X, Y, Z) = dims (0, 1, 2), or
    # (1, 0, 2) with x_on_imaris_x
    for c, v in enumerate(volumes):
        data = piw.read_ims(path, channel=c, order='F')     # (Imaris X, Y, Z)
        expect = np.transpose(v, (1, 0, 2)) if x_on_imaris_x else v
        if not np.array_equal(data, expect):
            return False
    return True


def image_info(path):
    # Imaris X, Y, Z and ExtMax0, 1, 2
    info = piw.ims_info(path, order='F')
    return info['shape'], info['extent_max']


def cmake_version():
    # project(... VERSION x.y.z) of the repository's CMakeLists.txt, when the tests run from it
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.path.dirname(here), os.path.dirname(os.path.dirname(here))):
        p = os.path.join(d, 'CMakeLists.txt')
        if os.path.isfile(p):
            with open(p) as f:
                m = re.search(r'project\s*\([^)]*?\bVERSION\s+([0-9]+(?:\.[0-9]+)*)', f.read())
            if m:
                return m.group(1)
    return None


def test_cli(tmp):
    # Version: the CMake project's, and the installed distribution's
    version = cmake_version()
    check(re.fullmatch(r'[0-9]+(\.[0-9]+)*', piw.__version__), f'version {piw.__version__}')
    if version is not None:
        check(piw.__version__ == version, f'version equals CMakeLists.txt ({version})')
    try:
        from importlib.metadata import version as dist_version, PackageNotFoundError
        try:
            check(dist_version('parallel-imaris-writer') == piw.__version__, 'version equals the distribution')
        except PackageNotFoundError:
            skip('version of the distribution (not installed)')
    except ImportError:
        pass
    check(pybind11_default_threads() >= 1, f'default threads {pybind11_default_threads()}')

    # Input: two channels of one time point
    inputs = os.path.join(tmp, 'in')
    os.makedirs(inputs)
    vols = [zvolume(0), zvolume(1)]
    for name, v in zip(('CamA_t0000.zarr', 'CamB_t0000.zarr'), vols):
        write_zarr(os.path.join(inputs, name), v)

    # The command line, twice in one process
    out1 = os.path.join(tmp, 'out1')
    rc = piw.run_cli(['-P', 'CamA,CamB', '-F', inputs, '-o', out1, '-r', 'zarr', '-v', '0.108,0.108,0.25'])
    f1 = os.path.join(out1, 'output.ims')
    check(rc == 0 and is_hdf5(f1), f'run_cli: exit code {rc}, HDF5 file written')
    check(ims_matches(f1, vols), 'run_cli: voxels')
    out2 = os.path.join(tmp, 'out2')
    rc = piw.run_cli(['-P', 'CamA', '-F', inputs, '-o', out2, '-n', 'second', '-r', 'zarr', '-C', '32,32,32'])
    f2 = os.path.join(out2, 'second.ims')
    check(rc == 0 and is_hdf5(f2) and ims_matches(f2, vols[:1]), f'run_cli again (-C): exit code {rc}')
    check(piw.run_cli([]) == 1, 'run_cli without arguments: exit code 1')
    raises(TypeError, lambda: piw.run_cli('-P CamA'), 'run_cli refuses one string')

    # python -m parallel_imaris_writer (the console script's main)
    out3 = os.path.join(tmp, 'out3')
    r = subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-P', 'CamB', '-F', inputs, '-o', out3,
                        '-r', 'zarr'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    check(r.returncode == 0 and 'Current File: ' in r.stdout and is_hdf5(os.path.join(out3, 'output.ims')),
          f'python -m parallel_imaris_writer: exit code {r.returncode}')
    # An output it cannot write (output.ims is a folder): exit code 1 and "ERROR: ..." on stderr
    out_dir = os.path.join(tmp, 'out_isdir')
    os.makedirs(os.path.join(out_dir, 'output.ims'))
    r = subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-P', 'CamA', '-F', inputs, '-o', out_dir,
                        '-r', 'zarr'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    check(r.returncode == 1 and 'ERROR: ' in r.stderr and 'ERROR: ' not in r.stdout,
          f'python -m with an unwritable output: exit code {r.returncode}, "ERROR: ..." on stderr')

    # The file conversion call
    files = pybind11_find_input_files([inputs], ['CamA', 'CamB'])
    check([[os.path.normpath(p) for p in c] for c in files]
          == [[os.path.join(inputs, 'CamA_t0000.zarr')], [os.path.join(inputs, 'CamB_t0000.zarr')]],
          'find_input_files: files[channel][timepoint]')
    f4 = os.path.join(tmp, 'convert.ims')
    captured, sys.stdout = sys.stdout, io.StringIO()
    values = []
    try:
        pybind11_convert('zarr', files, f4, (0.108, 0.108, 0.25), channel_names=['A', 'B'], num_threads=2,
                         verbose=True, progress=lambda f, b: values.append(f))
        text = sys.stdout.getvalue()
    finally:
        sys.stdout = captured
    check(is_hdf5(f4) and ims_matches(f4, vols), 'convert: HDF5 file written')
    check(text.count('Current File: ') == 2, 'convert(verbose=True): messages through sys.stdout')
    check(values and values[-1] == 1.0 and values == sorted(values), f'convert: progress ends at 1.0 ({values})')
    pybind11_convert('zarr', files, f4, (1, 1, 1), block_size=(32, 32, 32), process_block=(64, 64, 64))
    check(is_hdf5(f4) and ims_matches(f4, vols), 'convert again: block_size, process_block')
    # x_on_imaris_x: dim 1 on Imaris X, dim 0 on Imaris Y; the voxel sizes stay along dims 0, 1, 2
    f6 = os.path.join(tmp, 'x_on_x.ims')
    pybind11_convert('zarr', files[:1], f6, (0.25, 0.5, 2.0), x_on_imaris_x=True)
    info = image_info(f6)
    check(is_hdf5(f6) and ims_matches(f6, vols[:1], x_on_imaris_x=True) and
          info[0] == (ZSHAPE[1], ZSHAPE[0], ZSHAPE[2]) and
          np.allclose(info[1], (ZSHAPE[1] * 0.5, ZSHAPE[0] * 0.25, ZSHAPE[2] * 2.0)),
          f'convert(x_on_imaris_x=True): Imaris X, Y, Z = dims 1, 0, 2 ({info})')

    # Errors as exceptions
    raises(FileNotFoundError, lambda: pybind11_find_input_files([inputs], ['CamC']), 'pattern without a match')
    raises(ValueError, lambda: pybind11_convert('png', files, f4, (1, 1, 1)), 'unknown reader')
    raises(ValueError, lambda: pybind11_convert('zarr', [], f4, (1, 1, 1)), 'no input files')
    raises(ValueError, lambda: pybind11_convert('zarr', files, f4, (1, 1, 1), block_size=(1, 2)), 'two block sizes')
    raises(RuntimeError, lambda: pybind11_convert('zarr', [[os.path.join(inputs, 'none.zarr')]],
                                                  os.path.join(tmp, 'none.ims'), (1, 1, 1)), 'missing input')

    # Ctrl-C (a simulated SIGINT, 0.05 s in) stops a conversion: KeyboardInterrupt, no output. The
    # volume is large enough that on a fast machine the cancel still comes while blocks are fed;
    # a cancel that comes only during Finish (which does not stop) leaves a complete file.
    big = os.path.join(tmp, 'big')
    os.makedirs(big)
    write_zarr(os.path.join(big, 'CamA.zarr'), zvolume(2, (400, 400, 800)), chunks=(128, 128, 64))
    f5 = os.path.join(tmp, 'cancelled.ims')
    interrupted, seconds = interrupt(lambda: pybind11_convert('zarr', [[os.path.join(big, 'CamA.zarr')]], f5,
                                                              (1, 1, 1), num_threads=1), 0.05)
    if interrupted and os.path.exists(f5) and complete_ims(f5):
        skip(f'Ctrl-C: the cancel came after every block was written ({seconds:.2f} s); the file is complete')
    elif interrupted:
        check(not os.path.exists(f5), f'Ctrl-C: KeyboardInterrupt after {seconds:.2f} s, partial output removed')
    else:
        skip(f'Ctrl-C: the conversion finished first ({seconds:.2f} s)')
    pybind11_convert('zarr', files, f4, (1, 1, 1))
    check(is_hdf5(f4), 'a conversion after Ctrl-C')


def complete_ims(path):
    # A finished .ims: readable, with the dataset metadata, and every dataset holding all its chunks
    try:
        info = piw.ims_info(path)
    except Exception:
        return False
    for d in info['datasets']:
        expected = 1
        for n, c in zip(d['shape'], d['chunk_shape']):
            expected *= -(-n // c)
        if d['chunks'] != expected:
            return False
    return bool(info['datasets']) and bool(info['channel_names'])


def interrupt(call, after):
    # Runs call() with a simulated Ctrl-C (KeyboardInterrupt in this, the main, thread) after
    # `after` seconds: (whether it raised KeyboardInterrupt, its seconds)
    interrupted = False
    t0 = time.time()
    seconds = None
    timer = threading.Timer(after, _thread.interrupt_main)
    try:
        timer.start()
        try:
            call()
        except KeyboardInterrupt:
            interrupted = True
        seconds = time.time() - t0
        timer.join()
        time.sleep(0.2)     # (where a KeyboardInterrupt after a finished call comes)
    except KeyboardInterrupt:
        pass
    if seconds is None:     # (the interrupt came between the call's end and the line after it)
        seconds = time.time() - t0
    return interrupted, seconds


# ---- convert: convert_to_ims and find_input_files

def normalized(files):
    return [[os.path.normpath(f) for f in channel] for channel in files]


def mtime_ns(path):
    return os.stat(path).st_mtime_ns


def seconds(stamp):
    # 'YYYY-MM-DD HH:MM:SS[.mmm]' as seconds
    t = datetime.datetime.strptime(stamp[:19], '%Y-%m-%d %H:%M:%S').timestamp()
    return t + (int(stamp[20:23]) / 1000 if len(stamp) > 20 else 0)


def test_convert(tmp):
    # One (z, y, x) volume stored as PetaKit5D stores it, (y, x, z): an F-order Zarr and a TIFF;
    # and as a C-order (z, y, x) Zarr
    a = sample((37, 61, 45), np.uint16, seed=11)
    img = np.transpose(a, (1, 2, 0))
    voxel = (0.5, 0.2, 0.1)
    dirs = {k: os.path.join(tmp, k) for k in ('zarr', 'tiff', 'zyx')}
    for d in dirs.values():
        os.makedirs(d)
    zf = os.path.join(dirs['zarr'], 'CamA_t0000.zarr')
    tf = os.path.join(dirs['tiff'], 'CamA_t0000.tif')
    cf = os.path.join(dirs['zyx'], 'CamA_t0000.zarr')
    write_zarr(zf, img, chunks=(32, 32, 16))
    write_tiff(tf, img)
    write_zarr(cf, a, chunks=(16, 32, 32), order='C')

    def out(name):
        return os.path.join(tmp, name + '.ims')

    def write(name, data, **kw):
        piw.write_ims(out(name), data, start_time=START, channel_names=['CamA'], **kw)
        return out(name)

    def convert(name, **kw):
        kw.setdefault('start_time', START)
        piw.convert_to_ims(out(name), **kw)
        return out(name)
    ref = write('ref', a, voxel_size=voxel)
    check(same_file(ref, convert('z', folders=dirs['zarr'], channel_patterns='CamA', voxel_size=voxel)) and
          np.array_equal(piw.read_ims(out('z')), a), 'convert_to_ims of a (y, x, z) Zarr: the file of write_ims')
    check(same_file(ref, convert('t', folders=[dirs['tiff']], channel_patterns=['CamA'], voxel_size=voxel)),
          'convert_to_ims of a (y, x, z) TIFF: the file of write_ims')
    check(same_file(ref, convert('tf', files=tf, voxel_size=voxel, channel_names=['CamA'])),
          'convert_to_ims(files=one TIFF): the same file')
    check(same_file(ref, convert('c', folders=dirs['zyx'], channel_patterns='CamA', voxel_size=voxel)),
          'a C-order (z, y, x) Zarr, input_axes from its order: the same file')
    check(same_file(ref, convert('c2', files=[cf], input_axes='zyx', voxel_size=voxel, channel_names=['CamA'])),
          "input_axes='zyx': the same file")
    other = convert('c3', files=cf, input_axes='yxz', voxel_size=voxel, channel_names=['CamA'])
    check(crcs(other) != crcs(ref) and piw.ims_info(other, order='F')['shape'] == a.shape,
          "the C-order Zarr with input_axes='yxz' is the (y, x, z) image of its dims: another file")
    check(same_file(ref, convert('zF', folders=dirs['zarr'], channel_patterns='CamA', voxel_size=(0.2, 0.1, 0.5),
                                 order='F')), "order='F': voxel_size (dy, dx, dz)")
    check(same_file(ref, convert('zmap', folders=dirs['zarr'], channel_patterns='CamA',
                                 voxel_size={'x': 0.1, 'y': 0.2, 'z': 0.5})), 'voxel_size as a mapping')

    # block_size and process_block_size do not change the file
    for name, kw in (('z', dict(folders=dirs['zarr'])), ('t', dict(folders=dirs['tiff'])),
                     ('c', dict(folders=dirs['zyx']))):
        for pb in ((8, 16, 16), (20, 61, 45)):
            check(crcs(convert('pb', channel_patterns='CamA', voxel_size=voxel, process_block_size=pb, **kw)) ==
                  crcs(ref), f'{name}: process_block_size {pb}')
        check(crcs(convert('bs', channel_patterns='CamA', voxel_size=voxel, block_size=(16, 32, 24), **kw)) ==
              crcs(ref), f'{name}: block_size (16, 32, 24)')

    # input_bbox (z0, y0, x0, z1, y1, x1): the crop of the array
    box = (3, 5, 7, 30, 50, 40)
    crop = write('crop', a[3:30, 5:50, 7:40], voxel_size=voxel)
    for name, kw in (('zarr', dict(folders=dirs['zarr'])), ('TIFF', dict(folders=dirs['tiff'])),
                     ('(z, y, x) Zarr', dict(folders=dirs['zyx']))):
        for pb in (None, (8, 16, 16)):
            check(same_file(crop, convert('box', channel_patterns='CamA', voxel_size=voxel, input_bbox=box,
                                          process_block_size=pb, **kw)),
                  f'input_bbox {box} on the {name}' + (f', process_block_size {pb}' if pb else '') +
                  ': write_ims of a[3:30, 5:50, 7:40]')
    check(same_file(crop, convert('boxF', folders=dirs['zarr'], channel_patterns='CamA', voxel_size=(0.2, 0.1, 0.5),
                                  input_bbox=(5, 7, 3, 50, 40, 30), order='F')), "input_bbox with order='F'")

    # x_on_imaris_x: the image's x on Imaris X; read back with it, the same array
    xref = write('xref', a, voxel_size=voxel, x_on_imaris_x=True)
    info = piw.ims_info(xref, order='F')
    check(info['shape'] == (a.shape[2], a.shape[1], a.shape[0]) and np.allclose(info['voxel_size'], (0.1, 0.2, 0.5)) and
          np.array_equal(piw.read_ims(xref, x_on_imaris_x=True), a) and
          np.array_equal(piw.read_ims(xref, order='F', x_on_imaris_x=True), img) and
          piw.ims_info(xref, x_on_imaris_x=True)['shape'] == a.shape and
          np.array_equal(piw.read_ims(xref, order='F'), np.transpose(a, (2, 1, 0))),
          f'write_ims(x_on_imaris_x=True): Imaris X, Y, Z = x, y, z; read back with it: the array ({info["shape"]})')
    check(np.array_equal(piw.read_ims(xref, start_coords=(1, 2, 3), end_coords=(20, 30, 40), x_on_imaris_x=True),
                         a[1:20, 2:30, 3:40]), 'read_ims(x_on_imaris_x=True) of a region')
    for name, folder in (('zarr', dirs['zarr']), ('TIFF', dirs['tiff']), ('(z, y, x) Zarr', dirs['zyx'])):
        check(same_file(xref, convert('x', folders=folder, channel_patterns='CamA', voxel_size=voxel,
                                      x_on_imaris_x=True)), f'convert_to_ims(x_on_imaris_x=True) of the {name}: '
                                                            'the file of write_ims(x_on_imaris_x=True)')
    check(same_file(write('xcrop', a[3:30, 5:50, 7:40], voxel_size=voxel, x_on_imaris_x=True),
                    convert('xbox', folders=dirs['zyx'], channel_patterns='CamA', voxel_size=voxel, input_bbox=box,
                            x_on_imaris_x=True)), 'x_on_imaris_x with input_bbox on the (z, y, x) Zarr')

    # Two channels by patterns, three time points: files in name order
    multi = os.path.join(tmp, 'multi')
    os.makedirs(multi)
    vols = {(c, t): sample((9, 30, 22), np.uint16, seed=20 + 3 * c + t) for c in range(2) for t in range(3)}
    for t in (2, 0, 1):
        for c, name in enumerate(('CamA', 'CamB')):
            write_zarr(os.path.join(multi, f'{name}_t{t:04d}.zarr'), np.transpose(vols[c, t], (1, 2, 0)),
                       chunks=(16, 16, 8))
    files = piw.find_input_files(multi, ['CamA', 'CamB'])
    expect = [[os.path.join(multi, f'{n}_t{t:04d}.zarr') for t in range(3)] for n in ('CamA', 'CamB')]
    check(normalized(files) == expect,
          'find_input_files: files[channel][timepoint] in name order')
    m = convert('multi', folders=multi, channel_patterns=['CamA', 'CamB'])
    info = piw.ims_info(m)
    check(info['channels'] == 2 and info['timepoints'] == 3 and info['channel_names'] == ['CamA', 'CamB'] and
          all(np.array_equal(piw.read_ims(m, timepoint=t, channel=c), vols[c, t]) for c in range(2) for t in range(3)),
          'two channels, three time points: the volumes in name order, channels named after the patterns')
    check(same_file(m, convert('multi_f', files=files, channel_names=['CamA', 'CamB'])),
          'files=files[channel][timepoint] gives the same file')
    piw.write_ims(out('multi_w'), [[vols[c, t] for c in range(2)] for t in range(3)], channel_names=['CamA', 'CamB'],
                  start_time=START)
    check(same_file(m, out('multi_w')), 'two channels, three time points: the file of write_ims')

    # find_input_files and the reader
    both = os.path.join(tmp, 'both')
    os.makedirs(both)
    write_zarr(os.path.join(both, 'CamA_t0000.zarr'), img, chunks=(32, 32, 16))
    write_tiff(os.path.join(both, 'CamA_t0001.tif'), img)
    raises(ValueError, lambda: piw.find_input_files(both, 'CamA'), 'find_input_files of TIFF and Zarr inputs')
    check(normalized(piw.find_input_files(both, 'CamA', reader='zarr')) == [[os.path.join(both, 'CamA_t0000.zarr')]] and
          normalized(piw.find_input_files(both, 'CamA', reader='tiff')) == [[os.path.join(both, 'CamA_t0001.tif')]],
          'find_input_files with a reader takes its files only')
    check(same_file(ref, convert('r', folders=both, channel_patterns='CamA', reader='tiff', voxel_size=voxel)),
          "convert_to_ims(reader='tiff') of a folder with both")

    # The command line (python -m parallel_imaris_writer; its X, Y, Z = y, x, z) gives the same file
    cli = os.path.join(tmp, 'cli')
    r = subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-P', 'CamA,CamB', '-F', multi, '-o', cli,
                        '-n', 'multi', '-r', 'zarr', '-v', '0.2,0.1,0.5', '-B', '5,7,1,25,20,8', '-C', '16,16,4'],
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    p = convert('multi_cli', folders=multi, channel_patterns=['CamA', 'CamB'], reader='zarr', voxel_size=voxel,
                input_bbox=(1, 5, 7, 8, 25, 20), process_block_size=(4, 16, 16), start_time=None)
    check(r.returncode == 0 and same_file(os.path.join(cli, 'multi.ims'), p),
          f'python -m parallel_imaris_writer -B -C: the file of convert_to_ims (exit code {r.returncode})')

    # verbose, progress, threads
    captured, sys.stdout = sys.stdout, io.StringIO()
    values = []
    try:
        convert('v', folders=multi, channel_patterns=['CamA', 'CamB'], verbose=True, num_threads=2,
                progress=lambda f, b: values.append(f))
        text = sys.stdout.getvalue()
    finally:
        sys.stdout = captured
    check(text.count('Current File: ') == 6 and values and values[-1] == 1.0 and values == sorted(values),
          f'verbose prints the messages; progress ends at 1.0 ({len(values)} calls)')

    # With cpp-zarr and cpp-tiff: compressed Zarrs (zstd, F and C order) and an LZW TIFF
    try:
        import cppzarr
        cz = os.path.join(tmp, 'cppzarr')
        os.makedirs(cz)
        cppzarr.write_zarr(os.path.join(cz, 'CamA_f.zarr'), np.asfortranarray(img))
        cppzarr.write_zarr(os.path.join(cz, 'CamB_c.zarr'), a, order='C')
        check(same_file(ref, convert('czf', files=os.path.join(cz, 'CamA_f.zarr'), voxel_size=voxel,
                                     channel_names=['CamA'])) and
              same_file(ref, convert('czc', files=os.path.join(cz, 'CamB_c.zarr'), voxel_size=voxel,
                                     channel_names=['CamA'])),
              'cppzarr.write_zarr (zstd) of the (y, x, z) image, and of the array in C order: the same file')
    except ImportError:
        skip('compressed Zarrs: cpp-zarr is not installed')
    try:
        import cpptiff
        ct = os.path.join(tmp, 'lzw.tif')
        cpptiff.write_tiff(ct, a)
        check(same_file(ref, convert('ct', files=ct, voxel_size=voxel, channel_names=['CamA'])),
              'cpptiff.write_tiff (LZW) of the array: the same file')
    except ImportError:
        skip('LZW TIFF: cpp-tiff is not installed')

    # Errors: exceptions, nothing left behind
    err = os.path.join(tmp, 'errors')
    os.makedirs(err)
    e = os.path.join(err, 'e.ims')
    raises(FileNotFoundError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns='CamC'),
           'a pattern without a match')
    raises(FileNotFoundError, lambda: piw.convert_to_ims(e, folders=os.path.join(tmp, 'none'), channel_patterns='A'),
           'a folder that does not exist')
    bad = os.path.join(tmp, 'bad')
    os.makedirs(os.path.join(bad, 'CamA_nometa.zarr'))
    raises(RuntimeError, lambda: piw.convert_to_ims(e, folders=bad, channel_patterns='CamA'),
           'a .zarr folder without metadata')
    trunc = os.path.join(tmp, 'trunc')
    os.makedirs(trunc)
    tz = os.path.join(trunc, 'CamA_t0000.zarr')
    write_zarr(tz, img, chunks=(32, 32, 16))
    with open(os.path.join(tz, '1.1.1'), 'r+b') as f:
        f.truncate(100)
    raises(RuntimeError, lambda: piw.convert_to_ims(e, folders=trunc, channel_patterns='CamA'), 'a truncated chunk')
    raises(ValueError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns='CamA',
                                                  input_bbox=(0, 0, 0, 38, 61, 45)), 'an input_bbox outside the data')
    raises(ValueError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns='CamA',
                                                  input_bbox=(5, 0, 0, 5, 61, 45)), 'an empty input_bbox')
    raises(ValueError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns='CamA',
                                                  input_bbox=(0, 0, 0, 5, 6)), 'an input_bbox of five values')
    raises(TypeError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns='CamA',
                                                 input_bbox=(0, 0, 0.5, 5, 6, 7)), 'an input_bbox of 0.5')
    sizes = os.path.join(tmp, 'sizes')
    os.makedirs(sizes)
    write_zarr(os.path.join(sizes, 'CamA_t0000.zarr'), img, chunks=(32, 32, 16))
    write_zarr(os.path.join(sizes, 'CamA_t0001.zarr'), img[:, :40], chunks=(32, 32, 16))
    raises(ValueError, lambda: piw.convert_to_ims(e, folders=sizes, channel_patterns='CamA'),
           'inputs of different sizes')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=[cf, zf]), 'Zarrs of C and F order without input_axes')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=tf, input_axes='zyx'), "input_axes='zyx' with a TIFF")
    raises(ValueError, lambda: piw.convert_to_ims(e, files=tf, input_axes='xyz'), "input_axes='xyz'")
    raises(ValueError, lambda: piw.convert_to_ims(e), 'no inputs')
    raises(ValueError, lambda: piw.convert_to_ims(e, folders=dirs['zarr']), 'folders without channel_patterns')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=tf, folders=dirs['zarr'], channel_patterns='CamA'),
           'files and folders')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=os.path.join(tmp, 'x.png')), 'a file of no known reader')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=[tf, zf]), 'TIFF and Zarr files without a reader')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=tf, reader='png'), "reader='png'")
    raises(ValueError, lambda: piw.convert_to_ims(os.path.join(err, 'e.h5'), files=tf), 'a file name without .ims')
    raises(TypeError, lambda: piw.convert_to_ims(e, folders=dirs['zarr'], channel_patterns=[1]), 'a pattern that is 1')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=tf, num_threads=0), 'num_threads=0')
    raises(ValueError, lambda: piw.convert_to_ims(e, files=[[tf], [tf, tf]]),
           'channels with different numbers of files')
    check(not leftovers(err), f'no file left behind by the refused conversions ({leftovers(err)})')


# ---- dataset: convert_dataset, the MATLAB dataset wrapper's behaviour

def test_dataset(tmp):
    stamps = (1000, 3500, 6000)                 # ms, in the names as PetaKit5D writes them
    shape = (13, 40, 33)                        # (z, y, x)
    vols = {(c, t): sample(shape, np.uint16, seed=40 + 3 * c + t) for c in range(2) for t in range(3)}

    def dataset(folder, zarr=True, times=range(3), sizes=None):
        os.makedirs(folder, exist_ok=True)
        for t in times:
            for c, cam in enumerate(('CamA', 'CamB')):
                v = vols[c, t] if sizes is None else sizes(c, t)
                name = f'Iter_{t:04d}_{cam}_ch0_{stamps[t]:010d}msecAbs'
                if zarr:
                    write_zarr(os.path.join(folder, name + '.zarr'), np.transpose(v, (1, 2, 0)), chunks=(16, 16, 8))
                else:
                    write_tiff(os.path.join(folder, name + '.tif'), np.transpose(v, (1, 2, 0)))
        return folder
    ds = dataset(os.path.join(tmp, 'ds'))
    stem = 'Iter_0000_CamA_ch0_0000001000msecAbs'
    voxel = (0.25, 0.108, 0.108)                # (dz, dy, dx)

    # The MATLAB wrapper's command line, then its rename
    cli_dir = os.path.join(tmp, 'cli')
    args = ['-P', 'CamA,CamB', '-F', ds + '/', '-n', stem + '_abcd', '-r', 'zarr', '-v',
            '%.20f,%.20f,%.20f,' % (voxel[1], voxel[2], voxel[0]), '-o', cli_dir + '/', '-b', '64,64,64,']
    rc = piw.run_cli(args)
    cli = os.path.join(cli_dir, stem + '.ims')
    if rc == 0:
        os.replace(os.path.join(cli_dir, stem + '_abcd.ims'), cli)
    result = piw.convert_dataset(ds, channel_patterns=('CamA', 'CamB'), zarr_file=True, voxel_size=voxel)
    expect = os.path.join(ds, 'imaris', stem + '.ims')
    check(rc == 0 and result == [expect] and os.path.isfile(expect),
          f'convert_dataset: <folder>/imaris/<first file>.ims ({result})')
    check(rc == 0 and same_file(cli, expect), "convert_dataset: the file of the MATLAB wrapper's command line")
    info = piw.ims_info(expect)
    check(info['channels'] == 2 and info['timepoints'] == 3 and info['channel_names'] == ['CamA', 'CamB'] and
          np.allclose(info['voxel_size'], voxel) and
          all(np.array_equal(piw.read_ims(expect, timepoint=t, channel=c), vols[c, t]) for c in range(2)
              for t in range(3)) and
          np.allclose([seconds(p) - seconds(info['time_points'][0]) for p in info['time_points']], (0, 2.5, 5)),
          f'convert_dataset: channels, data, voxel size, time points from the names ({info["time_points"]})')
    check(leftovers(os.path.join(ds, 'imaris')) == [stem + '.ims'],
          'convert_dataset: only the .ims in the result folder')

    # overwrite=False leaves an existing output alone; True writes it again
    os.utime(expect, ns=(10 ** 18, 10 ** 18))
    again = piw.convert_dataset(ds, channel_patterns=('CamA', 'CamB'), zarr_file=True, voxel_size=voxel)
    check(again == [expect] and mtime_ns(expect) == 10 ** 18, 'a second call: the path, nothing written')
    piw.convert_dataset(ds, channel_patterns=('CamA', 'CamB'), zarr_file=True, voxel_size=voxel, overwrite=True)
    check(mtime_ns(expect) != 10 ** 18 and same_file(cli, expect), 'overwrite=True: written again, the same file')

    # result_dir_name; TIFF datasets (the default); defaults of the MATLAB wrapper (0.108, its patterns)
    r = piw.convert_dataset(ds, channel_patterns=['CamB'], zarr_file=True, result_dir_name='other')
    check(r == [os.path.join(ds, 'other', 'Iter_0000_CamB_ch0_0000001000msecAbs.ims')] and
          np.allclose(piw.ims_info(r[0])['voxel_size'], 0.108), 'result_dir_name; voxel_size defaults to 0.108')
    tds = dataset(os.path.join(tmp, 'tds'), zarr=False)
    r = piw.convert_dataset(tds, channel_patterns=('CamA', 'CamB'), voxel_size=voxel)
    check(r == [os.path.join(tds, 'imaris', stem + '.ims')] and crcs(cli) == crcs(r[0]),
          'a TIFF dataset: the data of the Zarr one')
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        r = piw.convert_dataset(tds, zarr_file=True)
    check(r == [None] and any('does not contain' in str(x.message) for x in w),
          "no file with the default patterns ('CamA_ch0', 'CamA_ch1', 'CamB_ch0') and zarr_file: None, a warning")

    # A list of datasets, one of them in two folders
    ds2 = dataset(os.path.join(tmp, 'ds2'))
    parts = [dataset(os.path.join(tmp, 'part1'), times=[0]), dataset(os.path.join(tmp, 'part2'), times=[1, 2])]
    empty = os.path.join(tmp, 'empty')
    os.makedirs(empty)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        r = piw.convert_dataset([ds2, parts, empty], channel_patterns=('CamA', 'CamB'), zarr_file=True,
                                voxel_size=voxel)
    check(r == [os.path.join(ds2, 'imaris', stem + '.ims'), os.path.join(parts[0], 'imaris', stem + '.ims'), None] and
          len([x for x in w if 'does not contain' in str(x.message)]) == 1, f'a list of datasets: {r}')
    check(r[0] and crcs(cli) == crcs(r[0]) and r[1] and crcs(cli) == crcs(r[1]),
          'a dataset in two folders: the data of one folder holding them all')

    # Inputs of different sizes: refused before any output, unless input_bbox lies inside all of them
    odd = dataset(os.path.join(tmp, 'odd'), sizes=lambda c, t: vols[c, t][:, :35] if t == 1 else vols[c, t])
    try:
        piw.convert_dataset(odd, channel_patterns=('CamA', 'CamB'), zarr_file=True)
        check(False, 'inputs of different sizes: no exception')
    except ValueError as e:
        check('Iter_0000_CamA' in str(e) and 'Iter_0001_CamA' in str(e) and
              not [f for f in os.listdir(os.path.join(odd, 'imaris')) if f.endswith('.ims')],
              f'inputs of different sizes: ValueError naming two of them, no .ims ({e})')
    box = (1, 2, 3, 12, 35, 30)
    r = piw.convert_dataset(odd, channel_patterns=('CamA', 'CamB'), zarr_file=True, input_bbox=box)
    check(r[0] is not None and all(np.array_equal(piw.read_ims(r[0], timepoint=t, channel=c),
                                                  vols[c, t][1:12, 2:35, 3:30]) for c in range(2) for t in range(3)),
          'a box inside all of them converts: the crops')
    raises(FileNotFoundError, lambda: piw.convert_dataset(os.path.join(tmp, 'none'), zarr_file=True),
           'a dataset folder that does not exist')


# ---- roundtrip

def test_roundtrip(tmp):
    voxel = (0.5, 0.2, 0.1)    # (dz, dy, dx)
    for dtype in TYPES:
        name = np.dtype(dtype).name
        for order in 'CF':
            a = sample((13, 37, 29) if order == 'C' else (37, 29, 13), dtype, seed=3)
            f = os.path.join(tmp, f'{name}_{order}.ims')
            piw.write_ims(f, a, voxel_size=voxel if order == 'C' else (0.2, 0.1, 0.5), order=order)
            back = piw.read_ims(f, order=order)
            info = piw.ims_info(f, order=order)
            expect_voxel = voxel if order == 'C' else (0.2, 0.1, 0.5)
            check(back.dtype == a.dtype and np.array_equal(back, a) and back.flags[f'{order}_CONTIGUOUS'],
                  f'{name} {a.shape} order={order}: read_ims equals the data')
            check(info['shape'] == a.shape and info['dtype'] == a.dtype and info['channels'] == 1 and
                  info['timepoints'] == 1 and np.allclose(info['voxel_size'], expect_voxel) and
                  np.allclose(info['extent_max'], np.multiply(a.shape, expect_voxel)) and
                  np.allclose(info['extent_min'], 0),
                  f'{name} order={order}: ims_info shape, type, voxel size, extents '
                  f'({info["shape"]}, {info["voxel_size"]}, {info["extent_max"]})')
            # The other order reads the same file as the other axes
            check(np.array_equal(piw.read_ims(f, order='F' if order == 'C' else 'C'),
                                 np.transpose(a, (1, 2, 0) if order == 'C' else (2, 0, 1))),
                  f'{name} order={order}: read with the other order is the transpose')

        # 5-D (t, c, z, y, x) and 4-D F-order (y, x, z, c), odd sizes; every volume read back
        a5 = sample((2, 3, 5, 17, 23), dtype, seed=5)
        f = os.path.join(tmp, f'{name}_5d.ims')
        piw.write_ims(f, a5)
        info = piw.ims_info(f)
        check(info['channels'] == 3 and info['timepoints'] == 2 and info['shape'] == (5, 17, 23) and
              all(np.array_equal(piw.read_ims(f, timepoint=t, channel=c), a5[t, c])
                  for t in range(2) for c in range(3)), f'{name} 5-D (t, c, z, y, x) {a5.shape}')
        a4 = sample((19, 7, 11, 2), dtype, seed=6)
        f = os.path.join(tmp, f'{name}_4dF.ims')
        piw.write_ims(f, a4, order='F')
        info = piw.ims_info(f, order='F')
        check(info['channels'] == 2 and info['timepoints'] == 1 and info['shape'] == (19, 7, 11) and
              all(np.array_equal(piw.read_ims(f, channel=c, order='F'), a4[..., c]) for c in range(2)),
              f'{name} 4-D order=F (y, x, z, c) {a4.shape}')

    # 2-D: one plane
    p = sample((31, 45), np.uint8)
    f = os.path.join(tmp, 'plane_C.ims')
    piw.write_ims(f, p)
    check(np.array_equal(piw.read_ims(f), p[np.newaxis]), '2-D (y, x): read back as (1, y, x)')
    f = os.path.join(tmp, 'plane_F.ims')
    piw.write_ims(f, p, order='F')
    check(np.array_equal(piw.read_ims(f, order='F'), p[:, :, np.newaxis]), '2-D order=F (y, x): read back as (y, x, 1)')
    # Tiny volumes: one voxel per axis at most a few
    for shape in ((1, 2, 3), (1, 1, 1), (3, 1, 1)):
        a = sample(shape, np.uint16, seed=2)
        f = os.path.join(tmp, 'tiny.ims')
        piw.write_ims(f, a)
        check(np.array_equal(piw.read_ims(f), a), f'tiny volume {shape}')

    # Lists: channels, and data[timepoint][channel], both orders
    chans = [sample((9, 14, 6), np.float32, seed=s) for s in range(3)]
    f = os.path.join(tmp, 'list.ims')
    piw.write_ims(f, chans)
    check(piw.ims_info(f)['channels'] == 3 and all(np.array_equal(piw.read_ims(f, channel=c), v)
                                                   for c, v in enumerate(chans)), 'a list of 3 (z, y, x) channels')
    grid = [[sample((14, 6, 9), np.uint16, seed=10 * t + c) for c in range(2)] for t in range(3)]
    f = os.path.join(tmp, 'nested.ims')
    piw.write_ims(f, grid, order='F')
    info = piw.ims_info(f, order='F')
    check(info['channels'] == 2 and info['timepoints'] == 3 and
          all(np.array_equal(piw.read_ims(f, timepoint=t, channel=c, order='F'), grid[t][c])
              for t in range(3) for c in range(2)), 'data[timepoint][channel], order=F')

    # Every resolution level, regions and out=
    a = smooth((70, 300, 260))
    f = os.path.join(tmp, 'levels.ims')
    piw.write_ims(f, a, num_threads=4)
    info = piw.ims_info(f)
    levels = info['resolution_levels']
    check(levels >= 2 and len(info['datasets']) == levels, f'{a.shape}: {levels} resolution levels')
    ok = True
    for d in info['datasets']:
        v = piw.read_ims(f, resolution_level=d['resolution_level'])
        ok &= v.shape == d['shape']
    check(ok, 'read_ims of every level has the shape ims_info gives ' +
          str([d['shape'] for d in info['datasets']]))
    level1 = piw.read_ims(f, resolution_level=1).astype(np.float64)
    expect = a[:a.shape[0] // 2 * 2, :a.shape[1] // 2 * 2, :a.shape[2] // 2 * 2].astype(np.float64)
    expect = expect.reshape(a.shape[0] // 2, 2, a.shape[1] // 2, 2, a.shape[2] // 2, 2).mean(axis=(1, 3, 5))
    check(info['datasets'][1]['shape'] == expect.shape and np.array_equal(level1, np.floor(expect)),
          'level 1 is the truncated 2x2x2 mean of level 0')
    region = piw.read_ims(f, start_coords=(3, 50, 7), end_coords=(61, 221, 199))
    check(np.array_equal(region, a[3:61, 50:221, 7:199]), 'a region (z, y, x)')
    region = piw.read_ims(f, start_coords=(50, 7, 3), end_coords=(221, 199, 61), order='F')
    check(np.array_equal(region, np.transpose(a, (1, 2, 0))[50:221, 7:199, 3:61]), 'a region, order=F')
    check(np.array_equal(piw.read_ims(f, end_coords=(0, 0, 0)), a), 'end_coords all 0: to the end')
    out = np.zeros((58, 171, 192), np.uint16)
    check(piw.read_ims(f, start_coords=(3, 50, 7), end_coords=(61, 221, 199), out=out) is out and
          np.array_equal(out, a[3:61, 50:221, 7:199]), 'out= is filled and returned')
    big = np.zeros((80, 200, 230), np.uint16)
    view = big[10:68, 20:191, 30:222]
    piw.read_ims(f, start_coords=(3, 50, 7), end_coords=(61, 221, 199), out=view, num_threads=2)
    check(np.array_equal(view, a[3:61, 50:221, 7:199]) and not big[:10].any() and not big[68:].any(),
          'out= a strided view of a bigger array: only its elements written')


# ---- layout

def test_layout(tmp):
    a = sample((21, 45, 38), np.uint16, seed=1)

    def write(name, data, **kw):
        f = os.path.join(tmp, name + '.ims')
        piw.write_ims(f, data, start_time=START, **kw)
        return f

    ref = write('c', a, voxel_size=(0.3, 0.2, 0.1))
    check(same_file(ref, write('c2', a.copy(), voxel_size=(0.3, 0.2, 0.1))), 'the same data twice: the same file')
    cases = {
        'F-contiguous': np.asfortranarray(a),
        'reversed axes': np.ascontiguousarray(a[::-1, ::-1])[::-1, ::-1],
        'other byte order': a.astype(a.dtype.newbyteorder('>' if sys.byteorder == 'little' else '<')),
    }
    buf = np.zeros(a.nbytes + 1, np.uint8)
    unaligned = np.ndarray(a.shape, a.dtype, buffer=buf, offset=1)
    unaligned[...] = a
    cases['unaligned'] = unaligned
    readonly = a.copy()
    readonly.flags.writeable = False
    cases['read-only'] = readonly
    cases['(y, x, z)-contiguous memory'] = np.transpose(np.ascontiguousarray(np.transpose(a, (1, 2, 0))), (2, 0, 1))
    cases['z-fastest memory'] = np.transpose(np.ascontiguousarray(np.transpose(a, (1, 2, 0)).T), (0, 2, 1))
    for what, data in cases.items():
        check(np.array_equal(data, a) and same_file(ref, write('case', data, voxel_size=(0.3, 0.2, 0.1))),
              f'{what} (z, y, x): the file of the C-contiguous array')
    # Strided views and their C-contiguous copies
    big = sample((44, 50, 117), np.uint16, seed=4)
    for what, v in (('every other z, every third x', big[::2, 3:48, ::3]), ('a block', big[5:26, 2:47, 40:78]),
                    ('reversed', big[::-2, ::-1, 100:10:-3])):
        check(same_file(write('view', v), write('copy', np.ascontiguousarray(v))), f'strided view ({what}) equals its copy')
    # The same memory read as (y, x, z) is another image
    other = write('as_f', np.asfortranarray(a), order='F', voxel_size=(0.3, 0.2, 0.1))
    check(crcs(other) != crcs(ref) and piw.ims_info(other, order='F')['shape'] == a.shape,
          "the same memory with order='F' is another (y, x, z) image: another file")
    # (y, x, z) with order='F', and voxel sizes as a mapping
    check(same_file(ref, write('yxz', np.transpose(a, (1, 2, 0)), order='F', voxel_size=(0.2, 0.1, 0.3))),
          "np.transpose(a, (1, 2, 0)) with order='F' equals a (z, y, x)")
    check(same_file(ref, write('yxzF', np.asfortranarray(np.transpose(a, (1, 2, 0))), order='F',
                               voxel_size=(0.2, 0.1, 0.3))), "an F-contiguous (y, x, z) copy with order='F' too")
    check(same_file(ref, write('map', a, voxel_size={'x': 0.1, 'y': 0.2, 'z': 0.3})), 'voxel_size as a mapping')
    # block_size and num_threads do not change the file
    for b in ((16, 32, 8), (7, 9, 11), (64, 64, 64), (21, 45, 38)):
        check(crcs(write('block', a, block_size=b, voxel_size=(0.3, 0.2, 0.1))) == crcs(ref), f'block_size {b}')
    for n in (1, 3):
        check(crcs(write('threads', a, num_threads=n, voxel_size=(0.3, 0.2, 0.1))) == crcs(ref), f'num_threads={n}')
    # Arrays and lists of the same volumes
    a5 = sample((3, 2, 7, 12, 10), np.uint16, seed=7)
    f5 = write('a5', a5)
    check(same_file(f5, write('nested', [[a5[t, c] for c in range(2)] for t in range(3)])),
          'data[timepoint][channel] equals the (t, c, z, y, x) array')
    check(same_file(f5, write('a5F', np.transpose(a5, (3, 4, 2, 1, 0)), order='F')),
          "(y, x, z, c, t) with order='F' equals (t, c, z, y, x)")
    f4 = write('a4', a5[0])
    check(same_file(f4, write('list', [a5[0, 0], a5[0, 1]])), 'a list of channels equals the (c, z, y, x) array')
    swapped = a.astype(a.dtype.newbyteorder('>' if sys.byteorder == 'little' else '<'))
    check(same_file(write('rep', [[a]] * 3), write('rep_swapped', [[swapped]] * 3)),
          'one array given for 3 time points, in either byte order')
    for dtype in (np.uint8, np.uint32, np.float32):
        b = sample((17, 33, 26), dtype, seed=8)
        check(same_file(write('t1', b), write('t2', np.asfortranarray(b)), dtype) and
              same_file(write('t3', b), write('t4', np.transpose(b, (1, 2, 0)), order='F'), dtype),
              f'{np.dtype(dtype).name}: F-contiguous and (y, x, z) order=F equal the C-contiguous (z, y, x)')


# ---- golden

GOLDEN_SHAPE = (2, 48, 160, 140)    # (c, z, y, x) uint16
# (level, timepoint, channel): (crc, histogram_crc), and the thumbnail's crc (computed on Linux)
GOLDEN = {(0, 0, 0): (1842625557, 2840480587), (0, 0, 1): (2767878816, 718280716),
          (1, 0, 0): (3541520938, 1218811845), (1, 0, 1): (3896457786, 2806588309), 'thumbnail': 72015132}


def test_golden(tmp):
    a = np.stack([smooth(GOLDEN_SHAPE[1:], seed=c) for c in range(GOLDEN_SHAPE[0])])
    f = os.path.join(tmp, 'golden.ims')
    piw.write_ims(f, a, voxel_size=(0.25, 0.108, 0.108), start_time=START, num_threads=2)
    info = piw.ims_info(f, checksums=True)
    got = {(d['resolution_level'], d['timepoint'], d['channel']): (d['crc'], d['histogram_crc'])
           for d in info['datasets']}
    got['thumbnail'] = info['thumbnail_crc']
    if GOLDEN is None:
        check(False, f'golden CRCs not set; this platform gives {got!r}')
        return
    for key in sorted(set(GOLDEN) | set(got), key=str):
        check(got.get(key) == GOLDEN.get(key), f'golden {key}: {got.get(key)} (expected {GOLDEN.get(key)})')


# ---- threads

def default_threads_of_siblings():
    # The default thread counts cpp-tiff's and cpp-zarr's modules report, when installed and they have it
    counts = {}
    for name in ('cpptiff', 'cppzarr'):
        try:
            module = __import__(f'{name}.{name}', fromlist=['pybind11_default_threads'])
        except ImportError:
            continue
        f = getattr(module, 'pybind11_default_threads', None)
        if f is not None:
            counts[name] = f()
    return counts


def test_threads(tmp):
    a = smooth((64, 512, 400))
    long = [[a]] * 8                    # 8 time points of one volume: a write of about 0.3 s or more
    before, siblings = pybind11_default_threads(), default_threads_of_siblings()
    piw.write_ims(os.path.join(tmp, 'n2.ims'), a, num_threads=2)
    piw.read_ims(os.path.join(tmp, 'n2.ims'), num_threads=3)
    check(pybind11_default_threads() == before and default_threads_of_siblings() == siblings,
          f'num_threads=2 and 3: default thread counts unchanged ({before}, {siblings})')

    # Another thread counts while this one writes
    count = [0]
    stop = threading.Event()

    def counter():
        while not stop.is_set():
            count[0] += 1
    t = threading.Thread(target=counter)
    t.start()
    t0 = time.time()
    try:
        piw.write_ims(os.path.join(tmp, 'gil.ims'), long, num_threads=2)
    finally:
        seconds = time.time() - t0
        stop.set()
        t.join()
    check(count[0] > 10000, f'another Python thread ran during a {seconds:.2f} s write ({count[0]} counts)')

    # Two threads write at once (the library writes one file at a time; both succeed)
    errors = []

    def writer(name, data):
        try:
            piw.write_ims(os.path.join(tmp, name), data, num_threads=2)
        except BaseException as e:
            errors.append(e)
    b = smooth((64, 512, 400), seed=3)
    threads = [threading.Thread(target=writer, args=('w1.ims', a)), threading.Thread(target=writer, args=('w2.ims', b))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(not errors and np.array_equal(piw.read_ims(os.path.join(tmp, 'w1.ims')), a) and
          np.array_equal(piw.read_ims(os.path.join(tmp, 'w2.ims')), b), f'two threads writing at once {errors}')


# ---- progress

class Stop(Exception):
    pass


def test_progress(tmp):
    a = smooth((64, 512, 512))
    long = [[a]] * 40                   # 40 time points of one volume: about 1 s on a cluster node
    calls = []
    t0 = time.monotonic()
    piw.write_ims(os.path.join(tmp, 'p.ims'), long, num_threads=2,
                  progress=lambda f, b: calls.append((time.monotonic(), f, b)))
    seconds = time.monotonic() - t0
    fractions = [c[1] for c in calls]
    gaps = [b[0] - a_[0] for a_, b in zip(calls[:-2], calls[1:-1])]
    check(len(calls) >= 2 and fractions == sorted(fractions) and fractions[-1] == 1.0 and fractions.count(1.0) == 1
          and all(0 <= f <= 1 for f in fractions) and all(isinstance(c[2], int) for c in calls) and calls[-1][2] > 0,
          f'progress: {len(calls)} calls in {seconds:.2f} s, never decreasing, once 1.0 at the end')
    check(len(calls) <= 10 * seconds + 2 and all(g > 0.09 for g in gaps),
          f'progress: at most about ten calls a second (shortest gap {min(gaps) if gaps else 0:.3f} s)')
    check(leftovers(tmp) == ['p.ims'], 'progress: the file under its final name only')

    # A callback that raises stops the write: its exception, no file
    for when in (2, 'final'):
        folder = os.path.join(tmp, f'stop_{when}')
        os.makedirs(folder)
        seen = []

        def stopping(f, b):
            seen.append(f)
            if (when == 'final' and f == 1.0) or len(seen) == when:
                raise Stop(f'stopped at {f}')
        try:
            piw.write_ims(os.path.join(folder, 's.ims'), long, num_threads=2, progress=stopping)
            check(False, f'a progress callback raising at call {when}: no exception')
        except Stop as e:
            check(not leftovers(folder), f'a progress callback raising at call {when}: Stop({e}), no file left')

    # Ctrl-C
    folder = os.path.join(tmp, 'ctrl_c')
    os.makedirs(folder)
    interrupted, seconds = interrupt(lambda: piw.write_ims(os.path.join(folder, 'c.ims'), long, num_threads=1), 0.3)
    if interrupted:
        check(not leftovers(folder), f'Ctrl-C (simulated): KeyboardInterrupt after {seconds:.2f} s, no file left')
    else:
        skip(f'Ctrl-C (simulated): the write finished first ({seconds:.2f} s)')
    piw.write_ims(os.path.join(folder, 'after.ims'), a[:8], num_threads=2)
    check(leftovers(folder) == (['after.ims'] if interrupted else ['after.ims', 'c.ims']), 'a write after Ctrl-C')

    # verbose
    captured, sys.stdout = sys.stdout, io.StringIO()
    try:
        piw.write_ims(os.path.join(tmp, 'v.ims'), [a[:5, :40, :30]] * 2, verbose=True, voxel_size=(1, 2, 3))
        text = sys.stdout.getvalue()
    finally:
        sys.stdout = captured
    check('Block Sizes: 64,64,64' in text and 'Voxel Sizes: 2.000000,3.000000,1.000000' in text and
          'Current Volume: channel 1, time point 0' in text, f'verbose=True prints the messages ({text!r})')


# ---- metadata

def test_metadata(tmp):
    a = sample((3, 2, 6, 20, 30), np.uint16)
    f = os.path.join(tmp, 'm.ims')
    piw.write_ims(f, a, voxel_size=(2, 0.5, 0.25), channel_names=['CamA', 'CamB'],
                  channel_colors=[(1, 0, 1), (0, 0.5, 1)], emission_wavelengths=[510, None],
                  excitation_wavelengths=[488, 560], time_interval=2.5, start_time=START)
    info = piw.ims_info(f)
    a_ = info['attributes']
    check(info['channel_names'] == ['CamA', 'CamB'], f'channel_names {info["channel_names"]}')
    check([tuple(round(v, 3) for v in c) for c in info['channel_colors']] == [(1, 0, 1), (0, 0.5, 1)],
          f'channel_colors {info["channel_colors"]}')
    check(info['time_points'] == ['2026-01-02 03:04:05', '2026-01-02 03:04:07.500', '2026-01-02 03:04:10'],
          f'start_time, time_interval: {info["time_points"]}')
    check(a_.get('/DataSetInfo/Channel 0/LSMEmissionWavelength') == '510' and
          '/DataSetInfo/Channel 1/LSMEmissionWavelength' not in a_ and
          a_.get('/DataSetInfo/Channel 1/LSMExcitationWavelength') == '560',
          'wavelengths: ' + str({k: v for k, v in a_.items() if 'Wavelength' in k}))
    check(np.allclose(info['voxel_size'], (2, 0.5, 0.25)) and
          np.allclose(piw.ims_info(f, order='F')['voxel_size'], (0.5, 0.25, 2)),
          f'voxel_size (dz, dy, dx) {info["voxel_size"]}; order=F (dy, dx, dz)')
    g = os.path.join(tmp, 'm2.ims')
    piw.write_ims(g, a, voxel_size=(2, 0.5, 0.25), channel_names=['CamA', 'CamB'],
                  channel_colors=[(1, 0, 1), (0, 0.5, 1)], emission_wavelengths=[510, 0],
                  excitation_wavelengths=[488, 560], time_interval=2.5,
                  start_time=datetime.datetime(2026, 1, 2, 3, 4, 5))
    check(same_file(f, g), 'start_time as a datetime equals the text')
    h = os.path.join(tmp, 'default.ims')
    before = datetime.datetime.now().replace(microsecond=0)
    piw.write_ims(h, a[0, 0])
    stamp = datetime.datetime.strptime(piw.ims_info(h)['time_points'][0][:19], '%Y-%m-%d %H:%M:%S')
    check(abs((stamp - before).total_seconds()) < 120 and piw.ims_info(h)['channel_names'] == ['Channel 0'],
          f'defaults: now ({stamp}), "Channel 0"')
    raises(ValueError, lambda: piw.write_ims(h, a, channel_names=['one']), 'one channel name for two channels')
    raises(ValueError, lambda: piw.write_ims(h, a, start_time='yesterday'), 'a start time that is not one')
    raises(ValueError, lambda: piw.write_ims(h, a, channel_colors=[(2, 0, 0), (0, 0, 1)]), 'a color above 1')
    raises(TypeError, lambda: piw.write_ims(h, a, channel_colors=[1, 0, 0]), 'a color that is not a list of colors')
    check(leftovers(tmp) == ['default.ims', 'm.ims', 'm2.ims'], 'refused metadata leaves nothing behind')


# ---- errors

def test_errors(tmp):
    a = sample((5, 20, 30), np.uint16)
    f = os.path.join(tmp, 'e.ims')
    for dtype in (np.int16, np.float64, np.bool_, np.uint64, np.complex64):
        raises(TypeError, lambda: piw.write_ims(f, a.astype(dtype)), f'data type {np.dtype(dtype).name}')
    raises(TypeError, lambda: piw.write_ims(f, [a, a.astype(np.uint8)]), 'volumes of different types')
    raises(ValueError, lambda: piw.write_ims(f, np.zeros((0, 20, 30), np.uint16)), 'an empty array')
    raises(ValueError, lambda: piw.write_ims(f, np.zeros((2, 0, 4, 20, 30), np.uint16)), 'an empty 5-D array')
    raises(ValueError, lambda: piw.write_ims(f, []), 'an empty list')
    raises(ValueError, lambda: piw.write_ims(f, np.zeros(10, np.uint16)), 'a 1-D array')
    raises(ValueError, lambda: piw.write_ims(f, np.zeros((1, 1, 1, 2, 3, 4), np.uint16)), 'a 6-D array')
    raises(ValueError, lambda: piw.write_ims(f, [a, a[:, :19]]), 'volumes of different shapes')
    raises(ValueError, lambda: piw.write_ims(f, [[a, a], [a]]), 'time points with different numbers of channels')
    raises(ValueError, lambda: piw.write_ims(f, [a[0], a[1]]), 'a list of 2-D volumes')
    raises(ValueError, lambda: piw.write_ims(f, [a, [a]]), 'a list mixing volumes and lists')
    raises(ValueError, lambda: piw.write_ims(f, a, order='X'), "order='X'")
    raises(ValueError, lambda: piw.write_ims(os.path.join(tmp, 'e.h5'), a), 'a file name without .ims')
    raises(TypeError, lambda: piw.write_ims(42, a), 'a file name that is a number')
    for n in (0, -1, 1.5, True):
        raises(ValueError, lambda: piw.write_ims(f, a, num_threads=n), f'num_threads={n!r}')
    raises(ValueError, lambda: piw.write_ims(f, a, voxel_size=(1, 2)), 'two voxel sizes')
    raises(ValueError, lambda: piw.write_ims(f, a, voxel_size=(1, -2, 3)), 'a negative voxel size')
    raises(ValueError, lambda: piw.write_ims(f, a, voxel_size={'x': 1, 'y': 2}), 'a voxel size mapping without z')
    raises(ValueError, lambda: piw.write_ims(f, a, block_size=(0, 64, 64)), 'a block size of 0')
    raises(TypeError, lambda: piw.write_ims(f, a, block_size=(1.5, 64, 64)), 'a block size of 1.5')
    raises(TypeError, lambda: piw.write_ims(f, a, progress=42), 'progress that is not callable')
    raises(FileNotFoundError, lambda: piw.write_ims(os.path.join(tmp, 'no', 'such', 'e.ims'), a), 'a missing folder')
    os.makedirs(os.path.join(tmp, 'folder.ims'))
    raises(IsADirectoryError, lambda: piw.write_ims(os.path.join(tmp, 'folder.ims'), a), 'file_name is a folder')
    if POSIX and os.geteuid() != 0:
        locked = os.path.join(tmp, 'locked')
        os.makedirs(locked)
        os.chmod(locked, 0o500)
        try:
            raises(PermissionError, lambda: piw.write_ims(os.path.join(locked, 'e.ims'), a), 'an unwritable folder')
        finally:
            os.chmod(locked, 0o700)
        check(not os.listdir(locked), 'an unwritable folder: nothing in it')
    else:
        skip('an unwritable folder (not on Windows or as root)')
    check(leftovers(tmp) == ['folder.ims', 'locked'] or leftovers(tmp) == ['folder.ims'],
          f'no file left behind by the refused writes ({leftovers(tmp)})')

    # read_ims and ims_info
    piw.write_ims(f, a)
    raises(FileNotFoundError, lambda: piw.read_ims(os.path.join(tmp, 'none.ims')), 'read_ims of a missing file')
    raises(FileNotFoundError, lambda: piw.ims_info(os.path.join(tmp, 'none.ims')), 'ims_info of a missing file')
    with open(os.path.join(tmp, 'text.ims'), 'w') as t:
        t.write('not HDF5\n')
    raises(RuntimeError, lambda: piw.ims_info(os.path.join(tmp, 'text.ims')), 'ims_info of a file that is not HDF5')
    raises(RuntimeError, lambda: piw.read_ims(os.path.join(tmp, 'text.ims')), 'read_ims of a file that is not HDF5')
    raises(ValueError, lambda: piw.read_ims(f, channel=1), 'read_ims of a channel the file does not have')
    raises(ValueError, lambda: piw.read_ims(f, resolution_level=9), 'read_ims of a level the file does not have')
    raises(ValueError, lambda: piw.read_ims(f, end_coords=(6, 20, 30)), 'a region past the end')
    raises(ValueError, lambda: piw.read_ims(f, start_coords=(2, 0, 0), end_coords=(2, 20, 30)), 'an empty region')
    raises(TypeError, lambda: piw.read_ims(f, start_coords=(0.5, 0, 0)), 'a region start of 0.5')
    raises(TypeError, lambda: piw.read_ims(f, out=np.zeros((5, 20, 30), np.float32)), 'out of another type')
    raises(ValueError, lambda: piw.read_ims(f, out=np.zeros((5, 30, 20), np.uint16)), 'out of another shape')
    ro = np.zeros((5, 20, 30), np.uint16)
    ro.flags.writeable = False
    raises(ValueError, lambda: piw.read_ims(f, out=ro), 'a read-only out')
    raises(ValueError, lambda: piw.read_ims(f, out=np.zeros((5, 20, 60), np.uint16)[:, :, ::-2]), 'out with negative strides')
    raises(ValueError, lambda: piw.read_ims(f, num_threads=0), 'read_ims num_threads=0')
    check(np.array_equal(piw.read_ims(f), a), 'the process works after every error')


# ---- h5py

def test_h5py(tmp):
    if h5py is None:
        skip('h5py: h5py and hdf5plugin are not installed')
        return
    a = np.stack([smooth((70, 300, 260), seed=c) for c in range(2)])
    f = os.path.join(tmp, 'h.ims')
    piw.write_ims(f, a)
    info = piw.ims_info(f)
    ok = True
    with h5py.File(f, 'r') as h:
        for d in info['datasets']:
            z, y, x = d['shape']
            data = h[f'DataSet/ResolutionLevel {d["resolution_level"]}/TimePoint {d["timepoint"]}/'
                     f'Channel {d["channel"]}/Data'][:z, :x, :y]          # (Imaris Z, Y, X) = (z, x, y)
            ok &= np.array_equal(np.transpose(data, (0, 2, 1)),
                                 piw.read_ims(f, d['resolution_level'], d['timepoint'], d['channel']))
    check(ok, f'h5py: all {len(info["datasets"])} datasets equal read_ims')


# ---- importorder

IMPORT_ORDER = r'''
import os, sys
order, folder = sys.argv[1], sys.argv[2]

def others(tag):
    import numpy as np, scipy.ndimage, cppzarr, cpptiff
    a = (np.arange(32 * 40 * 48, dtype=np.uint16) % 1000).reshape(32, 40, 48)
    z = os.path.join(folder, f'{order}_{tag}.zarr')
    cppzarr.write_zarr(z, a)
    assert np.array_equal(cppzarr.read_zarr(z), a)
    t = os.path.join(folder, f'{order}_{tag}.tif')
    cpptiff.write_tiff(t, a)
    b = cpptiff.read_tiff(t)
    assert b.size == a.size and int(b.astype(np.int64).sum()) == int(a.astype(np.int64).sum())
    assert scipy.ndimage.uniform_filter(a.astype(np.float32), 3).shape == a.shape

def ours(tag):
    import numpy as np, parallel_imaris_writer as piw
    a = (np.arange(30 * 70 * 90, dtype=np.uint16) % 3000).reshape(30, 70, 90)
    f = os.path.join(folder, f'{order}_{tag}.ims')
    piw.write_ims(f, a, num_threads=2)
    assert np.array_equal(piw.read_ims(f), a)

if order == 'others_first':
    others('1'); ours('1'); others('2'); ours('2')
else:
    ours('1'); others('1'); ours('2'); others('2')
print('IMPORT ORDER OK')
'''


def test_importorder(tmp):
    missing = [m for m in ('scipy', 'cppzarr', 'cpptiff') if importlib.util.find_spec(m) is None]
    if missing:
        skip(f'import order: needs scipy, cpp-zarr and cpp-tiff ({", ".join(missing)} missing)')
        return
    for order in ('others_first', 'piw_first'):
        r = subprocess.run([sys.executable, '-c', IMPORT_ORDER, order, tmp], stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT, universal_newlines=True, timeout=600)
        check(r.returncode == 0 and 'IMPORT ORDER OK' in r.stdout,
              f'import order {order} (numpy, scipy, cppzarr, cpptiff): exit code {r.returncode}' +
              ('' if r.returncode == 0 else '\n' + r.stdout[-3000:]))


# ---- fork

FORK = r'''
import os, sys, time, traceback
import numpy as np
import parallel_imaris_writer as piw
folder = sys.argv[1]
a = (np.arange(48 * 160 * 140, dtype=np.uint32) % 4093).astype(np.uint16).reshape(48, 160, 140)
piw.write_ims(os.path.join(folder, 'parent.ims'), a, num_threads=4)
sys.stdout.flush(); sys.stderr.flush()
pid = os.fork()
if pid == 0:
    code = 1
    try:
        f = os.path.join(folder, 'child.ims')
        piw.write_ims(f, a, num_threads=4)
        code = 0 if np.array_equal(piw.read_ims(f, num_threads=4), a) else 2
    except BaseException:
        traceback.print_exc()
    finally:
        sys.stdout.flush(); sys.stderr.flush()
        os._exit(code)
t0 = time.monotonic()
while time.monotonic() - t0 < 60:
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        print('CHILD', os.waitstatus_to_exitcode(status), 'after %.2f s' % (time.monotonic() - t0))
        sys.exit(0)
    time.sleep(0.05)
os.kill(pid, 9)
os.waitpid(pid, 0)
print('CHILD HUNG')
sys.exit(1)
'''


def test_fork(tmp):
    if not hasattr(os, 'fork'):
        skip('fork: no os.fork here')
        return
    r = subprocess.run([sys.executable, '-c', FORK, tmp], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       universal_newlines=True, timeout=180)
    check(r.returncode == 0 and 'CHILD 0 ' in r.stdout,
          f'fork after a write: a write in the child ends: {r.stdout.strip()[-2000:]}')


# ---- sigint

SIGINT = r'''
import os, sys, time
import numpy as np
import parallel_imaris_writer as piw
folder = sys.argv[1]
z, y, x = np.meshgrid(np.arange(64), np.arange(512), np.arange(512), indexing='ij')
a = ((z * 17 + y * 7 + x * 13) % 4096 + 1).astype(np.uint16)
print('STARTED', flush=True)
t0 = time.monotonic()
# 300 time points of one volume: several seconds (about 8 s on a cluster node)
piw.write_ims(os.path.join(folder, 'sigint.ims'), [[a]] * int(sys.argv[2]), num_threads=2)
print('FINISHED after %.2f s' % (time.monotonic() - t0), flush=True)
'''


def test_sigint(tmp):
    if not POSIX:
        skip('sigint: not on Windows')
        return
    p = subprocess.Popen([sys.executable, '-c', SIGINT, tmp, '300'], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         universal_newlines=True)
    started = p.stdout.readline()
    time.sleep(1.0)
    p.send_signal(signal.SIGINT)
    t0 = time.monotonic()
    try:
        out, err = p.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
    seconds = time.monotonic() - t0
    if 'FINISHED' in out:
        skip(f'sigint: the write finished before the signal ({out.strip()})')
        return
    check('STARTED' in started and p.returncode != 0 and 'KeyboardInterrupt' in err and seconds <= 5 and
          not leftovers(tmp), f'SIGINT 1 s into a write: KeyboardInterrupt after {seconds:.2f} s (exit code '
          f'{p.returncode}), files left: {leftovers(tmp)}' + ('' if 'KeyboardInterrupt' in err else '\n' + err[-2000:]))


# ---- cluster: the cluster steps (cluster_plan, cluster_task, cluster_status, cluster_finish)

CLUSTER_TASK = r'''
import sys
import parallel_imaris_writer as piw
stage = int(sys.argv[2]) if sys.argv[2].isdigit() else sys.argv[2]
r = piw.cluster_task(sys.argv[1], stage, int(sys.argv[3]), num_threads=2)
print('TASK %s %d %s' % (r['stage'], r['index'], 'skipped' if r['skipped'] else 'done'), flush=True)
'''


def cluster_task_process(workdir, stage, index):
    # One task in a Python process of its own: (exit code, stdout, stderr)
    r = subprocess.run([sys.executable, '-c', CLUSTER_TASK, workdir, str(stage), str(index)], stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, universal_newlines=True)
    return r.returncode, r.stdout, r.stderr


def shuffled(n, seed):
    order = list(range(n))
    for i in range(n):
        j = (i * 7 + seed) % n
        order[i], order[j] = order[j], order[i]
    return order


def run_plan_processes(workdir, stages):
    # Every task of every stage, each in a process of its own, stage after stage, shuffled within
    # one: the tasks that failed, as (stage, index, the last line of their stderr)
    failed = []
    for s in stages:
        for i in shuffled(s['tasks'], 3):
            rc, out, err = cluster_task_process(workdir, s['name'], i)
            if rc != 0 or f'TASK {s["name"]} {i} done' not in out:
                failed.append((s['name'], i, (err.strip().splitlines() or [''])[-1][:300]))
    return failed


def run_plan_here(workdir, stages, **kw):
    # Every task in this process
    for s in stages:
        for i in shuffled(s['tasks'], 5):
            piw.cluster_task(workdir, s['name'], i, **kw)


def cli_steps(workdir, plan_args):
    # plan, every task (each a process of its own) and finish through the command line
    # (python -m parallel_imaris_writer): the problems, as text
    def run(args):
        return subprocess.run([sys.executable, '-m', 'parallel_imaris_writer'] + args, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, universal_newlines=True)
    r = run(['plan', workdir] + plan_args)
    if r.returncode != 0:
        return [f'plan: exit code {r.returncode}: {r.stdout[-500:]}']
    with open(os.path.join(workdir, 'manifest.json')) as f:
        stages = json.load(f)['stages']
    problems = []
    for s in stages:
        for i in shuffled(s['tasks'], 1):
            t = run(['task', workdir, s['name'], str(i), '--threads', '2'])
            if t.returncode != 0:
                problems.append(f'task {s["name"]} {i}: exit code {t.returncode}: {t.stdout[-300:]}')
    f = run(['finish', workdir])
    if f.returncode != 0:
        problems.append(f'finish: exit code {f.returncode}: {f.stdout[-500:]}')
    return problems


def test_cluster(tmp):
    if not POSIX:
        # (the single file is not supported there; the packs store is not tested there, as in ctest)
        skip('cluster: not on Windows')
        return
    # Two channels and two time points as PetaKit5D stores them: (y, x, z) F-order Zarrs of
    # (z, y, x) arrays; two resolution levels
    inputs = os.path.join(tmp, 'in')
    os.makedirs(inputs)
    shape = (70, 180, 160)
    vols = {(c, t): sample(shape, np.uint16, seed=40 + 2 * c + t) for c in range(2) for t in range(2)}
    for (c, t), v in vols.items():
        write_zarr(os.path.join(inputs, f'Cam{"AB"[c]}_t{t:04d}.zarr'), np.transpose(v, (1, 2, 0)), chunks=(64, 64, 32))
    voxel = (0.5, 0.2, 0.1)
    box = (3, 5, 7, 66, 170, 150)
    opts = dict(folders=inputs, channel_patterns=['CamA', 'CamB'], voxel_size=voxel, input_bbox=box,
                time_interval=1.5, start_time=START)
    ref = os.path.join(tmp, 'ref.ims')
    piw.convert_to_ims(ref, process_block_size=(20, 64, 64), **opts)
    ref_info = piw.ims_info(ref)
    check(ref_info['resolution_levels'] >= 2, f'the reference has {ref_info["resolution_levels"]} resolution levels')
    # the command line's options for the same conversion: along the inputs' (y, x, z)
    cli_opts = ['-F', inputs, '-P', 'CamA,CamB', '-r', 'zarr', '-v', '0.2,0.1,0.5', '-B', '5,7,3,170,150,66',
                '--time-interval', '1.5', '--start-time', START]
    # ---- the plan
    wd = os.path.join(tmp, 'wd')
    out = os.path.join(tmp, 'series.ims')
    plan = piw.cluster_plan(wd, file_name=out, process_block_size=(20, 64, 64), **opts)
    check(plan['kind'] == 'series' and plan['store'] == 'mono' and plan['timepoints'] == 2 and
          plan['channels'] == 2 and plan['shape'] == (63, 165, 143) and plan['dtype'] == np.uint16 and
          plan['resolution_levels'] == ref_info['resolution_levels'] and
          os.path.normpath(plan['file_name']) == out and os.path.normpath(plan['workdir']) == wd and
          plan['stages'] == [{'name': 'convert', 'tasks': 4, 'after': None}] and plan['tasks'] == 4,
          f'cluster_plan of a time series: {plan}')
    check(piw.cluster_plan(wd, file_name=out, process_block_size=(20, 64, 64), **opts) == plan,
          'cluster_plan again with the same arguments: the plan is kept')
    raises(ValueError, lambda: piw.cluster_plan(wd, file_name=out, **opts), 'cluster_plan of another plan '
           'into the same workdir')
    st = piw.cluster_status(wd)
    check(st['kind'] == 'series' and st['missing'] == 4 and st['stages'] == [
          {'name': 'convert', 'tasks': 4, 'missing': [0, 1, 2, 3]}] and not st['sealed'] and not st['finished'],
          f'cluster_status of the new plan: {st}')
    raises(FileNotFoundError, lambda: piw.cluster_finish(wd), 'cluster_finish before any task')

    # ---- every task in a process of its own; task 2 = (t=1, c=0) has a truncated chunk
    chunk = os.path.join(inputs, 'CamA_t0001.zarr', '1.1.1')
    with open(chunk, 'rb') as f:
        whole = f.read()
    with open(chunk, 'r+b') as f:
        f.truncate(100)
    failed = run_plan_processes(wd, plan['stages'])
    check([(s, i) for s, i, _ in failed] == [('convert', 2)] and failed[0][2].startswith('RuntimeError'),
          f'every task in a process of its own: only task 2 (its truncated input) failed: {failed}')
    st = piw.cluster_status(wd)
    check(st['missing'] == 1 and st['stages'][0]['missing'] == [2], f'cluster_status names task 2: {st}')
    try:
        piw.cluster_finish(wd)
        check(False, 'cluster_finish with task 2 not done: no FileNotFoundError')
    except FileNotFoundError as e:
        check('convert 2' in str(e), f'cluster_finish with task 2 not done: FileNotFoundError: {e}')
    with open(chunk, 'wb') as f:
        f.write(whole)
    # task 2 again, here, by the stage's number, with another process block, verbose and progress
    captured, sys.stdout = sys.stdout, io.StringIO()
    values = []
    try:
        r = piw.cluster_task(wd, 0, 2, num_threads=2, process_block_size=(16, 40, 50), verbose=True,
                             progress=lambda f, b: values.append(f))
        text = sys.stdout.getvalue()
    finally:
        sys.stdout = captured
    check(r['stage'] == 'convert' and r['index'] == 2 and not r['skipped'] and r['timepoint'] == 1 and
          r['channel'] == 0 and r['owners'] == ['P_C0_T1'] and r['chunks'] > 0 and r['bytes'] > 0,
          f'task 2 once its input is whole: {r}')
    check('Current File: ' in text and values and values[-1] == 1.0 and values == sorted(values),
          f'cluster_task(verbose=True, progress=...): the messages, progress ends at 1.0 ({len(values)} calls)')
    check(piw.cluster_task(wd, 'convert', 0)['skipped'], 'a task that is done is skipped')
    st = piw.cluster_status(wd)
    check(st['missing'] == 0 and not st['sealed'], 'cluster_status: every task done')
    f = piw.cluster_finish(wd, verify=True, num_threads=2)
    check(os.path.normpath(f['file_name']) == out and not f['already_finished'] and f['timepoints'] == 2 and
          f['channels'] == 2 and f['resolution_levels'] == ref_info['resolution_levels'] and f['chunks'] > 0,
          f'cluster_finish: {f}')
    check(same_file(ref, out), 'the time-series plan\'s file: the CRCs and attributes of convert_to_ims of the '
                               'same inputs')
    st = piw.cluster_status(wd)
    check(st['finished'] and st['sealed'] and os.path.normpath(st['file_name']) == out,
          f'cluster_status after the finish: {st}')
    check(piw.cluster_finish(wd)['already_finished'], 'cluster_finish again: already finished')
    raises(ValueError, lambda: piw.cluster_task(wd, 'convert', 1), 'a task after the finish')

    # ---- the same steps through the command line
    wdc = os.path.join(tmp, 'wd_cli')
    problems = cli_steps(wdc, cli_opts + ['-C', '64,64,20', '-o', tmp, '-n', 'series_cli'])
    check(not problems and same_file(out, os.path.join(tmp, 'series_cli.ims')),
          f'plan, task and finish through the command line: the same file {problems}')

    # ---- (z, y, x) C-order Zarrs (input_axes from their order), also with x_on_imaris_x
    czyx = os.path.join(tmp, 'zyx')
    os.makedirs(czyx)
    small = {(c, t): sample((30, 70, 50), np.uint16, seed=60 + 2 * c + t) for c in range(2) for t in range(2)}
    for (c, t), v in small.items():
        write_zarr(os.path.join(czyx, f'Cam{"AB"[c]}_t{t:04d}.zarr'), v, chunks=(16, 32, 32), order='C')
    for x_on_x in (False, True):
        name = 'zyx_x' if x_on_x else 'zyx'
        kw = dict(folders=czyx, channel_patterns=['CamA', 'CamB'], voxel_size=voxel, input_bbox=(2, 3, 4, 28, 66, 45),
                  x_on_imaris_x=x_on_x, start_time=START)
        zref = os.path.join(tmp, name + '_ref.ims')
        piw.convert_to_ims(zref, **kw)
        wdz = os.path.join(tmp, 'wd_' + name)
        p = piw.cluster_plan(wdz, **kw)
        run_plan_here(wdz, p['stages'], num_threads=2)
        fz = piw.cluster_finish(wdz)
        check(p['shape'] == (26, 63, 41) and os.path.normpath(fz['file_name']) == os.path.join(wdz, 'output.ims') and
              same_file(zref, fz['file_name']) and
              np.array_equal(piw.read_ims(fz['file_name'], timepoint=1, channel=1, x_on_imaris_x=x_on_x),
                             small[1, 1][2:28, 3:66, 4:45]),
              f'(z, y, x) Zarrs{" with x_on_imaris_x" if x_on_x else ""}, the tasks run here: the file of '
              f'convert_to_ims (shape {p["shape"]})')

    # ---- Ctrl-C (simulated, 0.05 s in) stops a task: KeyboardInterrupt, nothing staged left, the
    # task still to do
    big = os.path.join(tmp, 'big')
    os.makedirs(big)
    write_zarr(os.path.join(big, 'CamA.zarr'), zvolume(2, (400, 400, 800)), chunks=(128, 128, 64))
    wdb = os.path.join(tmp, 'wd_big')
    stage = os.path.join(tmp, 'stage')
    piw.cluster_plan(wdb, files=os.path.join(big, 'CamA.zarr'))
    interrupted, seconds = interrupt(lambda: piw.cluster_task(wdb, 'convert', 0, num_threads=1, stage_dir=stage),
                                     0.05)
    done = piw.cluster_status(wdb)['missing'] == 0
    if interrupted and not done:
        check(not leftovers(stage), f'Ctrl-C: KeyboardInterrupt after {seconds:.2f} s, the staged part removed '
                                    f'({leftovers(stage)}), the task not done')
    elif interrupted:
        skip(f'Ctrl-C: the cancel came after the conversion ({seconds:.2f} s); the task is done')
    else:
        skip(f'Ctrl-C: the task finished first ({seconds:.2f} s)')

    # ---- tile plans, two tiles per task: the single file and the packs store
    tile_opts = dict(opts, tile_size=(64, 96, 96), tiles_per_task=2)
    wdt = os.path.join(tmp, 'wd_tiles')
    outt = os.path.join(tmp, 'tiles.ims')
    plan = piw.cluster_plan(wdt, file_name=outt, **tile_opts)
    names = [s['name'] for s in plan['stages']]
    check(plan['kind'] == 'tiles' and plan['store'] == 'mono' and plan['shape'] == (63, 165, 143) and
          len(names) == plan['resolution_levels'] == ref_info['resolution_levels'] and
          names == [f'level{r}' for r in range(len(names))] and
          [s['after'] for s in plan['stages']] == [None] + names[:-1] and plan['stages'][0]['tasks'] >= 2,
          f'cluster_plan of tiles: {plan}')
    raises(FileNotFoundError, lambda: piw.cluster_task(wdt, 'level1', 0), 'a level-1 task before level 0')
    failed = run_plan_processes(wdt, plan['stages'])
    check(not failed, f'every tile task in a process of its own, level after level: {failed}')
    piw.cluster_finish(wdt)
    check(same_file(ref, outt), "the tile plan's file: the CRCs and attributes of convert_to_ims")
    wdtc = os.path.join(tmp, 'wd_tiles_cli')
    problems = cli_steps(wdtc, cli_opts + ['--tiles', '96,96,64', '--tiles-per-task', '2', '-o', tmp,
                                           '-n', 'tiles_cli'])
    check(not problems and same_file(outt, os.path.join(tmp, 'tiles_cli.ims')),
          f'the tile plan through the command line: the same file {problems}')
    # another chunk budget: other chunks, the same data
    wdcb = os.path.join(tmp, 'wd_budget')
    p = piw.cluster_plan(wdcb, chunk_bytes=1 << 18, **dict(tile_opts, tiles_per_task=4))
    run_plan_here(wdcb, p['stages'], num_threads=2)
    fb = piw.cluster_finish(wdcb)['file_name']
    binfo = piw.ims_info(fb)
    check([d['chunk_shape'] for d in binfo['datasets']] != [d['chunk_shape'] for d in ref_info['datasets']] and
          all(np.array_equal(piw.read_ims(fb, level, t, c), piw.read_ims(ref, level, t, c))
              for level in range(ref_info['resolution_levels']) for t in range(2) for c in range(2)),
          'chunk_bytes=262144: smaller chunks, the same data at every level')
    wdp = os.path.join(tmp, 'wd_packs')
    outp = os.path.join(tmp, 'packs.ims')
    p = piw.cluster_plan(wdp, file_name=outp, store='packs', **tile_opts)
    run_plan_here(wdp, p['stages'], num_threads=2)
    piw.cluster_finish(wdp)
    check(p['store'] == 'packs' and same_file(ref, outp), "the packs store's file, the tasks run here: the CRCs and "
                                                          "attributes of convert_to_ims")

    # ---- errors
    raises(FileNotFoundError, lambda: piw.cluster_status(os.path.join(tmp, 'none')), 'cluster_status of no plan')
    raises(FileNotFoundError, lambda: piw.cluster_task(os.path.join(tmp, 'none'), 'convert', 0),
           'cluster_task of no plan')
    raises(ValueError, lambda: piw.cluster_task(wdp, 'convert', 0), 'a stage the plan does not have')
    raises(ValueError, lambda: piw.cluster_task(wdp, 'level0', 10 ** 6), 'a task index out of range')
    raises(ValueError, lambda: piw.cluster_task(wdp, 'level0', -1), 'a negative task index')
    raises(ValueError, lambda: piw.cluster_task(wdp, 'level0', 0, process_block_size=(8, 8, 8)),
           'process_block_size for a tile task')
    e = os.path.join(tmp, 'wd_err')
    raises(ValueError, lambda: piw.cluster_plan(e, store='packs', **opts), 'store without tile_size')
    raises(ValueError, lambda: piw.cluster_plan(e, chunk_bytes=1 << 20, **opts), 'chunk_bytes without tile_size')
    raises(ValueError, lambda: piw.cluster_plan(e, chunk_bytes=100, tile_size=(8, 8, 8), **opts), 'chunk_bytes=100')
    raises(ValueError, lambda: piw.cluster_plan(e, tile_size=(8, 8), **opts), 'a tile_size of two values')
    raises(ValueError, lambda: piw.cluster_plan(e, store='disk', tile_size=(8, 8, 8), **opts), "store='disk'")
    raises(ValueError, lambda: piw.cluster_plan(e, tile_size=(8, 8, 8), x_on_imaris_x=True, **opts),
           'a tile plan with x_on_imaris_x')
    raises(ValueError, lambda: piw.cluster_plan(e, folders=czyx, channel_patterns='CamA', tile_size=(8, 8, 8)),
           'a tile plan of (z, y, x) Zarrs')
    raises(ValueError, lambda: piw.cluster_plan(e, file_name=os.path.join(tmp, 'x.h5'), **opts),
           'a file_name without .ims')
    raises(FileNotFoundError, lambda: piw.cluster_plan(e, folders=inputs, channel_patterns='CamC'),
           'a pattern without a match')
    check(not os.path.exists(e), 'no workdir made by the refused plans')


SECTIONS = [('cli', test_cli), ('convert', test_convert), ('dataset', test_dataset), ('roundtrip', test_roundtrip),
            ('layout', test_layout), ('golden', test_golden), ('threads', test_threads), ('progress', test_progress),
            ('metadata', test_metadata), ('errors', test_errors), ('h5py', test_h5py), ('importorder', test_importorder),
            ('fork', test_fork), ('sigint', test_sigint), ('cluster', test_cluster)]


def main(names):
    unknown = [n for n in names if n not in dict(SECTIONS)]
    if unknown:
        sys.exit(f'unknown sections {unknown}; the sections are {[n for n, _ in SECTIONS]}')
    t0 = time.time()
    for name, test in SECTIONS:
        if names and name not in names:
            continue
        print(f'== {name}', flush=True)
        tmp = tempfile.mkdtemp()
        t1 = time.time()
        try:
            test(tmp)
        except Exception as e:
            import traceback
            traceback.print_exc()
            check(False, f'{name}: {type(e).__name__}: {e}')
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        print(f'   ({name}: {time.time() - t1:.1f} s)', flush=True)
    if failures:
        print(f'{len(failures)} FAILED:\n  ' + '\n  '.join(f.splitlines()[0] for f in failures))
        sys.exit('parallel_imaris_writer Python test FAILED')
    print(f'parallel_imaris_writer Python test passed ({time.time() - t0:.1f} s)')


if __name__ == '__main__':
    main(sys.argv[1:])
