"""Smoke test for the parallel-imaris-writer Python wheel.

Writes small synthetic Zarr v2 volumes (uncompressed, written here with NumPy alone) and converts
them to .ims in this process: through the command line (run_cli, a second time with -C, and
python -m parallel_imaris_writer, also with an output it cannot write), and through the library
call (pybind11_convert) with its input selection (pybind11_find_input_files), its messages, the
opt-in x-on-Imaris-X axis mapping, its errors as exceptions and a Ctrl-C that stops it. Checks the package version against the CMake project's. With h5py and hdf5plugin
installed it also reads the files back and compares the voxels. Run automatically against each
built wheel, so it also verifies that the wheel imports and bundles its native libraries. Exits
non-zero on any failure.
"""
import _thread
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

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
SHAPE = (101, 83, 47)   # dims 0, 1, 2 = Imaris X, Y, Z (y, x, z); odd sizes pad every axis
CHUNKS = (64, 64, 32)


def volume(seed, shape=SHAPE):
    y, x, z = np.meshgrid(*(np.arange(n) for n in shape), indexing='ij')
    return ((y * 7 + x * 13 + z * 17 + seed * 137) % 4096 + 1).astype(np.uint16)


def write_zarr(path, data, chunks=CHUNKS):
    # An uncompressed Zarr v2 array in F order (MATLAB's layout), chunk files named i.j.k
    os.makedirs(path)
    meta = {'zarr_format': 2, 'shape': list(data.shape), 'chunks': list(chunks), 'dtype': data.dtype.str,
            'order': 'F', 'compressor': None, 'fill_value': 0, 'filters': None, 'dimension_separator': '.'}
    with open(os.path.join(path, '.zarray'), 'w') as f:
        json.dump(meta, f)
    counts = [-(-n // c) for n, c in zip(data.shape, chunks)]
    for index in np.ndindex(*counts):
        chunk = np.zeros(chunks, data.dtype)   # edge chunks are stored whole
        region = tuple(slice(i * c, min((i + 1) * c, n)) for i, c, n in zip(index, chunks, data.shape))
        part = data[region]
        chunk[tuple(slice(0, s) for s in part.shape)] = part
        with open(os.path.join(path, '.'.join(map(str, index))), 'wb') as f:
            f.write(chunk.tobytes(order='F'))


def is_hdf5(path):
    if not os.path.isfile(path):
        return False
    with open(path, 'rb') as f:
        return f.read(8) == HDF5_SIGNATURE


def ims_matches(path, volumes, x_on_imaris_x=False):
    # Every channel's level 0 equals its volume, Imaris (X, Y, Z) = dims (0, 1, 2), or (1, 0, 2) with
    # x_on_imaris_x (True without h5py)
    if h5py is None:
        return True
    with h5py.File(path, 'r') as f:
        for c, v in enumerate(volumes):
            data = f[f'DataSet/ResolutionLevel 0/TimePoint 0/Channel {c}/Data']
            if x_on_imaris_x:
                z, y, x = v.shape[2], v.shape[0], v.shape[1]
                same = np.array_equal(np.transpose(data[:z, :y, :x], (1, 2, 0)), v)
            else:
                z, y, x = v.shape[2], v.shape[1], v.shape[0]
                same = np.array_equal(np.transpose(data[:z, :y, :x], (2, 1, 0)), v)
            if not same:
                return False
    return True


def image_info(path):
    # /DataSetInfo/Image X, Y, Z and ExtMax0, 1, 2 (None without h5py)
    if h5py is None:
        return None
    with h5py.File(path, 'r') as f:
        a = f['DataSetInfo/Image'].attrs
        text = {k: b''.join(a[k]).decode() for k in ('X', 'Y', 'Z', 'ExtMax0', 'ExtMax1', 'ExtMax2')}
    return (tuple(int(text[k]) for k in ('X', 'Y', 'Z')), tuple(float(text[f'ExtMax{i}']) for i in range(3)))


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


def main():
    ok = True

    def check(cond, what):
        nonlocal ok
        print(('ok    ' if cond else 'FAILED ') + what)
        ok &= bool(cond)

    tmp = tempfile.mkdtemp()
    try:
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
                print('skip  version of the distribution (not installed)')
        except ImportError:
            pass
        check(pybind11_default_threads() >= 1, f'default threads {pybind11_default_threads()}')

        # Input: two channels of one time point
        inputs = os.path.join(tmp, 'in')
        os.makedirs(inputs)
        vols = [volume(0), volume(1)]
        for name, v in zip(('CamA_t0000.zarr', 'CamB_t0000.zarr'), vols):
            write_zarr(os.path.join(inputs, name), v)

        # The command line, twice in one process
        out1 = os.path.join(tmp, 'out1')
        rc = piw.run_cli(['-P', 'CamA,CamB', '-F', inputs, '-o', out1, '-r', 'zarr', '-v', '0.108,0.108,0.25'])
        f1 = os.path.join(out1, 'output.ims')
        check(rc == 0 and is_hdf5(f1), f'run_cli: exit code {rc}, HDF5 file written')
        check(ims_matches(f1, vols), 'run_cli: voxels' + ('' if h5py else ' (skipped: no h5py)'))
        out2 = os.path.join(tmp, 'out2')
        rc = piw.run_cli(['-P', 'CamA', '-F', inputs, '-o', out2, '-n', 'second', '-r', 'zarr', '-C', '32,32,32'])
        f2 = os.path.join(out2, 'second.ims')
        check(rc == 0 and is_hdf5(f2) and ims_matches(f2, vols[:1]), f'run_cli again (-C): exit code {rc}')
        check(piw.run_cli([]) == 1, 'run_cli without arguments: exit code 1')
        try:
            piw.run_cli('-P CamA')
            check(False, 'run_cli refuses one string')
        except TypeError:
            check(True, 'run_cli refuses one string')

        # python -m parallel_imaris_writer (the console script's main)
        out3 = os.path.join(tmp, 'out3')
        r = subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-P', 'CamB', '-F', inputs, '-o', out3,
                            '-r', 'zarr'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        check(r.returncode == 0 and 'Current File: ' in r.stdout and is_hdf5(os.path.join(out3, 'output.ims')),
              f'python -m parallel_imaris_writer: exit code {r.returncode}')
        # An output it cannot write (output.ims is a folder): exit code 1 and "Error: ..." on stderr
        out_dir = os.path.join(tmp, 'out_isdir')
        os.makedirs(os.path.join(out_dir, 'output.ims'))
        r = subprocess.run([sys.executable, '-m', 'parallel_imaris_writer', '-P', 'CamA', '-F', inputs, '-o', out_dir,
                            '-r', 'zarr'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        check(r.returncode == 1 and 'Error: ' in r.stderr and 'Error: ' not in r.stdout,
              f'python -m with an unwritable output: exit code {r.returncode}, "Error: ..." on stderr')

        # The library call
        files = pybind11_find_input_files([inputs], ['CamA', 'CamB'])
        check([[os.path.normpath(p) for p in c] for c in files]
              == [[os.path.join(inputs, 'CamA_t0000.zarr')], [os.path.join(inputs, 'CamB_t0000.zarr')]],
              'find_input_files: files[channel][timepoint]')
        f4 = os.path.join(tmp, 'convert.ims')
        captured, sys.stdout = sys.stdout, io.StringIO()
        try:
            pybind11_convert('zarr', files, f4, (0.108, 0.108, 0.25), channel_names=['A', 'B'], num_threads=2,
                             verbose=True)
            text = sys.stdout.getvalue()
        finally:
            sys.stdout = captured
        check(is_hdf5(f4) and ims_matches(f4, vols), 'convert: HDF5 file written')
        check(text.count('Current File: ') == 2, 'convert(verbose=True): messages through sys.stdout')
        pybind11_convert('zarr', files, f4, (1, 1, 1), block_size=(32, 32, 32), process_block=(64, 64, 64))
        check(is_hdf5(f4) and ims_matches(f4, vols), 'convert again: block_size, process_block')
        # x_on_imaris_x: dim 1 on Imaris X, dim 0 on Imaris Y; the voxel sizes stay along dims 0, 1, 2
        f6 = os.path.join(tmp, 'x_on_x.ims')
        pybind11_convert('zarr', files[:1], f6, (0.25, 0.5, 2.0), x_on_imaris_x=True)
        info = image_info(f6)
        check(is_hdf5(f6) and ims_matches(f6, vols[:1], x_on_imaris_x=True) and
              (info is None or (info[0] == (SHAPE[1], SHAPE[0], SHAPE[2]) and
                                np.allclose(info[1], (SHAPE[1] * 0.5, SHAPE[0] * 0.25, SHAPE[2] * 2.0)))),
              f'convert(x_on_imaris_x=True): Imaris X, Y, Z = dims 1, 0, 2 ({info})')

        # Errors as exceptions
        def raises(exception, call, what):
            try:
                call()
                check(False, what)
            except exception as e:
                check(True, f'{what}: {type(e).__name__}: {e}')
        raises(FileNotFoundError, lambda: pybind11_find_input_files([inputs], ['CamC']), 'pattern without a match')
        raises(ValueError, lambda: pybind11_convert('png', files, f4, (1, 1, 1)), 'unknown reader')
        raises(ValueError, lambda: pybind11_convert('zarr', [], f4, (1, 1, 1)), 'no input files')
        raises(ValueError, lambda: pybind11_convert('zarr', files, f4, (1, 1, 1), block_size=(1, 2)), 'two block sizes')
        raises(RuntimeError, lambda: pybind11_convert('zarr', [[os.path.join(inputs, 'none.zarr')]],
                                                      os.path.join(tmp, 'none.ims'), (1, 1, 1)), 'missing input')

        # Ctrl-C (a simulated SIGINT, 0.05 s in) stops a conversion: KeyboardInterrupt, no output
        big = os.path.join(tmp, 'big')
        os.makedirs(big)
        write_zarr(os.path.join(big, 'CamA.zarr'), volume(2, (400, 400, 200)), chunks=(128, 128, 64))
        f5 = os.path.join(tmp, 'cancelled.ims')
        interrupted = False
        t0 = time.time()
        timer = threading.Timer(0.05, _thread.interrupt_main)
        try:
            timer.start()
            try:
                pybind11_convert('zarr', [[os.path.join(big, 'CamA.zarr')]], f5, (1, 1, 1), num_threads=1)
            except KeyboardInterrupt:
                interrupted = True
            seconds = time.time() - t0
            timer.join()
            time.sleep(0.2)     # (where a KeyboardInterrupt after a finished conversion comes)
        except KeyboardInterrupt:
            pass
        if interrupted:
            check(not os.path.exists(f5), f'Ctrl-C: KeyboardInterrupt after {seconds:.2f} s, partial output removed')
        else:
            print(f'skip  Ctrl-C: the conversion finished first ({seconds:.2f} s)')
        pybind11_convert('zarr', files, f4, (1, 1, 1))
        check(is_hdf5(f4), 'a conversion after Ctrl-C')
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    if not ok:
        sys.exit('parallel_imaris_writer Python test FAILED')
    print('parallel_imaris_writer Python test passed')


if __name__ == '__main__':
    main()
