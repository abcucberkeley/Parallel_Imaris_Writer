import os
import sys
from .parallel_imaris_writer import pybind11_run_cli, pybind11_version

__version__ = pybind11_version()


def _path(value, name):
    # A str, bytes or path-like value as a str (file names are passed on as text)
    if isinstance(value, str):
        return value
    try:
        return os.fsdecode(value)
    except TypeError:
        raise TypeError(f'{name} must be a str, bytes or path-like value, not {type(value).__name__}') from None


def run_cli(args):
    # The parallelimariswriter command line in this process: args are its arguments without the
    # program name (str, bytes or path-like), e.g. ['-P', 'CamA,CamB', '-F', folder, '-o', out,
    # '-r', 'zarr']. Returns its exit code; its output goes to the process's stdout and stderr, as
    # the executable's. Runs on a worker thread with the GIL released; it cannot be interrupted.
    if isinstance(args, (str, bytes)):
        raise TypeError('args must be a list of arguments, not one string')
    args = [_path(a, 'every argument') for a in args]
    # (what Python printed before comes first)
    sys.stdout.flush()
    sys.stderr.flush()
    return pybind11_run_cli(args)
