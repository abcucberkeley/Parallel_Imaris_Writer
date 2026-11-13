import datetime
import json
import numbers
import operator
import os
import secrets
import sys
import warnings
from collections.abc import Mapping
import numpy as np
from .parallel_imaris_writer import (pybind11_cluster_finish, pybind11_cluster_plan, pybind11_cluster_read_plan,
                                     pybind11_cluster_status, pybind11_cluster_task, pybind11_convert,
                                     pybind11_find_input_files, pybind11_ims_dataset, pybind11_ims_info,
                                     pybind11_read_ims, pybind11_run_cli, pybind11_version, pybind11_write_ims)

__version__ = pybind11_version()

# Axes. order='C' (the default) names arrays (z, y, x), (c, z, y, x) and (t, c, z, y, x), and
# per-axis values (z, y, x); order='F' MATLAB's (y, x, z), (y, x, z, c) and (y, x, z, c, t), and
# per-axis values (y, x, z). Either way Imaris X is the image's y, Imaris Y its x and Imaris Z its
# z, as in every file the command line (and PetaKit5D's MATLAB code) writes; x_on_imaris_x=True
# puts the image's x on Imaris X and its y on Imaris Y instead. The order names the axes only: any
# memory layout is read in place and means the same. Inside, values travel as the image's
# (y, x, z) (_per_axis), then become Imaris X, Y, Z (_swap) or the input files' axes (_input_values).

# The sample types of an .ims file
_TYPES = (np.dtype(np.uint8), np.dtype(np.uint16), np.dtype(np.uint32), np.dtype(np.float32))


def _path(value, name):
    # A str, bytes or path-like value as a str (file names are passed on as text)
    if isinstance(value, str):
        return value
    try:
        return os.fsdecode(value)
    except TypeError:
        raise TypeError(f'{name} must be a str, bytes or path-like value, not {type(value).__name__}') from None


def _native(path):
    # A path as the library takes it: the file system's bytes (any name the OS has), str on Windows
    return os.fsdecode(path) if os.name == 'nt' else os.fsencode(path)


def _ims_name(file_name):
    file_name = _path(file_name, 'file_name')
    if not file_name.lower().endswith('.ims'):
        raise ValueError(f'file_name must end in .ims, not {file_name!r}')
    return file_name


def _threads(num_threads):
    # A call's number of threads: None (0 to the module) for the default, OpenMP's
    # (OMP_NUM_THREADS, or every CPU the process may run on)
    if num_threads is None:
        return 0
    if isinstance(num_threads, bool) or not isinstance(num_threads, numbers.Real) or int(num_threads) != num_threads \
            or num_threads < 1:
        raise ValueError(f'num_threads must be a whole number of at least 1, not {num_threads!r}')
    return int(num_threads)


def _order(order):
    # 'C' (or None) for (z, y, x) arrays, 'F' for MATLAB's (y, x, z)
    if order is None:
        return 'C'
    if order not in ('C', 'F'):
        raise ValueError(f"order must be 'C' or 'F', not {order!r}")
    return order


def _index(value, name):
    if isinstance(value, (bool, np.bool_)):
        raise TypeError(f'{name} must be a whole number, not {value!r}')
    try:
        n = operator.index(value)
    except TypeError:
        raise TypeError(f'{name} must be a whole number, not {value!r}') from None
    if n < 0:
        raise ValueError(f'{name} must be at least 0, not {n}')
    return n


def _per_axis(values, name, order):
    # Three values in the order's axis order ((z, y, x) for order='C', (y, x, z) for 'F'), or a
    # mapping with the keys 'x', 'y' and 'z' (the image's axes), as the image's (y, x, z)
    if isinstance(values, Mapping):
        if set(values) != {'x', 'y', 'z'}:
            raise ValueError(f"{name} as a mapping needs the keys 'x', 'y' and 'z', not {sorted(map(str, values))}")
        return [values['y'], values['x'], values['z']]
    wrong = f"{name} must be three values, {'(z, y, x)' if order == 'C' else '(y, x, z)'}, or a mapping with the " \
            f"keys 'x', 'y' and 'z', not {values!r}"
    if isinstance(values, (str, bytes)):
        raise TypeError(wrong)
    try:
        values = list(values)
    except TypeError:
        raise TypeError(wrong) from None
    if len(values) != 3:
        raise ValueError(wrong)
    return [values[1], values[2], values[0]] if order == 'C' else values


def _voxel_size(voxel_size, order):
    if voxel_size is None:
        return [0.108, 0.108, 0.108]
    values = []
    for v in _per_axis(voxel_size, 'voxel_size', order):
        if isinstance(v, (bool, np.bool_)) or not isinstance(v, numbers.Real):
            raise TypeError(f'voxel_size values must be numbers, not {v!r}')
        if not (np.isfinite(v) and v > 0):
            raise ValueError(f'voxel_size values must be positive, not {v!r}')
        values.append(float(v))
    return values


def _swap(values, x_on_imaris_x):
    # The image's (y, x, z) as Imaris X, Y, Z, and back: the same values, the first two swapped
    # when the image's x is on Imaris X
    v = list(values)
    return [v[1], v[0], v[2]] if x_on_imaris_x else v


def _sizes(values, name, order):
    # Whole numbers of at least 1 along the image's (y, x, z), or None
    if values is None:
        return None
    sizes = []
    for v in _per_axis(values, name, order):
        n = _index(v, f'{name} values')
        if n < 1:
            raise ValueError(f'{name} values must be at least 1, not {n}')
        sizes.append(n)
    return sizes


def _start_time(start_time):
    # 'YYYY-MM-DD HH:MM:SS.mmm' of a datetime (local wall-clock time: an aware one is converted)
    if start_time is None:
        return ''
    if isinstance(start_time, datetime.datetime):
        if start_time.tzinfo is not None:
            start_time = start_time.astimezone().replace(tzinfo=None)
        return start_time.strftime('%Y-%m-%d %H:%M:%S') + f'.{start_time.microsecond // 1000:03d}'
    if not isinstance(start_time, str):
        raise TypeError(f"start_time must be a datetime or a str 'YYYY-MM-DD HH:MM:SS[.mmm]', not {start_time!r}")
    return start_time


def _numbers(values, name):
    # One number per channel (None: 0, none written)
    if values is None:
        return []
    if isinstance(values, (str, bytes)) or not hasattr(values, '__iter__'):
        raise TypeError(f'{name} must be a list with one number per channel, not {values!r}')
    result = []
    for v in values:
        if v is None:
            v = 0
        if isinstance(v, (bool, np.bool_)) or not isinstance(v, numbers.Real):
            raise TypeError(f'{name} must be a list with one number per channel, not {values!r}')
        result.append(float(v))
    return result


def _metadata(channel_names, channel_colors, emission_wavelengths, excitation_wavelengths, time_interval,
              start_time):
    # The metadata arguments of the module's writers
    if channel_names is None:
        names = []
    elif isinstance(channel_names, str):
        names = [channel_names]
    else:
        names = list(channel_names)
        if not all(isinstance(n, str) for n in names):
            raise TypeError(f'channel_names must be a list of str, one per channel, not {channel_names!r}')
    colors = []
    if channel_colors is not None:
        wrong = f'channel_colors must be a list with one (red, green, blue) per channel, 0 to 1, not {channel_colors!r}'
        if isinstance(channel_colors, (str, bytes)):
            raise TypeError(wrong)
        for c in channel_colors:
            if isinstance(c, (str, bytes)) or not hasattr(c, '__len__') or len(c) != 3:
                raise TypeError(wrong)
            if not all(isinstance(v, numbers.Real) and not isinstance(v, (bool, np.bool_)) for v in c):
                raise TypeError(wrong)
            colors.append([float(v) for v in c])
    if time_interval is None:
        interval = 0.0
    elif isinstance(time_interval, (bool, np.bool_)) or not isinstance(time_interval, numbers.Real):
        raise TypeError(f'time_interval must be a number of seconds, not {time_interval!r}')
    else:
        interval = float(time_interval)
    return dict(channel_names=names, channel_colors=colors,
                emission_wavelengths=_numbers(emission_wavelengths, 'emission_wavelengths'),
                excitation_wavelengths=_numbers(excitation_wavelengths, 'excitation_wavelengths'),
                time_interval=interval, start_time=_start_time(start_time))


def _progress(progress):
    if progress is not None and not callable(progress):
        raise TypeError(f'progress must be a function progress(fraction, bytes_written), not {progress!r}')
    return progress


def _array(value, name):
    # value as an array of a type an .ims file holds, in this machine's byte order (only an array
    # in the other byte order is copied)
    a = np.asarray(value)
    if a.dtype.newbyteorder('=') not in _TYPES or a.dtype.fields is not None or a.dtype.subdtype is not None:
        raise TypeError(f'{name} has the data type {a.dtype}: an .ims file holds uint8, uint16, uint32 or float32 '
                        'data (convert it with astype first)')
    if not a.dtype.isnative:
        a = a.astype(a.dtype.newbyteorder('='))
    return a


def _readable(v, order):
    # A volume as the library reads it in place: aligned, with positive strides along its axes of
    # more than one element; else a copy in the order's layout
    if not v.flags.aligned or any(s <= 0 for s, n in zip(v.strides, v.shape) if n > 1):
        # (a copy: np.ascontiguousarray keeps a contiguous unaligned array as it is)
        v = v.copy(order=order)
    return v


def _volumes(data, order):
    # The volumes of data, each as a view along Imaris X, Y, Z (the image's y, x, z), in the
    # library's order (the time points of channel 0, then of channel 1, ...), and the numbers of
    # channels and time points. Nothing is copied but arrays in the other byte order, unaligned
    # ones and ones with negative (or 0) strides (an array given more than once is copied once).
    converted = {}

    def array(value, name):
        if id(value) not in converted:
            converted[id(value)] = (value, _array(value, name))
        return converted[id(value)][1]
    if isinstance(data, (list, tuple)):
        if not data:
            raise ValueError('data is an empty list')
        nested = [isinstance(d, (list, tuple)) for d in data]
        if all(nested):
            # data[timepoint][channel]
            channels = len(data[0])
            if not channels or any(len(d) != channels for d in data):
                raise ValueError('data[timepoint][channel] needs the same number of channels, at least one, at every '
                                 'time point')
            grid = [[array(data[t][c], f'data[{t}][{c}]') for c in range(channels)] for t in range(len(data))]
        elif any(nested):
            raise ValueError('data must be a list of volumes (channels) or a list of such lists (data[timepoint]'
                             '[channel]), not a mix')
        else:
            grid = [[array(d, f'data[{c}]') for c, d in enumerate(data)]]
        for t, row in enumerate(grid):
            for c, v in enumerate(row):
                if v.ndim != 3:
                    raise ValueError(f"every volume in a list must be 3-D, {'(z, y, x)' if order == 'C' else '(y, x, z)'};"
                                     f" volume {c} of time point {t} has the shape {v.shape}")
    else:
        a = _array(data, 'data')
        if not 2 <= a.ndim <= 5:
            raise ValueError(f'data must have 2 to 5 dimensions, not {a.ndim} (shape {a.shape})')
        if a.size == 0:
            raise ValueError(f'cannot write an empty array (shape {a.shape})')
        if order == 'C':
            # (t, c, z, y, x) with the missing leading axes ((y, x) is one plane); views, never copies
            a = a[(np.newaxis,) * (5 - a.ndim)]
            grid = [[a[t, c] for c in range(a.shape[1])] for t in range(a.shape[0])]
        else:
            # (y, x, z, c, t) with the missing trailing axes
            a = a[(Ellipsis,) + (np.newaxis,) * (5 - a.ndim)]
            grid = [[a[:, :, :, c, t] for c in range(a.shape[3])] for t in range(a.shape[4])]
    timepoints, channels = len(grid), len(grid[0])
    first = grid[0][0]
    for t, row in enumerate(grid):
        for c, v in enumerate(row):
            if v.dtype != first.dtype:
                raise TypeError(f'every volume needs the same data type: {first.dtype} and {v.dtype} were given')
            if v.shape != first.shape:
                raise ValueError(f'every volume needs the same shape: {first.shape} and {v.shape} were given')
    if first.size == 0:
        raise ValueError(f'cannot write empty volumes (shape {first.shape})')
    readable, volumes = {}, []
    for c in range(channels):
        for t in range(timepoints):
            v = grid[t][c]
            if id(v) not in readable:
                readable[id(v)] = (v, _readable(v, order))
            v = readable[id(v)][1]
            volumes.append(np.transpose(v, (1, 2, 0)) if order == 'C' else v)
    return volumes, channels, timepoints


def _temporary(file_name):
    # A new empty file beside file_name, <stem>_<8 hex digits>.ims: the write goes there and is
    # renamed when complete. Made here (with the umask's permissions, which the file keeps) so
    # that no other writer takes the name.
    folder, base = os.path.split(file_name)
    for _ in range(100):
        name = os.path.join(folder, f'{base[:-4]}_{secrets.token_hex(4)}{base[-4:]}')
        try:
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_BINARY', 0), 0o666)
        except FileExistsError:
            continue
        os.close(fd)
        return name
    raise FileExistsError(f'no free temporary name beside {file_name}')


def _remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _rename(temporary, file_name):
    # The complete temporary file under its final name. If only this rename fails (on Windows, for
    # example, while the old file_name is open in another program), the complete file is kept under
    # its temporary name and the error names it, as the library keeps its <name>.ims.partial (the
    # caller removes the temporary file on any other exception)
    try:
        os.replace(temporary, file_name)
    except OSError as e:
        message = (f'the complete file was kept as {temporary}: it could not be renamed to {file_name} '
                   f'({e.strerror or e})')
        raise (OSError(e.errno, message) if e.errno is not None else OSError(message)) from e


def write_ims(file_name, data, voxel_size=None, block_size=None, order='C', num_threads=None, progress=None,
              verbose=False, channel_names=None, channel_colors=None, emission_wavelengths=None,
              excitation_wavelengths=None, time_interval=None, start_time=None, x_on_imaris_x=False):
    """Write arrays to an Imaris (.ims) file, with its resolution pyramid.

    data: a NumPy array, or a list of 3-D arrays (one per channel), or a list of such lists
    (data[timepoint][channel]). With order='C' (the default) an array is (z, y, x), (c, z, y, x)
    or (t, c, z, y, x), and a 2-D (y, x) array is one plane; with order='F' it is MATLAB's
    (y, x, z), (y, x, z, c) or (y, x, z, c, t), and (y, x). The order names the axes, not a
    memory layout: arrays are read in place whatever their layout (a (z, y, x) C-contiguous
    array without a copy, an F-contiguous or strided one as its C-contiguous copy would be), and
    only arrays in the other byte order, unaligned ones and ones with negative strides are copied.
    uint8, uint16, uint32 or float32; other types raise TypeError.
    Imaris X is the image's y, Imaris Y its x and Imaris Z its z (as the command line writes
    every image); x_on_imaris_x=True puts its x on Imaris X and its y on Imaris Y (read such a
    file back with x_on_imaris_x=True too).

    voxel_size, block_size: three values in the order's axis order, (dz, dy, dx) by default, or a
    mapping with the keys 'x', 'y' and 'z'. voxel_size defaults to 0.108 for all three.
    block_size is the size of the pieces handed to the Imaris writer (default 64 each); it does
    not change the file.
    num_threads: the threads this call uses (by default OMP_NUM_THREADS, or every CPU); it
    changes no other thread's setting. The GIL is released while writing.
    progress: progress(fraction, bytes_written), called on the calling thread at most about ten
    times a second, the fraction never decreasing, and once with 1.0 when the file is complete;
    an exception it raises stops the write and is raised here. Ctrl-C stops the write too
    (KeyboardInterrupt).
    verbose: print the command line's lines (block and voxel sizes, the current volume).
    channel_names, channel_colors ((red, green, blue), 0 to 1), emission_wavelengths,
    excitation_wavelengths (nm): one per channel, for the file's metadata. time_interval: seconds
    between time points (default 1); start_time: a datetime or 'YYYY-MM-DD HH:MM:SS[.mmm]' (local
    time) of the first time point (default now).

    The file is written under a temporary name beside file_name (<stem>_<8 hex digits>.ims) and
    renamed when complete, replacing an existing file_name: a file under its final name is always
    whole, and a failed or stopped write leaves nothing. If only that rename fails (on Windows, for
    example, while the old file is open in Imaris), the complete file is kept under its temporary
    name and the OSError names it. Calls in one process write one at a time.
    """
    file_name = _ims_name(file_name)
    order = _order(order)
    threads = _threads(num_threads)
    progress = _progress(progress)
    volumes, channels, timepoints = _volumes(data, order)
    x_on_imaris_x = bool(x_on_imaris_x)
    if x_on_imaris_x:
        volumes = [np.transpose(v, (1, 0, 2)) for v in volumes]
    voxel = _swap(_voxel_size(voxel_size, order), x_on_imaris_x)
    block = _sizes(block_size, 'block_size', order)
    if block is not None:
        block = _swap(block, x_on_imaris_x)
    metadata = _metadata(channel_names, channel_colors, emission_wavelengths, excitation_wavelengths, time_interval,
                         start_time)
    if os.path.isdir(file_name):
        raise IsADirectoryError(f'{file_name} is a folder')
    if verbose:
        # (what Python printed before comes first)
        sys.stdout.flush()
    temporary, written = None, False
    try:
        temporary = _temporary(file_name)
        pybind11_write_ims(_native(temporary), volumes, channels, timepoints, voxel, block, threads,
                           verbose=bool(verbose), progress=progress, **metadata)
        written = True
        _rename(temporary, file_name)
    except BaseException as e:
        if temporary is not None and not (written and isinstance(e, OSError)):
            _remove(temporary)
        raise


# ---- files: find_input_files, convert_to_ims, convert_dataset

def _paths(value, name):
    # One path, or a list of paths, as a list of str
    if isinstance(value, (str, bytes, os.PathLike)):
        return [_path(value, name)]
    try:
        paths = [_path(v, name) for v in value]
    except TypeError:
        raise TypeError(f'{name} must be a path or a list of paths, not {value!r}') from None
    if not paths:
        raise ValueError(f'{name} is an empty list')
    return paths


def _folders(value):
    # Input folders, a leading ~ expanded (as the command line's -F)
    return [os.path.expanduser(f) for f in _paths(value, 'folders')]


def _patterns(value):
    # Channel patterns: one str, or a list of str (one per channel)
    patterns = [value] if isinstance(value, str) else value
    if isinstance(patterns, bytes) or not hasattr(patterns, '__iter__'):
        raise TypeError(f'channel_patterns must be a str or a list of str (one per channel), not {value!r}')
    patterns = list(patterns)
    if not patterns or not all(isinstance(p, str) for p in patterns):
        raise TypeError(f'channel_patterns must be a str or a list of str (one per channel), not {value!r}')
    if not all(patterns):
        raise ValueError(f'channel_patterns must not be empty: {value!r}')
    return patterns


def _reader(reader):
    if reader is None:
        return None
    if reader not in ('tiff', 'zarr'):
        raise ValueError(f"reader must be 'tiff', 'zarr' or None (from the file names), not {reader!r}")
    return reader


def _takes(name, reader):
    # Whether the reader takes the file (the library's rule: tiff .tif and .tiff in any case, zarr .zarr)
    if reader == 'zarr':
        return name.endswith('.zarr')
    return os.path.splitext(name)[1].lower() in ('.tif', '.tiff')


def _reader_of(files):
    # The reader of files[channel][timepoint], from their names
    readers = {('zarr' if _takes(f, 'zarr') else 'tiff' if _takes(f, 'tiff') else None, f)
               for channel in files for f in channel}
    kinds = {r for r, _ in readers}
    if None in kinds:
        raise ValueError(f'cannot tell the reader of {next(f for r, f in readers if r is None)} from its name (.tif, '
                         ".tiff or .zarr): give reader='tiff' or 'zarr'")
    if len(kinds) > 1:
        raise ValueError("the inputs are TIFF and Zarr files: give reader='tiff' or 'zarr'")
    return kinds.pop()


def _file_grid(files):
    # files: one path (one channel, one time point), a list of paths (the time points of one
    # channel), or files[channel][timepoint]
    if isinstance(files, (str, bytes, os.PathLike)):
        return [[_path(files, 'files')]]
    try:
        items = list(files)
    except TypeError:
        raise TypeError(f'files must be a path, a list of paths or files[channel][timepoint], not {files!r}') from None
    if not items:
        raise ValueError('files is an empty list')
    if all(isinstance(f, (str, bytes, os.PathLike)) for f in items):
        return [[_path(f, 'files') for f in items]]
    return [_paths(channel, 'files[channel]') for channel in items]


def _zarr_order(path):
    # 'C' or 'F' of a Zarr's metadata (v2 .zarray "order"; v3 zarr.json: F when a transpose codec
    # reverses the axes, as cpp-zarr reads it), None when it has none that can be read
    try:
        with open(os.path.join(path, '.zarray'), 'rb') as f:
            order = json.load(f).get('order')
        return order if order in ('C', 'F') else None
    except (OSError, ValueError, AttributeError):
        pass
    try:
        with open(os.path.join(path, 'zarr.json'), 'rb') as f:
            meta = json.load(f)
        ndim = len(meta['shape'])
        for codec in meta.get('codecs') or []:
            if isinstance(codec, Mapping) and codec.get('name') == 'transpose':
                order = (codec.get('configuration') or {}).get('order')
                return 'F' if order == 'F' or order == list(range(ndim - 1, -1, -1)) else 'C'
        return 'C'
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


def _input_axes(input_axes, reader, files):
    # 'yxz' or 'zyx'. None: TIFF 'yxz'; Zarr from the arrays' order: F (MATLAB's layout, [ny, nx,
    # nz]) 'yxz', C 'zyx', the same for every input (arrays without readable metadata are left to
    # the library, which refuses them)
    if input_axes is not None:
        if input_axes not in ('yxz', 'zyx'):
            raise ValueError(f"input_axes must be 'yxz', 'zyx' or None (from each Zarr's order), not {input_axes!r}")
        if input_axes == 'zyx' and reader != 'zarr':
            raise ValueError("input_axes='zyx' is for Zarr input only; TIFF pages are (y, x, z)")
        return input_axes
    if reader != 'zarr':
        return 'yxz'
    first = None
    for channel in files:
        for f in channel:
            order = _zarr_order(f)
            if order is None:
                continue
            if first is None:
                first = (f, order)
            elif order != first[1]:
                raise ValueError(f'{first[0]} is in {first[1]} order but {f} in {order} order (F: (y, x, z), C: '
                                 '(z, y, x)): give input_axes')
    return 'zyx' if first is not None and first[1] == 'C' else 'yxz'


def _input_values(yxz, input_axes):
    # Values along the image's (y, x, z) along the input's axes 0, 1, 2
    return list(yxz) if input_axes == 'yxz' else [yxz[2], yxz[0], yxz[1]]


def _bbox(input_bbox, order, input_axes):
    # Six values in the order's axis order, the starts then the ends, as the library's box along
    # the input's axes, or None
    if input_bbox is None:
        return None
    axes = '(z0, y0, x0, z1, y1, x1)' if order == 'C' else '(y0, x0, z0, y1, x1, z1)'
    wrong = f'input_bbox must be six whole numbers, {axes}, 0-based with the end excluded, not {input_bbox!r}'
    if isinstance(input_bbox, (str, bytes, Mapping)):
        raise TypeError(wrong)
    try:
        values = list(input_bbox)
    except TypeError:
        raise TypeError(wrong) from None
    if len(values) != 6:
        raise ValueError(wrong)
    start = [_index(v, 'input_bbox values') for v in _per_axis(values[:3], 'input_bbox', order)]
    end = [_index(v, 'input_bbox values') for v in _per_axis(values[3:], 'input_bbox', order)]
    if any(e <= s for s, e in zip(start, end)):
        raise ValueError(f'input_bbox must end after it starts along every axis: {tuple(values)}')
    return _input_values(start, input_axes) + _input_values(end, input_axes)


def find_input_files(folders, channel_patterns, reader=None):
    """The files the command line converts for -F folders -P patterns, as files[channel][timepoint].

    folders: a folder or a list of folders (a leading ~ is the home folder). channel_patterns: a
    str or a list of str, one per channel. In every folder, the entries the reader takes (reader
    'tiff': .tif and .tiff in any case; 'zarr': .zarr; None: both, but not both for one pattern)
    whose name contains the pattern are that channel's time points, sorted by full path.
    FileNotFoundError for a folder that does not exist or a pattern without a match; ValueError
    for an entry that matches two patterns or channels with different numbers of files.
    """
    folders = _folders(folders)
    patterns = _patterns(channel_patterns)
    reader = _reader(reader)
    return pybind11_find_input_files([_native(f) for f in folders], [_native(p) for p in patterns], reader or '')


def _conversion(folders, channel_patterns, files, reader, voxel_size, block_size, input_bbox, process_block_size,
                order, input_axes, channel_names, channel_colors, emission_wavelengths, excitation_wavelengths,
                time_interval, start_time):
    # The arguments of a conversion as the module takes them: the inputs found (files[channel]
    # [timepoint], native paths), the reader, the input axes and the per-axis values along the
    # input's axes, the metadata (channel names default to the patterns)
    reader = _reader(reader)
    patterns = None
    if files is not None:
        if folders is not None or channel_patterns is not None:
            raise ValueError('give either files, or folders and channel_patterns, not both')
        grid = _file_grid(files)
    elif folders is None or channel_patterns is None:
        raise ValueError('give the inputs: folders and channel_patterns, or files')
    else:
        patterns = _patterns(channel_patterns)
        grid = find_input_files(folders, patterns, reader)
    if reader is None:
        reader = _reader_of(grid)
    axes = _input_axes(input_axes, reader, grid)
    block = _sizes(block_size, 'block_size', order)
    process = _sizes(process_block_size, 'process_block_size', order)
    if channel_names is None and patterns is not None:
        channel_names = patterns
    return dict(reader=reader, files=[[_native(f) for f in channel] for channel in grid], axes=axes,
                voxel=_input_values(_voxel_size(voxel_size, order), axes),
                block=None if block is None else _input_values(block, axes), box=_bbox(input_bbox, order, axes),
                process=None if process is None else _input_values(process, axes),
                metadata=_metadata(channel_names, channel_colors, emission_wavelengths, excitation_wavelengths,
                                   time_interval, start_time))


def convert_to_ims(file_name, folders=None, channel_patterns=None, files=None, reader=None, voxel_size=None,
                   block_size=None, input_bbox=None, process_block_size=None, order='C', input_axes=None,
                   x_on_imaris_x=False, num_threads=None, progress=None, verbose=False, channel_names=None,
                   channel_colors=None, emission_wavelengths=None, excitation_wavelengths=None, time_interval=None,
                   start_time=None):
    """Convert TIFF or Zarr volumes to one Imaris (.ims) file, as the command line does.

    The inputs: folders and channel_patterns (the command line's -F and -P: find_input_files picks
    and orders the files), or files: a path, a list of paths (the time points of one channel), or
    files[channel][timepoint]. reader: 'tiff' or 'zarr'; by default from the file names. Every
    input must have the first one's size (unless input_bbox lies inside all of them) and sample
    type (uint8, uint16, uint32 or float32); this is checked before anything is written.

    voxel_size, block_size, process_block_size: three values in the order's axis order, (dz, dy,
    dx) by default, or a mapping with the keys 'x', 'y' and 'z'. voxel_size defaults to 0.108 for
    all three. block_size is the size of the pieces handed to the Imaris writer (default the
    input's chunks; it does not change the file). process_block_size bounds the voxels of a time
    point held in memory at once (TIFF: along z only; default a whole time point).
    input_bbox: crop every input to this box, six values in the order's axis order, the starts then
    the ends, 0-based with the end excluded: (z0, y0, x0, z1, y1, x1) by default.

    input_axes: the axes of the input files. 'yxz' is MATLAB's layout: every TIFF (rows, columns,
    pages) and a Zarr of [ny, nx, nz]; 'zyx' a Zarr of (z, y, x), as written from a C-order NumPy
    array. None (the default): 'yxz' for TIFF files and, for Zarr arrays, from their order: 'F'
    (MATLAB's) 'yxz', 'C' 'zyx' (the command line takes every input as 'yxz').
    Imaris X is the image's y, Imaris Y its x and Imaris Z its z, as the command line writes;
    x_on_imaris_x=True puts its x on Imaris X and its y on Imaris Y.

    channel_names default to the channel patterns (as the command line names them), else "Channel
    0", "Channel 1", ...; channel_colors, emission_wavelengths, excitation_wavelengths, time_interval
    and start_time as write_ims takes them. Without time_interval the time points come from
    "<n>msecAbs" in the file names, else 1 s apart; without start_time the first time point is the
    modification time of the first input. With the same options the file is the command line's.
    num_threads, progress, verbose and the temporary name as write_ims; no timepoints.txt is written.
    """
    file_name = _ims_name(file_name)
    order = _order(order)
    threads = _threads(num_threads)
    progress = _progress(progress)
    c = _conversion(folders, channel_patterns, files, reader, voxel_size, block_size, input_bbox, process_block_size,
                    order, input_axes, channel_names, channel_colors, emission_wavelengths, excitation_wavelengths,
                    time_interval, start_time)
    if os.path.isdir(file_name):
        raise IsADirectoryError(f'{file_name} is a folder')
    if verbose:
        sys.stdout.flush()
    temporary, written = None, False
    try:
        temporary = _temporary(file_name)
        pybind11_convert(c['reader'], c['files'], _native(temporary), c['voxel'], c['block'], c['box'], c['process'],
                         threads, x_on_imaris_x=bool(x_on_imaris_x), input_axes=c['axes'], verbose=bool(verbose),
                         progress=progress, **c['metadata'])
        written = True
        _rename(temporary, file_name)
    except BaseException as e:
        if temporary is not None and not (written and isinstance(e, OSError)):
            _remove(temporary)
        raise


def _first_input(folder, patterns, reader):
    # The first entry, by name, of folder that the reader takes and whose name contains a pattern
    # (None if there is none)
    try:
        names = sorted(n for n in os.listdir(os.fsencode(folder)))
    except FileNotFoundError:
        raise FileNotFoundError(f'Input folder "{folder}" does not exist') from None
    for n in names:
        name = os.fsdecode(n)
        if _takes(name, reader) and any(p in name for p in patterns):
            return name
    return None


def convert_dataset(data_paths, channel_patterns=('CamA_ch0', 'CamA_ch1', 'CamB_ch0'), result_dir_name='imaris',
                    overwrite=False, zarr_file=False, voxel_size=None, block_size=(64, 64, 64), input_bbox=None,
                    process_block_size=None, order='C', input_axes=None, x_on_imaris_x=False, num_threads=None,
                    progress=None, verbose=False, channel_names=None, channel_colors=None, emission_wavelengths=None,
                    excitation_wavelengths=None, time_interval=None, start_time=None):
    """Convert datasets to .ims files on this machine, as PetaKit5D's MATLAB dataset wrapper
    (XR_imaris_conversion_data_wrapper) does with the command line: one .ims per dataset.

    data_paths: a dataset folder, or a list whose items are a folder or a list of folders (one
    dataset in several folders). In a dataset's folders the TIFF files (zarr_file=True: the Zarr
    arrays) whose names contain a channel pattern are converted (each pattern one channel, its
    files the time points in name order) into <first folder>/<result_dir_name>/<stem>.ims, <stem>
    being the name without its extension of the first such file of the first folder; the result
    folder is made. Written under a temporary name (<stem>_<8 hex digits>.ims) and renamed when
    complete.
    overwrite=False leaves an existing output as it is (its path is returned, nothing written);
    True replaces it (the folder's other files stay).
    Inputs of different sizes are refused (ValueError naming two of them) before anything is
    written, unless input_bbox lies inside all of them.

    Returns the .ims paths, one per dataset: None for a dataset without matching files (with a
    warning, and the next dataset is converted). The other arguments are convert_to_ims's, with the
    MATLAB wrapper's defaults (its pixelSizes are voxel_size here, in the order's axis order:
    (dz, dy, dx) by default); progress is called for each dataset's conversion in turn.
    """
    if isinstance(data_paths, (str, bytes, os.PathLike)):
        datasets = [[data_paths]]
    else:
        try:
            datasets = [[d] if isinstance(d, (str, bytes, os.PathLike)) else list(d) for d in data_paths]
        except TypeError:
            raise TypeError(f'data_paths must be a folder or a list of folders or of lists of folders, not '
                            f'{data_paths!r}') from None
        if not datasets:
            raise ValueError('data_paths is an empty list')
    datasets = [_folders(d) for d in datasets]
    patterns = _patterns(channel_patterns)
    reader = 'zarr' if zarr_file else 'tiff'
    if not isinstance(result_dir_name, str):
        raise TypeError(f'result_dir_name must be a str, not {result_dir_name!r}')
    result_dir_name = result_dir_name or 'imaris'
    options = dict(reader=reader, voxel_size=voxel_size, block_size=block_size, input_bbox=input_bbox,
                   process_block_size=process_block_size, order=order, input_axes=input_axes,
                   x_on_imaris_x=x_on_imaris_x, num_threads=num_threads, progress=progress, verbose=verbose,
                   channel_names=channel_names, channel_colors=channel_colors,
                   emission_wavelengths=emission_wavelengths, excitation_wavelengths=excitation_wavelengths,
                   time_interval=time_interval, start_time=start_time)
    results = []
    for folders in datasets:
        first = _first_input(folders[0], patterns, reader)
        if first is None:
            kind = 'Zarr' if zarr_file else 'TIFF'
            warnings.warn(f'Folder {folders[0]} does not contain any {kind} input whose name contains one of the '
                          f'channel patterns {patterns}', stacklevel=2)
            results.append(None)
            continue
        result_dir = os.path.join(folders[0], result_dir_name)
        output = os.path.join(result_dir, os.path.splitext(first)[0] + '.ims')
        if os.path.exists(output) and not overwrite:
            results.append(output)
            continue
        os.makedirs(result_dir, exist_ok=True)
        convert_to_ims(output, folders=folders, channel_patterns=patterns, **options)
        results.append(output)
    return results


def _image_to_order(yxz, order):
    # Values along the image's (y, x, z) in the order's axis order
    return (yxz[2], yxz[0], yxz[1]) if order == 'C' else tuple(yxz)


def _existing(file_name):
    name = _path(file_name, 'file_name')
    if not os.path.isfile(name):
        raise FileNotFoundError(f'{name} does not exist')
    return name


def _single(value):
    # A single-precision value of the file as the shortest decimal that is that value (0.108, not
    # 0.10800000280141830)
    return float(str(np.float32(value)))


def ims_info(file_name, checksums=False, order='C', x_on_imaris_x=False):
    """What an .ims file holds, as a dict; sizes and per-axis values in the order's axis order
    ((z, y, x) by default, (y, x, z) for order='F'), Imaris X taken as the image's y (its x with
    x_on_imaris_x=True, for files written that way):

    shape (one volume at resolution level 0), dtype, channels, timepoints, resolution_levels,
    voxel_size, extent_min and extent_max (the image's extent in the file's units), channel_names,
    channel_colors ((red, green, blue) each), time_points (as the file writes them), datasets
    (one dict per resolution level, time point and channel, in that order: resolution_level,
    timepoint, channel, shape, chunk_shape, chunks (the number stored), crc, histogram_crc),
    thumbnail_crc, and attributes: every attribute of the file as {'<object path>/<name>': text}.

    checksums=True also computes the CRCs (else None): crc of a dataset's stored chunks (their
    positions and compressed bytes), histogram_crc of its histograms and attributes, and
    thumbnail_crc of /Thumbnail, so that two files can be compared with these and attributes.
    Without checksums it still counts every dataset's stored chunks: about half a second per
    dataset of 80,000 chunks (read_ims does not need this).
    """
    order = _order(order)
    x_on_imaris_x = bool(x_on_imaris_x)
    raw = pybind11_ims_info(_native(_existing(file_name)), bool(checksums))

    def to_order(xyz):
        # Imaris X, Y, Z in the order's axis order
        return _image_to_order(_swap(xyz, x_on_imaris_x), order)
    size = raw['size']
    ext_min, ext_max, voxel = ([_single(v) for v in raw[k]] for k in ('extent_min', 'extent_max', 'voxel_size'))
    colors = []
    for text in raw['channel_colors']:
        try:
            colors.append(tuple(float(v) for v in text.split()))
        except ValueError:
            colors.append(text)
    datasets = [{'resolution_level': d['level'], 'timepoint': d['timepoint'], 'channel': d['channel'],
                 'shape': to_order(d['size']), 'chunk_shape': to_order(d['chunk']),
                 'chunks': d['chunks'], 'crc': d['crc'], 'histogram_crc': d['histogram_crc']}
                for d in raw['datasets']]
    return {'shape': to_order(size), 'dtype': np.dtype(raw['data_type']), 'channels': raw['channels'],
            'timepoints': raw['timepoints'], 'resolution_levels': raw['levels'],
            'voxel_size': to_order(voxel), 'extent_min': to_order(ext_min),
            'extent_max': to_order(ext_max), 'channel_names': list(raw['channel_names']),
            'channel_colors': colors, 'time_points': list(raw['time_points']), 'datasets': datasets,
            'thumbnail_crc': raw['thumbnail_crc'], 'attributes': dict(raw['attributes'])}


def _coords(values, name, order):
    # Three whole numbers in the order's axis order, as the image's (y, x, z)
    return [_index(v, f'{name} values') for v in _per_axis(values, name, order)]


def read_ims(file_name, resolution_level=0, timepoint=0, channel=0, start_coords=None, end_coords=None, order='C',
             num_threads=None, out=None, x_on_imaris_x=False):
    """Read one volume (or a region of it) of an .ims file into a NumPy array.

    order='C' (the default) returns (z, y, x), C-contiguous; order='F' MATLAB's (y, x, z),
    F-contiguous: the arrays write_ims takes with that order, so that read_ims(f) equals the
    data of write_ims(f, data) (at resolution level 0).
    start_coords, end_coords: the region [start, end) in the order's axis order, 0-based, end
    excluded (as cppzarr.read_zarr; None or all zeros for end_coords: to the end).
    num_threads: the threads that decode chunks (by default OMP_NUM_THREADS, or every CPU); the
    GIL is released while reading.
    out: an array to read into instead of a new one: the region's shape and the file's data type,
    writeable, any layout with positive strides. It is returned.
    x_on_imaris_x: Imaris X is the image's x (for files written with x_on_imaris_x=True); by
    default its y, as write_ims and the command line write.
    """
    order = _order(order)
    threads = _threads(num_threads)
    x_on_imaris_x = bool(x_on_imaris_x)
    level, t, c = (_index(resolution_level, 'resolution_level'), _index(timepoint, 'timepoint'),
                   _index(channel, 'channel'))
    name = _existing(file_name)
    size, type_name = pybind11_ims_dataset(_native(name), level, t, c)
    size = _swap(size, x_on_imaris_x)                 # (the image's y, x, z, as start and end)
    dtype = np.dtype(type_name)
    start = [0, 0, 0] if start_coords is None else _coords(start_coords, 'start_coords', order)
    end = list(size) if end_coords is None else _coords(end_coords, 'end_coords', order)
    if not any(end):
        end = list(size)
    for i in range(3):
        if not start[i] < end[i] <= size[i]:
            raise ValueError(f'the region [{_image_to_order(start, order)}, {_image_to_order(end, order)}) must lie '
                             f'inside the volume of shape {_image_to_order(size, order)} and end after it starts '
                             'along every axis')
    shape = _image_to_order([e - s for s, e in zip(start, end)], order)
    if out is None:
        out = np.empty(shape, dtype, order=order)
    else:
        if not isinstance(out, np.ndarray):
            raise TypeError(f'out must be a NumPy array, not {type(out).__name__}')
        if out.dtype != dtype or not out.dtype.isnative:
            raise TypeError(f'out must have the data type of the file, {dtype}, not {out.dtype}')
        if out.shape != shape:
            raise ValueError(f'out must have the shape of the region, {shape}, not {out.shape}')
        if not out.flags.writeable:
            raise ValueError('out must be writeable')
    # out viewed along the image's (y, x, z), then along Imaris X, Y, Z
    view = np.transpose(out, (1, 2, 0)) if order == 'C' else out
    if x_on_imaris_x:
        view = np.transpose(view, (1, 0, 2))
    pybind11_read_ims(_native(name), level, t, c, _swap(start, x_on_imaris_x), _swap(end, x_on_imaris_x), view,
                      threads)
    return out


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


# ---- the cluster steps: cluster_plan, cluster_task, cluster_status, cluster_finish

def _workdir(workdir):
    return _path(workdir, 'workdir')


def _plan(raw, order, x_on_imaris_x):
    # A plan of the module (sizes along Imaris X, Y, Z) as cluster_plan returns it
    shape = _image_to_order(_swap(raw['size'], x_on_imaris_x), order)
    return {'workdir': raw['workdir'], 'kind': raw['kind'], 'store': raw['store'], 'file_name': raw['output_file'],
            'timepoints': raw['timepoints'], 'channels': raw['channels'], 'resolution_levels': raw['levels'],
            'shape': shape, 'dtype': np.dtype(raw['data_type']),
            'stages': [dict(s) for s in raw['stages']], 'tasks': sum(s['tasks'] for s in raw['stages'])}


def cluster_plan(workdir, folders=None, channel_patterns=None, files=None, reader=None, file_name=None,
                 voxel_size=None, block_size=None, input_bbox=None, process_block_size=None, order='C',
                 input_axes=None, x_on_imaris_x=False, tile_size=None, store=None, tiles_per_task=None,
                 chunk_bytes=None, channel_names=None, channel_colors=None, emission_wavelengths=None,
                 excitation_wavelengths=None, time_interval=None, start_time=None):
    """Plan a conversion as stages of independent tasks, for any job system (or none): the first of
    the cluster steps. Then run every task of each stage with cluster_task (each in any process on
    any node, in any order, each again after a failure; a stage only after the one before it),
    and write the file with cluster_finish. This never submits a job: the caller's job framework
    runs the tasks, each a single-node call.

    The inputs and options are convert_to_ims's (folders and channel_patterns, or files; reader,
    voxel_size, block_size, input_bbox, process_block_size, order, input_axes, x_on_imaris_x and
    the metadata), and are checked as convert_to_ims checks them before anything is written; the
    finished file equals convert_to_ims of the same inputs and options. file_name: the finished
    .ims (default <workdir>/output.ims), on workdir's filesystem. workdir is made if needed; one
    that holds this same plan is kept (ValueError if it holds another).

    Without tile_size: a time series, stage 'convert' with one task per (time point, channel),
    task i = time point i // channels, channel i % channels; each task converts its volume
    (process_block_size bounds the voxels it holds) and places it in the one file. Linux and
    macOS; uint8, uint16 and float32 data.
    tile_size (three values in the order's axis order, (z, y, x) by default): one enormous volume
    per (time point, channel) cut into tiles, stages 'level0', 'level1', ... (each after the one
    before), tiles_per_task consecutive tiles per task (default 1). Zarr input of (y, x, z) axes
    (input_axes 'yxz'), uint8 or uint16, x_on_imaris_x False. store: 'mono' (straight into the one
    file; the default, not on Windows) or 'packs' (one file per tile, merged by cluster_finish;
    uint16). chunk_bytes: the chunk budget in bytes (default the environment's PIW_CHUNK_BYTES,
    else 1048576, the converter's); another value changes the file's chunks, not its data.

    Returns a dict: workdir, kind ('series' or 'tiles'), store, file_name, timepoints, channels,
    resolution_levels, shape (one volume at level 0, in the order's axis order), dtype, stages (in
    the order they run, each {'name', 'tasks', 'after'}: task indices 0 .. tasks-1, after the
    stage that must be done first, or None) and tasks (all stages').
    """
    workdir = _workdir(workdir)
    order = _order(order)
    x_on_imaris_x = bool(x_on_imaris_x)
    if file_name is not None:
        file_name = _ims_name(file_name)
        if os.path.isdir(file_name):
            raise IsADirectoryError(f'{file_name} is a folder')
    tiles = _sizes(tile_size, 'tile_size', order)
    if tiles is None and (store is not None or tiles_per_task is not None or chunk_bytes is not None):
        raise ValueError('store, tiles_per_task and chunk_bytes are for a tile plan (give tile_size)')
    if store is not None and store not in ('mono', 'packs'):
        raise ValueError(f"store must be 'mono', 'packs' or None (the default), not {store!r}")
    per_task = 1 if tiles_per_task is None else _index(tiles_per_task, 'tiles_per_task')
    if per_task < 1:
        raise ValueError(f'tiles_per_task must be at least 1, not {per_task}')
    budget = 0 if chunk_bytes is None else _index(chunk_bytes, 'chunk_bytes')
    if chunk_bytes is not None and budget < 4096:
        raise ValueError(f'chunk_bytes must be at least 4096, not {budget}')
    c = _conversion(folders, channel_patterns, files, reader, voxel_size, block_size, input_bbox, process_block_size,
                    order, input_axes, channel_names, channel_colors, emission_wavelengths, excitation_wavelengths,
                    time_interval, start_time)
    raw = pybind11_cluster_plan(_native(workdir), c['reader'], c['files'],
                                '' if file_name is None else _native(file_name), c['voxel'], c['block'], c['box'],
                                c['process'], x_on_imaris_x=x_on_imaris_x, input_axes=c['axes'],
                                tile_size=tiles, store=store or '', tiles_per_task=per_task, chunk_bytes=budget,
                                **c['metadata'])
    return _plan(raw, order, x_on_imaris_x)


def _stage(stage):
    # A stage's name, or its number in the plan
    if isinstance(stage, str):
        return stage
    return str(_index(stage, 'stage'))


def cluster_task(workdir, stage, index, num_threads=None, stage_dir=None, process_block_size=None, order='C',
                 progress=None, verbose=False):
    """Run one task of a plan (cluster_plan): task index of stage (its name, such as 'convert' or
    'level0', or its number in the plan's stages). One single-node call; run it in any process on
    any node, for example one array job per stage with the job's index as index.

    A task that is done is skipped (nothing changes); one that failed or was stopped is simply run
    again (also at the same time in two processes: the same bytes go to the same place). A task of
    a stage fails while the stage it comes after is not done; any task fails once the file was
    finished. A failure raises (RuntimeError for an input that cannot be read, such as a truncated
    chunk) and leaves every other task runnable.

    num_threads: the threads of this task (by default OMP_NUM_THREADS, or every CPU). Time series
    only: stage_dir, the folder where the volume is converted before it is placed (by default
    memory, /dev/shm, when it fits the job's memory, else <workdir>/stage); process_block_size, in
    the order's axis order, instead of the plan's. Ctrl-C (KeyboardInterrupt) or an exception from
    progress(fraction, bytes_written) stops a time-series task between blocks and removes what it
    staged; a tile task ends its tiles first. verbose prints the conversion's lines (tiles: one
    timing line per tile).

    Returns a dict: stage (its name), index, skipped, owners (what it placed: P_C<c>_T<t>, or its
    tiles L<r>_C<c>_T<t>_<tile>), timepoint and channel (None for a tile task), memory_stage,
    chunks and bytes placed, convert_seconds, place_seconds, total_seconds.
    """
    workdir = _workdir(workdir)
    stage = _stage(stage)
    index = _index(index, 'index')
    order = _order(order)
    threads = _threads(num_threads)
    progress = _progress(progress)
    process = _sizes(process_block_size, 'process_block_size', order)
    if process is not None:
        plan = pybind11_cluster_read_plan(_native(workdir))
        if plan['kind'] != 'series':
            raise ValueError('process_block_size is for the tasks of a time-series plan')
        process = _input_values(process, plan['input_axes'])
    if verbose:
        sys.stdout.flush()
    r = pybind11_cluster_task(_native(workdir), stage, index, threads,
                              '' if stage_dir is None else _native(_path(stage_dir, 'stage_dir')), process,
                              verbose=bool(verbose), progress=progress)
    lines = r.pop('lines')
    if verbose and lines:
        sys.stdout.write(''.join(line + '\n' for line in lines))
        sys.stdout.flush()
    return r


def cluster_status(workdir):
    """Which tasks of a plan are done (their results are in place); changes nothing.

    Returns a dict: kind, store, stages (each {'name', 'tasks', 'missing'}: the indices of the
    tasks not done), missing (their number over all stages), sealed (cluster_finish has started:
    no more tasks run), finished, and file_name (the finished file, or None).
    """
    r = pybind11_cluster_status(_native(_workdir(workdir)))
    return {'kind': r['kind'], 'store': r['store'], 'stages': [dict(s) for s in r['stages']],
            'missing': sum(len(s['missing']) for s in r['stages']), 'sealed': r['sealed'], 'finished': r['finished'],
            'file_name': r['finished_file']}


def cluster_finish(workdir, file_name=None, verify=False, num_threads=None):
    """Write the file of a plan once all its tasks are done: the last of the cluster steps.

    FileNotFoundError, naming the tasks not done per stage, while any task is not done (run them,
    then this again). file_name: the .ims to write instead of the plan's. verify: also read and
    decode every chunk of the file again. Running it again after it finished does nothing
    (already_finished). num_threads as cluster_task's.

    Returns a dict: file_name, already_finished, timepoints, channels, resolution_levels, chunks
    (placed by the tasks), file_bytes, total_seconds.
    """
    workdir = _workdir(workdir)
    if file_name is not None:
        file_name = _ims_name(file_name)
    threads = _threads(num_threads)
    r = pybind11_cluster_finish(_native(workdir), '' if file_name is None else _native(file_name), bool(verify),
                                threads)
    return {'file_name': r['output_file'], 'already_finished': r['already_finished'],
            'timepoints': r['timepoints'], 'channels': r['channels'], 'resolution_levels': r['levels'],
            'chunks': r['chunks'], 'file_bytes': r['file_bytes'], 'total_seconds': r['total_seconds']}
