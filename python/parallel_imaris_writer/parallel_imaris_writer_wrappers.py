import datetime
import numbers
import operator
import os
import secrets
import sys
from collections.abc import Mapping
import numpy as np
from .parallel_imaris_writer import (pybind11_ims_dataset, pybind11_ims_info, pybind11_read_ims, pybind11_run_cli,
                                     pybind11_version, pybind11_write_ims)

__version__ = pybind11_version()

# Axes. order='C' (the default) names arrays (z, y, x), (c, z, y, x) and (t, c, z, y, x), and
# per-axis values (z, y, x); order='F' MATLAB's (y, x, z), (y, x, z, c) and (y, x, z, c, t), and
# per-axis values (y, x, z). Either way Imaris X is the image's y, Imaris Y its x and Imaris Z its
# z, as in every file the command line (and PetaKit5D's MATLAB code) writes. The order names the
# axes only: any memory layout is read in place and means the same.

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
    # mapping with the keys 'x', 'y' and 'z' (the image's axes), as Imaris X, Y, Z: (y, x, z)
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


def _sizes(values, name, order):
    # Whole numbers of at least 1 along Imaris X, Y, Z, or None
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


def write_ims(file_name, data, voxel_size=None, block_size=None, order='C', num_threads=None, progress=None,
              verbose=False, channel_names=None, channel_colors=None, emission_wavelengths=None,
              excitation_wavelengths=None, time_interval=None, start_time=None):
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
    every image).

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
    whole, and a failed or stopped write leaves nothing. Calls in one process write one at a time.
    """
    file_name = _ims_name(file_name)
    order = _order(order)
    threads = _threads(num_threads)
    progress = _progress(progress)
    volumes, channels, timepoints = _volumes(data, order)
    voxel = _voxel_size(voxel_size, order)
    block = _sizes(block_size, 'block_size', order)
    metadata = _metadata(channel_names, channel_colors, emission_wavelengths, excitation_wavelengths, time_interval,
                         start_time)
    if os.path.isdir(file_name):
        raise IsADirectoryError(f'{file_name} is a folder')
    if verbose:
        # (what Python printed before comes first)
        sys.stdout.flush()
    temporary = None
    try:
        temporary = _temporary(file_name)
        pybind11_write_ims(_native(temporary), volumes, channels, timepoints, voxel, block, threads,
                           verbose=bool(verbose), progress=progress, **metadata)
        os.replace(temporary, file_name)
    except BaseException:
        if temporary is not None:
            _remove(temporary)
        raise


def _to_order(xyz, order):
    # Values along Imaris X, Y, Z (the image's y, x, z) in the order's axis order
    return (xyz[2], xyz[0], xyz[1]) if order == 'C' else tuple(xyz)


def _existing(file_name):
    name = _path(file_name, 'file_name')
    if not os.path.isfile(name):
        raise FileNotFoundError(f'{name} does not exist')
    return name


def _single(value):
    # A single-precision value of the file as the shortest decimal that is that value (0.108, not
    # 0.10800000280141830)
    return float(str(np.float32(value)))


def ims_info(file_name, checksums=False, order='C'):
    """What an .ims file holds, as a dict; sizes and per-axis values in the order's axis order
    ((z, y, x) by default, (y, x, z) for order='F'):

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
    raw = pybind11_ims_info(_native(_existing(file_name)), bool(checksums))
    size = raw['size']
    ext_min, ext_max, voxel = ([_single(v) for v in raw[k]] for k in ('extent_min', 'extent_max', 'voxel_size'))
    colors = []
    for text in raw['channel_colors']:
        try:
            colors.append(tuple(float(v) for v in text.split()))
        except ValueError:
            colors.append(text)
    datasets = [{'resolution_level': d['level'], 'timepoint': d['timepoint'], 'channel': d['channel'],
                 'shape': _to_order(d['size'], order), 'chunk_shape': _to_order(d['chunk'], order),
                 'chunks': d['chunks'], 'crc': d['crc'], 'histogram_crc': d['histogram_crc']}
                for d in raw['datasets']]
    return {'shape': _to_order(size, order), 'dtype': np.dtype(raw['data_type']), 'channels': raw['channels'],
            'timepoints': raw['timepoints'], 'resolution_levels': raw['levels'],
            'voxel_size': _to_order(voxel, order), 'extent_min': _to_order(ext_min, order),
            'extent_max': _to_order(ext_max, order), 'channel_names': list(raw['channel_names']),
            'channel_colors': colors, 'time_points': list(raw['time_points']), 'datasets': datasets,
            'thumbnail_crc': raw['thumbnail_crc'], 'attributes': dict(raw['attributes'])}


def _coords(values, name, order):
    # Three whole numbers in the order's axis order, as Imaris X, Y, Z
    return [_index(v, f'{name} values') for v in _per_axis(values, name, order)]


def read_ims(file_name, resolution_level=0, timepoint=0, channel=0, start_coords=None, end_coords=None, order='C',
             num_threads=None, out=None):
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
    """
    order = _order(order)
    threads = _threads(num_threads)
    level, t, c = (_index(resolution_level, 'resolution_level'), _index(timepoint, 'timepoint'),
                   _index(channel, 'channel'))
    name = _existing(file_name)
    size, type_name = pybind11_ims_dataset(_native(name), level, t, c)
    dtype = np.dtype(type_name)
    start = [0, 0, 0] if start_coords is None else _coords(start_coords, 'start_coords', order)
    end = list(size) if end_coords is None else _coords(end_coords, 'end_coords', order)
    if not any(end):
        end = list(size)
    for i in range(3):
        if not start[i] < end[i] <= size[i]:
            raise ValueError(f'the region [{_to_order(start, order)}, {_to_order(end, order)}) must lie inside the '
                             f'volume of shape {_to_order(size, order)} and end after it starts along every axis')
    shape = _to_order([e - s for s, e in zip(start, end)], order)
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
    pybind11_read_ims(_native(name), level, t, c, start, end, np.transpose(out, (1, 2, 0)) if order == 'C' else out,
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
