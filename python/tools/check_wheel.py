"""Lists the files of built wheels and checks what the repair step bundled into them.

    python tools/check_wheel.py [--forbid TEXT]... WHEEL...

Run by the Wheels workflow (.github/workflows/wheels.yml) on every wheel it builds; after
build_manylinux_wheels.sh as well. Checked for each wheel, by its platform tag:
  * the extension module, the console script, and the native libraries: libparallelImarisWriter,
    libcppTiff, libcppZarr and the OpenMP runtime (libgomp)
      Linux (auditwheel): in parallel_imaris_writer.libs/, renamed with a hash;
      macOS (delocate): in parallel_imaris_writer/.dylibs/;
      Windows (delvewheel): in parallel_imaris_writer.libs/, renamed with a hash, with MinGW's
      libwinpthread (and libgcc_s_seh where MinGW's libgomp needs it). No DLL may be named
      libcppTiff.dll, libcppZarr.dll, libgomp-1.dll or libwinpthread-1.dll: cpp-tiff's and
      cpp-zarr's wheels ship DLLs of those names, and Windows takes an already loaded DLL of the
      same name for the one a library asks for;
  * no binary holds a --forbid text, such as the folder the wheel was built in or the home folder
    (on Windows also checked with / and \\ swapped).
Exits with status 1 when a check fails. Only the standard library is needed.
"""
import argparse
import re
import sys
import zipfile

# The libraries every wheel bundles: (name pattern on Linux, on macOS, on Windows)
LIBRARIES = [
    ('libparallelImarisWriter', r'libparallelImarisWriter-[0-9a-f]+\.so$', r'libparallelImarisWriter\.dylib$',
     r'libparallelImarisWriter-[0-9a-f]+\.dll$'),
    ('libcppTiff', r'libcppTiff-[0-9a-f]+\.so$', r'libcppTiff\.dylib$', r'libcppTiff-[0-9a-f]+\.dll$'),
    ('libcppZarr', r'libcppZarr-[0-9a-f]+\.so$', r'libcppZarr\.dylib$', r'libcppZarr-[0-9a-f]+\.dll$'),
    ('libgomp', r'libgomp-[0-9a-f]+\.so(\.[0-9]+)*$', r'libgomp\.1\.dylib$', r'libgomp-1-[0-9a-f]+\.dll$'),
]
WINDOWS_ONLY = [('libwinpthread', r'libwinpthread-1-[0-9a-f]+\.dll$')]
# DLL names cpp-tiff's and cpp-zarr's Windows wheels use (compared in lower case)
TAKEN_DLL_NAMES = {'libcpptiff.dll', 'libcppzarr.dll', 'libgomp-1.dll', 'libwinpthread-1.dll'}
BINARY_MAGIC = (b'\x7fELF', b'MZ', b'\xcf\xfa\xed\xfe', b'\xce\xfa\xed\xfe', b'\xca\xfe\xba\xbe')


def platform_of(wheel):
    tag = wheel.rsplit('-', 1)[-1]
    if 'linux' in tag:
        return 'linux'
    if tag.startswith('macosx'):
        return 'macos'
    if tag.startswith('win'):
        return 'windows'
    return None


def check(path, forbid):
    errors = []
    system = platform_of(path)
    print(f'== {path}')
    with zipfile.ZipFile(path) as z:
        infos = z.infolist()
        names = [i.filename for i in infos]
        for i in infos:
            print(f'   {i.file_size:>11,}  {i.filename}')
        if system is None:
            return [f'{path}: no Linux, macOS or Windows platform tag']
        libdir = 'parallel_imaris_writer/.dylibs/' if system == 'macos' else 'parallel_imaris_writer.libs/'
        bundled = [n[len(libdir):] for n in names if n.startswith(libdir) and '/' not in n[len(libdir):]]
        column = {'linux': 1, 'macos': 2, 'windows': 3}[system]
        expected = [(lib[0], lib[column]) for lib in LIBRARIES]
        if system == 'windows':
            expected += WINDOWS_ONLY
        for what, pattern in expected:
            found = [b for b in bundled if re.match(pattern, b)]
            if len(found) == 1:
                print(f'ok    {what}: {libdir}{found[0]}')
            else:
                errors.append(f'{path}: {what}: expected one file matching {libdir}{pattern}, found {found}')
        module = r'parallel_imaris_writer/parallel_imaris_writer\.' + (r'cp3[0-9]+t?-win_amd64\.pyd$' if system == 'windows'
                                                                     else r'cpython-3[0-9]+t?-.*\.so$')
        modules = [n for n in names if re.match(module, n)]
        if len(modules) == 1:
            print(f'ok    extension module: {modules[0]}')
        else:
            errors.append(f'{path}: expected one extension module matching {module}, found {modules}')
        entry_points = [n for n in names if n.endswith('.dist-info/entry_points.txt')]
        if entry_points and 'parallelimariswriter = parallel_imaris_writer.__main__:main' in \
                z.read(entry_points[0]).decode():
            print('ok    console script parallelimariswriter')
        else:
            errors.append(f'{path}: no console script parallelimariswriter in the entry points')
        if system == 'windows':
            taken = [n for n in names if n.rsplit('/', 1)[-1].lower() in TAKEN_DLL_NAMES]
            if taken:
                errors.append(f"{path}: DLLs named as cpp-tiff's or cpp-zarr's: {taken}")
            else:
                print("ok    no DLL named as one of cpp-tiff's or cpp-zarr's (" + ', '.join(sorted(TAKEN_DLL_NAMES)) + ')')
            print('       other DLLs: ' + (', '.join(b for b in bundled if not any(re.match(p, b) for _, p in expected))
                                         or 'none'))
        texts = []
        for t in forbid:
            texts.append(t)
            if system == 'windows':
                texts += [t.replace('\\', '/'), t.replace('/', '\\')]
        texts = sorted(set(t.encode() for t in texts if t))
        binaries = 0
        for i in infos:
            data = z.read(i)
            if not data.startswith(BINARY_MAGIC):
                continue
            binaries += 1
            for t in texts:
                if t in data:
                    errors.append(f'{path}: {i.filename} holds {t.decode()!r} ({data.count(t)} times)')
        if texts:
            print(f'       {binaries} binaries searched for ' + ', '.join(repr(t.decode()) for t in texts))
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--forbid', action='append', default=[], metavar='TEXT',
                        help='text no binary of the wheel may hold (e.g. the build folder); repeatable')
    parser.add_argument('wheels', nargs='+', metavar='WHEEL')
    args = parser.parse_args()
    errors = []
    for w in args.wheels:
        errors += check(w, args.forbid)
    if errors:
        print(f'{len(errors)} FAILED:\n  ' + '\n  '.join(errors))
        sys.exit(1)
    print(f'{len(args.wheels)} wheel(s) checked: OK')


if __name__ == '__main__':
    main()
