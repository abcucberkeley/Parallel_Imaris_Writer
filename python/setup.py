import os
import platform
import re
import subprocess
from glob import glob
from setuptools import setup, find_packages, Extension
from pybind11.setup_helpers import build_ext
import pybind11

system = platform.system()
here = os.path.dirname(os.path.abspath(__file__))
# (Linux: the extension's exports, written by BuildExt)
module_version_script = os.path.join(here, 'build', 'module.map')

# The CMake project: the CMakeLists.txt next to this file (when it was copied here) or the
# repository's, one level up.
cmake_source_dir = next((d for d in (here, os.path.dirname(here))
                         if os.path.isfile(os.path.join(d, 'CMakeLists.txt'))), None)
if cmake_source_dir is None:
    raise RuntimeError('CMakeLists.txt not found next to setup.py or one level up')


def cmake_version():
    # project(... VERSION x.y.z) of the CMakeLists.txt: one version for the program and the package
    with open(os.path.join(cmake_source_dir, 'CMakeLists.txt')) as f:
        m = re.search(r'project\s*\([^)]*?\bVERSION\s+([0-9]+(?:\.[0-9]+)*)', f.read())
    if m is None:
        raise RuntimeError('No project(... VERSION ...) in CMakeLists.txt')
    return m.group(1)


def build_jobs():
    # PIW_BUILD_JOBS, else the CPUs this process may run on (in a container on a cluster node
    # os.cpu_count() is the whole node)
    if os.environ.get('PIW_BUILD_JOBS'):
        return os.environ['PIW_BUILD_JOBS']
    try:
        return str(len(os.sched_getaffinity(0)))
    except AttributeError:
        return str(os.cpu_count() or 2)


def configured_cmake(cmake_build_dir):
    # The cmake that configured cmake_build_dir (its CMakeCache.txt), or None before the first configure
    try:
        with open(os.path.join(cmake_build_dir, 'CMakeCache.txt')) as f:
            for line in f:
                if line.startswith('CMAKE_COMMAND:'):
                    return line.split('=', 1)[1].strip()
    except FileNotFoundError:
        return None
    return ''


class BuildExt(build_ext):
    def run(self):
        # The library (libparallelImarisWriter, with libcppTiff and libcppZarr) is built with
        # CMake into build/ and installed into build/install, then the extension is linked to it.
        # Windows: with MinGW-w64 (MSYS2's UCRT64 gcc, g++, cmake and ninja first on PATH), and the
        # extension too (see the end of this file); the repair step (delvewheel) then puts the DLLs
        # into the wheel.
        cmake_build_dir = os.path.join(here, 'build')
        install_dir = os.path.join(cmake_build_dir, 'install')
        cmake = configured_cmake(cmake_build_dir)
        installed = glob(os.path.join(install_dir, 'lib*', 'libparallelImarisWriter.*'))
        # Configured once: a second configure breaks cpp-zarr's libuuid step. Later builds (other
        # Python versions) reuse it, built with the cmake that configured it. pip's isolated build
        # environment (with its cmake) is removed after each build: then the installed library is
        # reused as it is (delete build/ to rebuild it).
        if cmake is None:
            os.makedirs(cmake_build_dir, exist_ok=True)
            subprocess.check_call([
                'cmake', '-S', cmake_source_dir, '-B', cmake_build_dir,
                # (cpp-zarr's libuuid step needs make: not Ninja; Windows has no libuuid step)
                '-G', 'Ninja' if system == 'Windows' else 'Unix Makefiles',
                '-DCMAKE_BUILD_TYPE=Release',
                # (no CMAKE_INSTALL_PREFIX: the install below gives the folder, so HDF5's configure
                # summary, compiled into the library, names the default prefix and not this folder)
                '-DCMAKE_INSTALL_LIBDIR=lib',
                # (its C++ runtime, and the reader libraries', kept private, as the module's is: see
                # CMakeLists.txt)
                '-DPIW_BUILD_PYTHON_LIB=ON',
            ])
            cmake = configured_cmake(cmake_build_dir)
        if cmake and os.path.isfile(cmake):
            subprocess.check_call([cmake, '--build', cmake_build_dir, '--parallel', build_jobs(),
                                   '--target', 'parallelImarisWriter_shared'])
            subprocess.check_call([cmake, '--install', cmake_build_dir, '--component', 'python',
                                   '--prefix', install_dir])
        elif installed:
            print(f'Reusing the library built in {cmake_build_dir} (its cmake, {cmake}, is gone)')
        else:
            raise RuntimeError(f'{cmake_build_dir} was configured by a cmake that is gone ({cmake}) and holds no '
                               'library: delete it and build again')
        if system == 'Linux':
            # Only the module's init function exported (the template instantiations of namespace
            # std keep default visibility despite -fvisibility=hidden)
            with open(module_version_script, 'w') as f:
                f.write('{ global: PyInit_*; local: *; };\n')

        # Run the regular build_ext to compile the Python extension
        super().run()


# No debug information, and the folders of this build recorded as relative names in the code's
# file names (pybind11's messages): the wheel's binaries name no folder of the machine that built
# them (as the library's, see CMakeLists.txt).
compile_args = ['-g0', f'-ffile-prefix-map={here}=.',
                f'-ffile-prefix-map={os.path.dirname(pybind11.get_include())}=pybind11']
define_macros = []
# Its own static C++ runtime, as the library's (see CMakeLists.txt), and only the module's init
# function exported
if system == 'Linux':
    # (the runtime kept private, __cxa_pure_virtual included; the unwinder is the shared libgcc_s)
    compile_args += ['-fvisibility=hidden', '-fvisibility-inlines-hidden']
    link_args = ['-static-libstdc++', f'-Wl,--exclude-libs,ALL,--version-script={module_version_script}',
                 '-Wl,--undefined=__cxa_pure_virtual']
elif system == 'Darwin':
    # (exported, the runtime's weak definitions would be merged by the loader with those of every
    # other image of the process that exports the same, as another GCC-built module's; the unwinder
    # is libSystem's, and -static-libgcc keeps Homebrew's libgcc_s out)
    compile_args += ['-fvisibility=hidden', '-fvisibility-inlines-hidden']
    link_args = ['-static-libstdc++', '-static-libgcc', '-Wl,-exported_symbol,_PyInit_parallel_imaris_writer']
else:
    # MinGW-w64 for python.org's Python (MSVC-built; the library's API is C++, so the module is built
    # with the library's compiler): the headers of Python 3.9 to 3.11 define MS_WIN64 for MSVC only
    # (defined empty, as 3.12's own definition for MinGW); NDEBUG as Python's own flags have it on
    # Linux and macOS (the assertions of Python's headers left out). The module exports PyInit_* alone
    # (its dllexport) and is stripped (-s, setuptools' MinGW linking).
    compile_args += ['-O2']
    define_macros += [('MS_WIN64', ''), ('NDEBUG', None)]
    link_args = ['-static-libstdc++', '-static-libgcc']

ext_modules = [
    Extension(
        'parallel_imaris_writer.parallel_imaris_writer',
        include_dirs=['build/install/include', pybind11.get_include()],
        library_dirs=['build/install/lib64', 'build/install/lib'],
        libraries=['parallelImarisWriter'],
        sources=['parallel_imaris_writer.cpp'],
        define_macros=define_macros,
        extra_compile_args=compile_args,
        extra_link_args=link_args,
    ),
]


setup(
    name='parallel-imaris-writer',
    version=cmake_version(),
    description='Write Imaris (.ims) files from TIFF and Zarr volumes, in parallel',
    url='https://github.com/abcucberkeley/Parallel_Imaris_Writer',
    license='Apache-2.0',
    packages=find_packages(exclude=['tests']),
    ext_modules=ext_modules,
    cmdclass={'build_ext': BuildExt},
    # Windows: MinGW-w64, as the library (above). The wheel holds no DLL of its own: the repair step
    # (delvewheel) adds the library, the readers and MinGW's runtime DLLs under new names, so that none
    # is taken for the DLL of the same name another package loaded (cpp-tiff's and cpp-zarr's wheels
    # ship libcppTiff.dll, libcppZarr.dll, libgomp-1.dll and libwinpthread-1.dll).
    options={'build_ext': {'compiler': 'mingw32'}} if system == 'Windows' else {},
    install_requires=['numpy>=1.20'],
    include_package_data=True,
    entry_points={'console_scripts': ['parallelimariswriter = parallel_imaris_writer.__main__:main']},
    zip_safe=False,
    python_requires='>=3.9, <3.15',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
