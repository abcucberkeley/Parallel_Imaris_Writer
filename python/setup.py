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
                # (cpp-zarr's libuuid step needs make: not Ninja)
                '-G', 'Unix Makefiles',
                '-DCMAKE_BUILD_TYPE=Release',
                f'-DCMAKE_INSTALL_PREFIX={install_dir}',
                '-DCMAKE_INSTALL_LIBDIR=lib',
                '-DPIW_BUILD_PYTHON_LIB=ON',
                # (the libraries' C++ runtimes kept private, as the module's is: see CMakeLists.txt)
                '-DPIW_PRIVATE_RUNTIME=ON',
            ])
            cmake = configured_cmake(cmake_build_dir)
        if cmake and os.path.isfile(cmake):
            subprocess.check_call([cmake, '--build', cmake_build_dir, '--parallel', build_jobs(),
                                   '--target', 'parallelImarisWriter_shared'])
            subprocess.check_call([cmake, '--install', cmake_build_dir, '--component', 'python'])
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


ext_modules = [
    Extension(
        'parallel_imaris_writer.parallel_imaris_writer',
        include_dirs=['build/install/include', pybind11.get_include()],
        library_dirs=['build/install/lib64', 'build/install/lib'],
        libraries=['parallelImarisWriter'],
        sources=['parallel_imaris_writer.cpp'],
        # Only the module's init function exported
        extra_compile_args=['-fvisibility=hidden', '-fvisibility-inlines-hidden'] if system != 'Windows' else [],
        # (the static C++ runtime kept private, as in the library: see CMakeLists.txt)
        extra_link_args=['-static-libstdc++'] + ([f'-Wl,--exclude-libs,ALL,--version-script={module_version_script}']
                                                 if system == 'Linux' else []),
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
    # Windows: the library is built beforehand (not by setup.py) and its DLLs are package data
    cmdclass={'build_ext': BuildExt} if system != 'Windows' else {'build_ext': build_ext},
    install_requires=['numpy>=1.20'],
    package_data={'': ['*.dll']} if system == 'Windows' else {},
    include_package_data=True,
    entry_points={'console_scripts': ['parallelimariswriter = parallel_imaris_writer.__main__:main']},
    zip_safe=False,
    python_requires='>=3.9, <3.15',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
