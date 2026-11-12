cmake_minimum_required(VERSION 3.18)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

project(
  parallel-imaris-writer
  VERSION 1.1.0
  LANGUAGES CXX)

include(GNUInstallDirs)
include(FetchContent)

# Build the fetched static deps with position-independent code so they can be
# linked into our (shared) libraries and the executable.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Everything we fetch is linked statically except the two abc libraries below,
# which are declared SHARED explicitly and are therefore unaffected by this.
set(BUILD_SHARED_LIBS OFF)

# Set the RPATH so the executable finds the shared libraries shipped next to it.
# Flat layout: everything lives in one folder. macOS uses @loader_path, not $ORIGIN.
if(APPLE)
  set(CMAKE_INSTALL_RPATH "@loader_path")
else()
  set(CMAKE_INSTALL_RPATH "$ORIGIN")
endif()
# Bake that RPATH into the binaries as they are built (not only at install time), so
# the executable/libraries are relocatable straight out of the build tree -- this is
# what lets the `release` target below just copy them into one folder and run.
set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)

# OpenMP is required for C++ only (main.cpp and the abc libraries use it; the C
# deps do not, and requiring it for C would break Apple Clang's C compiler).
find_package(OpenMP REQUIRED COMPONENTS CXX)
find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# zlib -- one in-tree build shared by libtiff (inside cpp-tiff) and ImarisWriter.
# Declaring it here first means cpp-tiff's own FetchContent(zlib) deduplicates to
# this one instead of creating a second, conflicting zlibstatic target.
# ---------------------------------------------------------------------------
if(UNIX)
  set(ZLIB_STATIC_NAME "libz.a")
else()
  set(ZLIB_STATIC_NAME "libzlibstatic.a")
endif()
set(SKIP_INSTALL_ALL ON CACHE BOOL "" FORCE)
set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
FetchContent_Declare(zlib
    URL      https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz
    URL_HASH SHA256=9a93b2b7dfdac77ceba5a558a580e74667dd6fede4585b91eefb60f03b72df23
)
FetchContent_MakeAvailable(zlib)
set_property(TARGET zlibstatic PROPERTY POSITION_INDEPENDENT_CODE ON)
# zconf.h is generated into the binary dir, so expose both dirs to libtiff's FindZLIB.
set(ZLIB_INCLUDE_DIR "${zlib_SOURCE_DIR};${zlib_BINARY_DIR}" CACHE STRING "" FORCE)
set(ZLIB_LIBRARY     "${zlib_BINARY_DIR}/${ZLIB_STATIC_NAME}" CACHE STRING "" FORCE)

# ---------------------------------------------------------------------------
# lz4 -- ImarisWriter registers an HDF5 LZ4 filter (bpH5LZ4) and links liblz4.
# ---------------------------------------------------------------------------
set(LZ4_BUILD_CLI OFF CACHE BOOL "" FORCE)
FetchContent_Declare(lz4
    URL      https://github.com/lz4/lz4/releases/download/v1.10.0/lz4-1.10.0.tar.gz
    URL_HASH SHA256=537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b
    SOURCE_SUBDIR build/cmake
)
FetchContent_MakeAvailable(lz4)
set(LZ4_INCLUDE_DIR "${lz4_SOURCE_DIR}/lib")

# ---------------------------------------------------------------------------
# HDF5 -- core C library only. ImarisWriter uses core H5Dwrite_chunk (no
# high-level lib). Built without zlib/szip: this tool only ever writes with LZ4
# (eCompressionAlgorithmLShuffleLZ4), and H5Pset_deflate still links fine without
# the deflate filter -- so we skip the fragile "point HDF5 at an in-tree zlib" wiring.
# ---------------------------------------------------------------------------
set(HDF5_ENABLE_Z_LIB_SUPPORT OFF CACHE BOOL "" FORCE)
set(HDF5_ENABLE_SZIP_SUPPORT  OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_HL_LIB   OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_CPP_LIB  OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_FORTRAN  OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_JAVA     OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_TOOLS    OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(HDF5_BUILD_UTILS    OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING       OFF CACHE BOOL "" FORCE)
# HDF5's _Float16 support uses FLT16_MAX, which GCC 15's MinGW headers don't define
# (the HDF5 build fails); the .ims files never store 16-bit floats.
set(HDF5_ENABLE_NONSTANDARD_FEATURE_FLOAT16 OFF CACHE BOOL "" FORCE)
FetchContent_Declare(hdf5
    URL      https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.6/hdf5-1.14.6.tar.gz
    URL_HASH SHA256=e4defbac30f50d64e1556374aa49e574417c9e72c6b1de7a4ff88c4b1bea6e9b
)
FetchContent_MakeAvailable(hdf5)

# ---------------------------------------------------------------------------
# cpp-tiff and cpp-zarr -- your reader libraries, built as libcppTiff/libcppZarr.
# Pinned to release tags for reproducibility; bump the GIT_TAGs to pick up new
# versions. cpp-tiff v1.5.1+ carries the Z-range readTiffParallelWrapper overload
# the -C block feature needs.
# ---------------------------------------------------------------------------
# cpp-zarr's bundled c-blosc2 otherwise builds its own zlib-ng, whose "zlib"/
# "zlibstatic" targets collide with the zlib we (and cpp-tiff) fetched above.
# Point blosc2 at the shared zlib via find_package(ZLIB) instead.
set(PREFER_EXTERNAL_ZLIB ON CACHE BOOL "" FORCE)
# The two libraries link their C++ runtime (libstdc++), LZ4, zlib, blosc, zstd and libtiff
# statically and, as their releases are built, export all of it. Built here they export only
# their own API (piw_reader_exports below), and every program and library of this project brings
# its own runtime, LZ4 and zlib (piw_link_like_executable): none of them binds a symbol of these
# to a reader library. Bound to the readers' copies, a program ran two C++ runtimes at once (on
# macOS iostreams, locales and std::filesystem copies aborted silently), and the LZ4 compressing
# every .ims chunk was whichever one cpp-zarr bundled. tests/runtime_isolation.cmake checks it.
# CPPZARR_PRIVATE_RUNTIME is cpp-zarr's own switch for this on Linux (--exclude-libs,ALL); left off
# on macOS, where newer cpp-zarr versions add an unexported-symbols list that our exported-symbols
# list (piw_reader_exports) already covers.
if(APPLE)
  set(CPPZARR_PRIVATE_RUNTIME OFF CACHE BOOL "" FORCE)
else()
  set(CPPZARR_PRIVATE_RUNTIME ON CACHE BOOL "" FORCE)
endif()
FetchContent_Declare(cpptiff
    GIT_REPOSITORY https://github.com/abcucberkeley/cpp-tiff.git
    GIT_TAG        v1.7.3
)
FetchContent_Declare(cppzarr
    GIT_REPOSITORY https://github.com/abcucberkeley/cpp-zarr.git
    GIT_TAG        v1.7.3
)
FetchContent_MakeAvailable(cpptiff cppzarr)

# The readers' exports: their API alone, the functions of the global namespace (_Z<length><name>...,
# all of cpp-tiff's and cpp-zarr's, listed by the length's first digit to keep to plain * patterns)
# and cpp-zarr's zarr class; the template code of namespace std and
# nlohmann/json their own objects instantiate is left out too (it keeps default visibility whatever
# -fvisibility says). Linux: no symbol of a static library exported (--exclude-libs,ALL) and a
# version script; macOS: an exported-symbols list (Mach-O names have one more leading underscore).
# Windows (MinGW, which exports every global symbol of a DLL): no symbol of a static library
# exported; the DLLs' own template instantiations stay exported, but nothing of ours imports them.
set(PIW_READER_API _Z1* _Z2* _Z3* _Z4* _Z5* _Z6* _Z7* _Z8* _Z9* _ZN4zarr* _ZNK4zarr*)
function(piw_reader_exports target)
    if(WIN32)
        target_link_options(${target} PRIVATE -Wl,--exclude-libs,ALL)
        return()
    endif()
    if(APPLE)
        list(TRANSFORM PIW_READER_API PREPEND "_" OUTPUT_VARIABLE syms)
        string(REPLACE ";" "\n" text "${syms}")
        set(file "${CMAKE_CURRENT_BINARY_DIR}/${target}.exports")
        file(WRITE "${file}" "${text}\n")
        target_link_options(${target} PRIVATE "LINKER:-exported_symbols_list,${file}")
    else()
        string(REPLACE ";" "; " text "${PIW_READER_API}")
        set(file "${CMAKE_CURRENT_BINARY_DIR}/${target}.map")
        file(WRITE "${file}" "{\n  global: ${text};\n  local: *;\n};\n")
        # (__cxa_pure_virtual: see piw_link_like_executable)
        target_link_options(${target} PRIVATE -Wl,--exclude-libs,ALL "-Wl,--version-script=${file}"
                            -Wl,--undefined=__cxa_pure_virtual)
    endif()
    set_property(TARGET ${target} APPEND PROPERTY LINK_DEPENDS "${file}")
endfunction()
piw_reader_exports(cppTiff_shared)
piw_reader_exports(cppZarr_shared)

# ---------------------------------------------------------------------------
# ImarisWriter -- fetched but NOT configured with its own CMakeLists (which does
# find_package(HDF5/ZLIB/LZ4 REQUIRED) and fights FetchContent). Pointing
# SOURCE_SUBDIR at a headers-only dir makes MakeAvailable populate the source
# without calling add_subdirectory; we then compile it ourselves below.
# ---------------------------------------------------------------------------
FetchContent_Declare(imariswriter
    GIT_REPOSITORY https://github.com/imaris/ImarisWriter.git
    GIT_TAG        b128e6e7d1a147261e9d5caf24ebc6b5c9c63779
    SOURCE_SUBDIR  interfaceC
)
FetchContent_MakeAvailable(imariswriter)

# A few ImarisWriter files are replaced by modified copies kept in third_party/imariswriter/
# (a failed write ends the conversion with an error instead of hanging it; the README there
# lists every change). ImarisWriter is compiled from a copy of the fetched tree with those
# files in place; configuring stops if a fetched file is not the one its override was made from.
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/imariswriter_overrides.cmake)
piw_imariswriter_tree("${imariswriter_SOURCE_DIR}" IW_DIR IW_SRCS)

add_library(ImarisWriter_static STATIC ${IW_SRCS})
# ImarisWriter is C++14; keep it isolated from the C++11 used for the rest.
set_target_properties(ImarisWriter_static PROPERTIES CXX_STANDARD 14 CXX_STANDARD_REQUIRED ON)
# We link ImarisWriter statically, but its C interface header defaults to
# __declspec(dllimport) on Windows -- which makes MinGW look for __imp_* symbols that
# a static lib doesn't have. EXPORT_IMARISWRITER_DLL switches it to dllexport (plain
# symbols); PUBLIC so main.cpp sees it too. No-op on Linux/macOS (visibility default
# either way).
target_compile_definitions(ImarisWriter_static PUBLIC EXPORT_IMARISWRITER_DLL)
target_include_directories(ImarisWriter_static
    PUBLIC
        ${IW_DIR}
        ${IW_DIR}/interface
        ${IW_DIR}/interfaceC
        ${IW_DIR}/writer
    PRIVATE
        ${hdf5_SOURCE_DIR}/src
        ${hdf5_BINARY_DIR}/src
        ${zlib_SOURCE_DIR}
        ${zlib_BINARY_DIR}
        ${LZ4_INCLUDE_DIR}
)
# PUBLIC so the executable that links ImarisWriter_static inherits these static deps.
target_link_libraries(ImarisWriter_static PUBLIC
    hdf5-static
    zlibstatic
    lz4_static
    Threads::Threads
)

# ---------------------------------------------------------------------------
# Converter core (piw_core) and the parallelimariswriter executable
# ---------------------------------------------------------------------------
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
# Every src/*.cpp except main.cpp is the converter core: converter.cpp (piw::convert and
# piw::writeVolumes, the library API of parallelimariswriter.h), imsread.cpp (piw::imsInfo and
# piw::readIms), cli.cpp (the command line, piw::runCli) and parallel.cpp (the cluster
# subcommands; it reads zarr chunks and writes raw HDF5 chunks directly, so it needs the
# HDF5 + lz4 headers). Compiled once, linked into the executable
# and into the tests that call the API. After adding a source file, use a fresh build
# directory (a re-configure breaks cpp-zarr's libuuid step).
file(GLOB PIW_CORE_SOURCES CONFIGURE_DEPENDS ${SRC_DIR}/*.cpp)
list(REMOVE_ITEM PIW_CORE_SOURCES ${SRC_DIR}/main.cpp)
add_library(piw_core OBJECT ${PIW_CORE_SOURCES})
# mono*.cpp: the single-file cluster output core (placement VFD, claims, sidecars), compiled on
# their own into the mono-core test, and mono.cpp, the steps built on it (monoplace, monofinish, ...);
# all part of piw_core through the glob.
set(MONO_SRCS ${SRC_DIR}/monovfd.cpp ${SRC_DIR}/monoclaim.cpp ${SRC_DIR}/monoidx.cpp)

# cpp-tiff's src dir is listed before cpp-zarr's on purpose: both ship a
# helperfunctions.h and converter.cpp includes it unqualified -- cpp-tiff's must win,
# matching the original gccImaris.txt include order.
set(PIW_INCLUDE_DIRS
    ${IW_DIR}/interfaceC        # bpImageConverterInterfaceC.h + bpConverterTypesC.h
    ${IW_DIR}/writer            # bpDeriche.h (parallel.cpp reuses the Gaussian for channel ColorRange)
    ${cpptiff_SOURCE_DIR}/src   # parallelreadtiff.h, helperfunctions.h
    ${cppzarr_SOURCE_DIR}/src   # zarr.h, parallelreadzarr.h
    ${hdf5_SOURCE_DIR}/src      # hdf5.h (parallel.cpp writes raw chunks)
    ${hdf5_BINARY_DIR}/src      # H5pubconf.h (generated)
    ${LZ4_INCLUDE_DIR}          # lz4.h (parallel.cpp compresses chunks, imsread.cpp decodes them)
    ${zlib_SOURCE_DIR}          # zlib.h (imsread.cpp: crc32 checksums)
    ${zlib_BINARY_DIR}          # zconf.h (generated)
)
set(PIW_LINK_LIBRARIES
    ImarisWriter_static
    cppTiff_shared
    cppZarr_shared
    nlohmann_json::nlohmann_json   # zarr.h includes <nlohmann/json.hpp>
    OpenMP::OpenMP_CXX
    Threads::Threads
    ${CMAKE_DL_LIBS}
)
target_include_directories(piw_core PRIVATE ${PIW_INCLUDE_DIRS})
target_compile_definitions(piw_core PRIVATE PIW_VERSION="${PROJECT_VERSION}")
target_link_libraries(piw_core PRIVATE ${PIW_LINK_LIBRARIES})   # usage requirements only (object library)
if(NOT WIN32)
    # The shared library (PIW_BUILD_PYTHON_LIB) then exports only the PIW_API functions of
    # parallelimariswriter.h (MinGW DLLs export everything). The executable works as before; it
    # only exports fewer symbols (the readers, linked -Bsymbolic, never bound to them).
    target_compile_options(piw_core PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)
endif()

add_executable(parallelimariswriter ${SRC_DIR}/main.cpp $<TARGET_OBJECTS:piw_core>)
target_include_directories(parallelimariswriter PRIVATE ${PIW_INCLUDE_DIRS})
target_link_libraries(parallelimariswriter PRIVATE ${PIW_LINK_LIBRARIES})

# Each program (and the Python package's library) links its own C++ runtime statically, so the
# only dynamic dependencies are the three libraries we ship in the folder (libcppTiff, libcppZarr,
# libgomp) plus the system's libraries; it takes no runtime symbol from the readers, which keep
# theirs private (piw_reader_exports). Exceptions the readers throw (std::string, std::exception)
# are caught across the two runtimes: the unwinder is one for the whole process and the catch
# compares type names. Linux: the unwinder is the system's libgcc_s, which the readers use too
# (measured: a caller with a static libgcc of its own aborted when it caught a reader's exception);
# macOS: it is libSystem's whatever -static-libgcc says (which keeps Homebrew's libgcc_s out);
# Windows: SEH unwinds through the system, each module with its own static libgcc, as the readers'
# DLLs have. Only valid for GNU; Apple Clang rejects these flags (build on macOS with Homebrew gcc).
# Used for the executable and every test program.
function(piw_link_like_executable target)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_link_options(${target} PRIVATE -static-libstdc++)
        if(APPLE OR WIN32)
            target_link_options(${target} PRIVATE -static-libgcc)
        else()
            # libstdc++.a refers to __cxa_pure_virtual weakly only: pulled in, it is the program's own
            # too, not left to whatever other library of the process defines one.
            target_link_options(${target} PRIVATE -Wl,--undefined=__cxa_pure_virtual)
        endif()
        if(NOT APPLE AND NOT WIN32)
            # Emit DT_RPATH (searched before LD_LIBRARY_PATH) instead of DT_RUNPATH, so the
            # bundled libgomp/libcppTiff/libcppZarr always take precedence -- matching the
            # $ORIGIN RPATH of the previous release. ELF-only, so not on macOS or Windows/MinGW.
            target_link_options(${target} PRIVATE -Wl,--disable-new-dtags)
        endif()
        if(WIN32)
            # Releases of libcppTiff.dll / libcppZarr.dll (built with -static-libgcc) export libgcc's
            # unwind symbols, which clashed with the program's own static libgcc (_Unwind_Resume).
            # Ours export none (piw_reader_exports); kept so that a stray duplicate cannot stop the
            # Windows build: what a program imports from the readers is checked by runtimeIsolation.
            target_link_options(${target} PRIVATE -Wl,--allow-multiple-definition)
        endif()
    endif()
endfunction()
piw_link_like_executable(parallelimariswriter)

# ---------------------------------------------------------------------------
# Release layout. Everything is linked statically EXCEPT the three libraries we
# ship next to the executable and load through the $ORIGIN / @loader_path RPATH:
#   * libcppTiff, libcppZarr  -- your reader libraries
#   * the OpenMP runtime (libgomp, for GCC)
# ---------------------------------------------------------------------------

# Locate the OpenMP runtime object (libgomp) to ship next to the executable, and the
# file name the loader asks for. Bundled only on Linux/macOS (ELF/Mach-O); on Windows
# the MinGW runtime DLLs (libgomp-1.dll, libwinpthread-1.dll) are resolved from the
# executable's directory / PATH, as in the previous Windows release.
set(BUNDLE_OPENMP FALSE)
if(NOT WIN32)
    if(OpenMP_gomp_LIBRARY)
        get_filename_component(OPENMP_RUNTIME "${OpenMP_gomp_LIBRARY}" REALPATH)
        if(APPLE)
            set(OPENMP_RUNTIME_SONAME "libgomp.1.dylib")
        else()
            set(OPENMP_RUNTIME_SONAME "libgomp.so.1")
        endif()
        set(BUNDLE_OPENMP TRUE)
    else()
        message(WARNING "OpenMP runtime (libgomp) not located -- it will not be bundled. "
                        "Building with GCC is expected.")
    endif()
endif()

set(RUNTIME_PERMS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

# `cmake --install <build> --prefix <dir>` produces a self-contained <dir>/bin.
install(TARGETS parallelimariswriter RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} PERMISSIONS ${RUNTIME_PERMS})
install(FILES $<TARGET_FILE:cppTiff_shared> $<TARGET_FILE:cppZarr_shared>
        DESTINATION ${CMAKE_INSTALL_BINDIR} PERMISSIONS ${RUNTIME_PERMS})
if(BUNDLE_OPENMP)
    install(FILES "${OPENMP_RUNTIME}" RENAME "${OPENMP_RUNTIME_SONAME}"
            DESTINATION ${CMAKE_INSTALL_BINDIR} PERMISSIONS ${RUNTIME_PERMS})
endif()

# Convenience: `cmake --build <build> --target release` assembles the exact flat
# folder you ship:  <build>/Parallel_Imaris_Writer/
#   parallelimariswriter  libcppTiff.so  libcppZarr.so  libgomp.so.1
set(RELEASE_DIR "${CMAKE_BINARY_DIR}/Parallel_Imaris_Writer")
set(RELEASE_CMDS
    COMMAND ${CMAKE_COMMAND} -E make_directory "${RELEASE_DIR}"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:parallelimariswriter> "${RELEASE_DIR}/"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:cppTiff_shared>       "${RELEASE_DIR}/"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:cppZarr_shared>       "${RELEASE_DIR}/")
if(BUNDLE_OPENMP)
    list(APPEND RELEASE_CMDS
        COMMAND ${CMAKE_COMMAND} -E copy_if_different "${OPENMP_RUNTIME}" "${RELEASE_DIR}/${OPENMP_RUNTIME_SONAME}")
    if(APPLE)
        # Homebrew's libgomp records an absolute install-name, so repoint the copied
        # binaries at the bundled copy (@loader_path) -- otherwise the release only runs
        # on Macs that have Homebrew GCC. (Standard install_name_tool fixup; verify on ARM.)
        list(APPEND RELEASE_CMDS
            COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/cmake/macos_bundle_openmp.sh" "${RELEASE_DIR}")
    endif()
endif()
add_custom_target(release ${RELEASE_CMDS}
    DEPENDS parallelimariswriter
    COMMENT "Assembling self-contained release in ${RELEASE_DIR}"
    VERBATIM)

# ---------------------------------------------------------------------------
# libparallelImarisWriter: the converter core as a shared library, for the Python package
# (python/setup.py configures with -DPIW_BUILD_PYTHON_LIB=ON). Its API is
# src/parallelimariswriter.h. Same objects as the executable; the default build is unchanged.
#     cmake --build <build> --target parallelImarisWriter_shared
#     cmake --install <build> --component python     (lib/: the three libraries; include/: the header)
# ---------------------------------------------------------------------------
option(PIW_BUILD_PYTHON_LIB "Build libparallelImarisWriter, the shared library of the Python package" OFF)
if(PIW_BUILD_PYTHON_LIB)
    add_library(parallelImarisWriter_shared SHARED $<TARGET_OBJECTS:piw_core>)
    set_target_properties(parallelImarisWriter_shared PROPERTIES OUTPUT_NAME parallelImarisWriter)
    target_link_libraries(parallelImarisWriter_shared PRIVATE ${PIW_LINK_LIBRARIES})
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # Its own static C++ runtime, as the executable's (piw_link_like_executable), kept private
        # like the readers': only the piw:: functions exported (the template instantiations of
        # namespace std keep default visibility despite -fvisibility=hidden).
        target_link_options(parallelImarisWriter_shared PRIVATE -static-libstdc++)
        if(APPLE OR WIN32)
            target_link_options(parallelImarisWriter_shared PRIVATE -static-libgcc)
        endif()
        if(APPLE)
            # (exported, the runtime's weak definitions would be shared with every other image of
            # the process that exports the same: macOS's loader merges them)
            set(PIW_EXPORTS_LIST "${CMAKE_CURRENT_BINARY_DIR}/parallelImarisWriter.exports")
            file(WRITE "${PIW_EXPORTS_LIST}" "__ZN3piw*\n__ZNK3piw*\n")
            target_link_options(parallelImarisWriter_shared PRIVATE "LINKER:-exported_symbols_list,${PIW_EXPORTS_LIST}")
            set_property(TARGET parallelImarisWriter_shared APPEND PROPERTY LINK_DEPENDS "${PIW_EXPORTS_LIST}")
        elseif(WIN32)
            # As the executable; no symbol of a static library exported (HDF5, LZ4, zlib, libgcc's
            # unwinder). And ImarisWriter's dllexport declarations would turn off MinGW's export of
            # everything, leaving the piw:: functions unexported.
            target_link_options(parallelImarisWriter_shared PRIVATE
                -Wl,--allow-multiple-definition -Wl,--exclude-libs,ALL -Wl,--export-all-symbols)
        else()
            # Static HDF5, LZ4, zlib, ImarisWriter and C++ runtime hidden (__cxa_pure_virtual included:
            # see piw_link_like_executable). The unwinder is the shared libgcc_s (never -static-libgcc
            # on Linux: see piw_link_like_executable).
            set(PIW_VERSION_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/parallelImarisWriter.map")
            file(WRITE "${PIW_VERSION_SCRIPT}" "{\n  global:\n    extern \"C++\" { piw::*; };\n  local: *;\n};\n")
            target_link_options(parallelImarisWriter_shared PRIVATE -Wl,--undefined=__cxa_pure_virtual
                -Wl,--exclude-libs,ALL "-Wl,--version-script=${PIW_VERSION_SCRIPT}")
            set_property(TARGET parallelImarisWriter_shared APPEND PROPERTY LINK_DEPENDS "${PIW_VERSION_SCRIPT}")
        endif()
    endif()
    install(TARGETS parallelImarisWriter_shared COMPONENT python
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
    if(WIN32)
        set(PIW_READERS_DESTINATION ${CMAKE_INSTALL_BINDIR})
    else()
        set(PIW_READERS_DESTINATION ${CMAKE_INSTALL_LIBDIR})
    endif()
    install(FILES $<TARGET_FILE:cppTiff_shared> $<TARGET_FILE:cppZarr_shared> COMPONENT python
            DESTINATION ${PIW_READERS_DESTINATION} PERMISSIONS ${RUNTIME_PERMS})
    install(FILES ${SRC_DIR}/parallelimariswriter.h COMPONENT python DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

# ---------------------------------------------------------------------------
# Tests (CTest). Each test program generates its inputs on the fly (no committed
# test data), runs the shipped executable on them and checks the .ims it writes with
# tests/imsutil.h (header-only .ims decoder and comparison; HDF5 + lz4). Enable with
# -DPIW_BUILD_TESTS=ON:
#     cmake --build <build>
#     ctest --test-dir <build> --output-on-failure
# A project-specific option name avoids clashing with dependencies' BUILD_TESTS.
# ---------------------------------------------------------------------------
option(PIW_BUILD_TESTS "Build and register the round-trip, block- and parallel-equivalence tests" OFF)
if(PIW_BUILD_TESTS)
    enable_testing()

    # The test binary and the executable it spawns both need the shared libs
    # co-located ($ORIGIN / @loader_path / same-dir), which the build-tree binaries
    # can't satisfy (libs live under _deps/). The setup fixture assembles the
    # pristine release folder, then mirrors it -- plus the test binaries -- into a
    # separate staging dir, so the tests run self-contained WITHOUT leaking a test
    # binary into the shipped release.
    set(ROUNDTRIP_STAGE "${CMAKE_BINARY_DIR}/roundtrip_stage")
    add_custom_target(piw_tests)   # holds the list of test binaries to stage (PIW_TEST_FILES)

    # piw_add_test(<name> <source> [LIBS <lib>...])
    # Builds <source> into <name>Test and registers the CTest test <name>, run from the
    # staging dir as `<name>Test <staged parallelimariswriter> <build>/<name>_work`.
    # Every test gets HDF5, lz4 (tests/imsutil.h), nlohmann_json and OpenMP, and the
    # cpp-tiff and cpp-zarr headers; LIBS adds the reader libraries it needs. Linked like the
    # executable (its own static C++ runtime).
    function(piw_add_test name source)
        cmake_parse_arguments(PIW_T "" "" "LIBS" ${ARGN})
        set(tgt ${name}Test)
        add_executable(${tgt} ${source})
        set_target_properties(${tgt} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
        target_compile_definitions(${tgt} PRIVATE PIW_VERSION="${PROJECT_VERSION}")   # the expected ImarisDataSet Version
        # cpp-tiff before cpp-zarr: both ship a helperfunctions.h (see the executable).
        target_include_directories(${tgt} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${cpptiff_SOURCE_DIR}/src ${cppzarr_SOURCE_DIR}/src
            ${hdf5_SOURCE_DIR}/src ${hdf5_BINARY_DIR}/src
            ${LZ4_INCLUDE_DIR})
        target_link_libraries(${tgt} PRIVATE
            ${PIW_T_LIBS} nlohmann_json::nlohmann_json OpenMP::OpenMP_CXX
            hdf5-static lz4_static Threads::Threads ${CMAKE_DL_LIBS})
        piw_link_like_executable(${tgt})
        add_dependencies(release ${tgt})
        set_property(TARGET piw_tests APPEND PROPERTY PIW_TEST_FILES "$<TARGET_FILE:${tgt}>")
        add_test(NAME ${name}
            COMMAND "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:${tgt}>"
                    "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:parallelimariswriter>"
                    "${CMAKE_CURRENT_BINARY_DIR}/${name}_work")
        set_tests_properties(${name} PROPERTIES FIXTURES_REQUIRED piw_release)
    endfunction()

    # Stages the release folder and every test binary added with piw_add_test (the
    # list is read at generate time, so tests may be added below this line too).
    add_test(NAME roundtrip_setup COMMAND ${CMAKE_COMMAND}
        -DRELEASE_DIR=${RELEASE_DIR} -DSTAGE_DIR=${ROUNDTRIP_STAGE}
        "-DTEST_EXES=$<JOIN:$<TARGET_GENEX_EVAL:piw_tests,$<TARGET_PROPERTY:piw_tests,PIW_TEST_FILES>>,|>"
        -DBUILD_DIR=${CMAKE_BINARY_DIR} -DCFG=$<CONFIG>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/stage_roundtrip.cmake)
    set_tests_properties(roundtrip_setup PROPERTIES FIXTURES_SETUP piw_release)

    # Voxels out == voxels in (TIFF/Zarr x uint8/uint16/float32), level 1 == box
    # average of level 0, extents, histogram, 2 channels x 3 timepoints, imsCompare self-test.
    piw_add_test(roundtrip tests/roundtrip.cpp LIBS cppTiff_shared cppZarr_shared)
    # -C block processing yields the same .ims as a whole-timepoint read.
    piw_add_test(blockEquivalence tests/blocktest.cpp LIBS cppTiff_shared cppZarr_shared)
    # The cluster subcommands assemble a single .ims with the same data as the
    # monolithic conversion, through both stores of the tile workers (packs and the
    # single file). Zarr-only (the parallel path reads Zarr).
    piw_add_test(parallelEquivalence tests/paralleltest.cpp LIBS cppZarr_shared)
    # piw::convert (the core objects, linked like the executable) writes the same .ims as
    # the executable, and fails cleanly: error returns, cancel, progress, concurrent calls.
    piw_add_test(apiEquivalence tests/apitest.cpp LIBS ${PIW_LINK_LIBRARIES})
    target_sources(apiEquivalenceTest PRIVATE $<TARGET_OBJECTS:piw_core>)
    target_include_directories(apiEquivalenceTest PRIVATE ${SRC_DIR} ${PIW_INCLUDE_DIRS})

    # imscompare: compares two .ims files chunk by chunk (--all: also the metadata),
    # --map (t,c) pairs between files, or checks one (--deep). A test and validation
    # tool (HDF5 + lz4 only): staged next to the test binaries, never released.
    add_executable(imscompare tests/imscompare.cpp)
    set_target_properties(imscompare PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(imscompare PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tests ${hdf5_SOURCE_DIR}/src ${hdf5_BINARY_DIR}/src ${LZ4_INCLUDE_DIR})
    target_link_libraries(imscompare PRIVATE hdf5-static lz4_static Threads::Threads ${CMAKE_DL_LIBS})
    piw_link_like_executable(imscompare)
    add_dependencies(release imscompare)
    set_property(TARGET piw_tests APPEND PROPERTY PIW_TEST_FILES "$<TARGET_FILE:imscompare>")
    # The tool's exit codes and messages on synthetic files with known differences.
    piw_add_test(imscompare tests/imscomparetest.cpp LIBS cppZarr_shared)
    add_dependencies(imscompareTest imscompare)

    # Mono-core test: placement VFD, claim allocator and sidecars of the single-file
    # cluster output, on synthetic data (HDF5 + lz4 only, no executable to stage).
    # Reports "skipped" on Windows, where the single-file path is not supported.
    add_executable(monoCoreTest ${CMAKE_CURRENT_SOURCE_DIR}/tests/monocoretest.cpp ${MONO_SRCS})
    set_target_properties(monoCoreTest PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(monoCoreTest PRIVATE
        ${SRC_DIR} ${hdf5_SOURCE_DIR}/src ${hdf5_BINARY_DIR}/src ${LZ4_INCLUDE_DIR})
    target_link_libraries(monoCoreTest PRIVATE hdf5-static lz4_static Threads::Threads ${CMAKE_DL_LIBS})
    piw_link_like_executable(monoCoreTest)
    add_test(NAME monoCore COMMAND monoCoreTest "${CMAKE_CURRENT_BINARY_DIR}/monocore_work")
    set_tests_properties(monoCore PROPERTIES SKIP_RETURN_CODE 77)

    # Single-file cluster output of the time-series path (setup/monoinit, monoplace, monostatus,
    # monofinish): equal to a single-node conversion of the same volumes, /Thumbnail included, in the
    # same storage order; restart and failure cases. Reports "skipped" on Windows.
    piw_add_test(mono tests/monotest.cpp LIBS cppZarr_shared)
    set_tests_properties(mono PROPERTIES SKIP_RETURN_CODE 77)

    # The converter's command line: options, values, input discovery and exit codes (every error
    # exits 1 with one ERROR line, never 0 or a signal); the POSIX-only cases are skipped on Windows.
    piw_add_test(clitest tests/clitest.cpp LIBS cppTiff_shared cppZarr_shared)
    set_tests_properties(clitest PROPERTIES TIMEOUT 600)

    # An output that cannot be written (file-size limit, a directory in the way) ends the
    # conversion with exit code 1 and an error, not a hang. POSIX only (setrlimit, fork).
    # Each run is killed after 30 s and counted as a hang; the timeout only backs that up.
    if(NOT WIN32)
        piw_add_test(failtest tests/failtest.cpp LIBS cppZarr_shared)
        set_tests_properties(failtest PROPERTIES TIMEOUT 300)
    endif()

    # The single-file finisher of a tile workdir (setup's mono store) from generated sidecars, without
    # image data: a small layout here; run by hand with a 100 TB-class layout for the scale check
    # (tests/monoscaletest.cpp). Links the core like apiEquivalence. Reports "skipped" on Windows.
    piw_add_test(monoScale tests/monoscaletest.cpp LIBS ${PIW_LINK_LIBRARIES})
    target_sources(monoScaleTest PRIVATE $<TARGET_OBJECTS:piw_core>)
    target_include_directories(monoScaleTest PRIVATE ${SRC_DIR} ${PIW_INCLUDE_DIRS})
    set_tests_properties(monoScale PROPERTIES SKIP_RETURN_CODE 77)

    # Every program and library above brings its own C++ runtime, LZ4 and zlib, none of them bound
    # to libcppTiff's or libcppZarr's, and the two export none of theirs (nm -D on Linux, nm -m on
    # macOS, objdump -p's import and export tables on Windows; tests/runtime_isolation.cmake).
    if(CMAKE_NM AND CMAKE_OBJDUMP)
        set(PIW_CHECKED "$<TARGET_FILE:parallelimariswriter>|$<TARGET_FILE:monoCoreTest>")
        string(APPEND PIW_CHECKED "|$<JOIN:$<TARGET_GENEX_EVAL:piw_tests,$<TARGET_PROPERTY:piw_tests,PIW_TEST_FILES>>,|>")
        if(PIW_BUILD_PYTHON_LIB)
            string(APPEND PIW_CHECKED "|$<TARGET_FILE:parallelImarisWriter_shared>")
        endif()
        add_test(NAME runtimeIsolation COMMAND ${CMAKE_COMMAND}
            -DPLATFORM=${CMAKE_SYSTEM_NAME} -DNM=${CMAKE_NM} -DOBJDUMP=${CMAKE_OBJDUMP}
            "-DREADERS=$<TARGET_FILE:cppTiff_shared>|$<TARGET_FILE:cppZarr_shared>" "-DBINARIES=${PIW_CHECKED}"
            -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/runtime_isolation.cmake)
    else()
        message(WARNING "nm or objdump not found: the runtimeIsolation test is not registered")
    endif()
endif()
