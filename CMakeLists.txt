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
FetchContent_Declare(hdf5
    URL      https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.6/hdf5-1.14.6.tar.gz
    URL_HASH SHA256=e4defbac30f50d64e1556374aa49e574417c9e72c6b1de7a4ff88c4b1bea6e9b
)
FetchContent_MakeAvailable(hdf5)

# ---------------------------------------------------------------------------
# cpp-tiff and cpp-zarr -- your reader libraries, built as libcppTiff/libcppZarr.
# Pinned to release tags for reproducibility; bump the GIT_TAGs to pick up new
# versions. cpp-tiff v1.5.1 carries the Z-range readTiffParallelWrapper overload
# the -C block feature needs.
# ---------------------------------------------------------------------------
# cpp-zarr's bundled c-blosc2 otherwise builds its own zlib-ng, whose "zlib"/
# "zlibstatic" targets collide with the zlib we (and cpp-tiff) fetched above.
# Point blosc2 at the shared zlib via find_package(ZLIB) instead.
set(PREFER_EXTERNAL_ZLIB ON CACHE BOOL "" FORCE)
FetchContent_Declare(cpptiff
    GIT_REPOSITORY https://github.com/abcucberkeley/cpp-tiff.git
    GIT_TAG        v1.5.1
)
FetchContent_Declare(cppzarr
    GIT_REPOSITORY https://github.com/abcucberkeley/cpp-zarr.git
    GIT_TAG        v1.5.1
)
FetchContent_MakeAvailable(cpptiff cppzarr)

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

set(IW_DIR "${imariswriter_SOURCE_DIR}")
file(GLOB IW_SRCS "${IW_DIR}/c/*.cxx" "${IW_DIR}/writer/*.cxx")

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
# parallelimariswriter executable
# ---------------------------------------------------------------------------
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
# parallel.cpp adds the cluster subcommands (setup/worker/combine/monitor); it reads
# zarr chunks and writes raw HDF5 chunks directly, so it needs the HDF5 + lz4 headers.
add_executable(parallelimariswriter ${SRC_DIR}/main.cpp ${SRC_DIR}/parallel.cpp)

# cpp-tiff's src dir is listed before cpp-zarr's on purpose: both ship a
# helperfunctions.h and main.cpp includes it unqualified -- cpp-tiff's must win,
# matching the original gccImaris.txt include order.
target_include_directories(parallelimariswriter PRIVATE
    ${IW_DIR}/interfaceC        # bpImageConverterInterfaceC.h + bpConverterTypesC.h
    ${IW_DIR}/writer            # bpDeriche.h (parallel.cpp reuses the Gaussian for channel ColorRange)
    ${cpptiff_SOURCE_DIR}/src   # parallelreadtiff.h, helperfunctions.h
    ${cppzarr_SOURCE_DIR}/src   # zarr.h, parallelreadzarr.h
    ${hdf5_SOURCE_DIR}/src      # hdf5.h (parallel.cpp writes raw chunks)
    ${hdf5_BINARY_DIR}/src      # H5pubconf.h (generated)
    ${LZ4_INCLUDE_DIR}          # lz4.h (parallel.cpp compresses chunks)
)

target_link_libraries(parallelimariswriter PRIVATE
    ImarisWriter_static
    cppTiff_shared
    cppZarr_shared
    nlohmann_json::nlohmann_json   # zarr.h includes <nlohmann/json.hpp>
    OpenMP::OpenMP_CXX
    Threads::Threads
    ${CMAKE_DL_LIBS}
)

# Statically pull in libstdc++/libgcc so the ONLY dynamic dependencies are the
# three libraries we ship in the folder (libcppTiff, libcppZarr, libgomp) plus the
# system C libraries. Only valid for GNU; Apple Clang rejects these flags (build on
# macOS with Homebrew gcc, as before).
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_options(parallelimariswriter PRIVATE -static-libstdc++ -static-libgcc)
    if(NOT APPLE AND NOT WIN32)
        # Emit DT_RPATH (searched before LD_LIBRARY_PATH) instead of DT_RUNPATH, so the
        # bundled libgomp/libcppTiff/libcppZarr always take precedence -- matching the
        # $ORIGIN RPATH of the previous release. ELF-only, so not on macOS or Windows/MinGW.
        target_link_options(parallelimariswriter PRIVATE -Wl,--disable-new-dtags)
    endif()
    if(WIN32)
        # libcppTiff.dll / libcppZarr.dll (built with -static-libgcc) re-export libgcc's
        # unwind symbols, which then clash with this exe's own static libgcc (e.g.
        # _Unwind_Resume). Same fix cpp-zarr uses for MinGW.
        target_link_options(parallelimariswriter PRIVATE -Wl,--allow-multiple-definition)
    endif()
endif()

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
# Round-trip smoke test (CTest): a per-platform "does the shipped binary actually
# run" check. tests/roundtrip.cpp generates a tiny TIFF and a tiny zstd/F-order
# Zarr on the fly (no committed test data), runs the executable on each, and
# verifies a valid Imaris/HDF5 file comes out. Enable with -DPIW_BUILD_TESTS=ON:
#     cmake --build <build>
#     ctest --test-dir <build> --output-on-failure
# A project-specific option name avoids clashing with dependencies' BUILD_TESTS.
# ---------------------------------------------------------------------------
option(PIW_BUILD_TESTS "Build and register the round-trip smoke + block-equivalence tests" OFF)
if(PIW_BUILD_TESTS)
    enable_testing()

    # Links the reader libraries to generate the inputs on the fly.
    add_executable(roundtripTest ${CMAKE_CURRENT_SOURCE_DIR}/tests/roundtrip.cpp)
    set_target_properties(roundtripTest PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(roundtripTest PRIVATE
        ${cpptiff_SOURCE_DIR}/src ${cppzarr_SOURCE_DIR}/src)
    target_link_libraries(roundtripTest PRIVATE
        cppTiff_shared cppZarr_shared nlohmann_json::nlohmann_json OpenMP::OpenMP_CXX)
    add_dependencies(release roundtripTest)

    # Block-equivalence test: additionally links (static) HDF5 so it can compare the
    # raw compressed chunks of the -C output against the whole-timepoint output
    # (the .ims voxel data must be identical). Portable -- no h5py / HDF5 tools.
    add_executable(blockTest ${CMAKE_CURRENT_SOURCE_DIR}/tests/blocktest.cpp)
    set_target_properties(blockTest PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(blockTest PRIVATE
        ${cpptiff_SOURCE_DIR}/src ${cppzarr_SOURCE_DIR}/src
        ${hdf5_SOURCE_DIR}/src ${hdf5_BINARY_DIR}/src)
    target_link_libraries(blockTest PRIVATE
        cppTiff_shared cppZarr_shared nlohmann_json::nlohmann_json OpenMP::OpenMP_CXX
        hdf5-static Threads::Threads ${CMAKE_DL_LIBS})
    add_dependencies(release blockTest)

    # Parallel-equivalence test: the same static-HDF5 raw-chunk comparison, but it
    # drives the cluster subcommands (setup/worker/combine) as independent processes
    # and checks the assembled .ims has byte-identical voxel data to the monolithic
    # conversion. Zarr-only (the parallel path reads Zarr), so no cpp-tiff link.
    add_executable(parallelTest ${CMAKE_CURRENT_SOURCE_DIR}/tests/paralleltest.cpp)
    set_target_properties(parallelTest PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
    target_include_directories(parallelTest PRIVATE
        ${cppzarr_SOURCE_DIR}/src
        ${hdf5_SOURCE_DIR}/src ${hdf5_BINARY_DIR}/src)
    target_link_libraries(parallelTest PRIVATE
        cppZarr_shared nlohmann_json::nlohmann_json OpenMP::OpenMP_CXX
        hdf5-static Threads::Threads ${CMAKE_DL_LIBS})
    add_dependencies(release parallelTest)

    # The test binary and the executable it spawns both need the shared libs
    # co-located ($ORIGIN / @loader_path / same-dir), which the build-tree binaries
    # can't satisfy (libs live under _deps/). The setup fixture assembles the
    # pristine release folder, then mirrors it -- plus the test binary -- into a
    # separate staging dir, so the test runs self-contained WITHOUT leaking the test
    # binary into the shipped release.
    set(ROUNDTRIP_STAGE "${CMAKE_BINARY_DIR}/roundtrip_stage")
    add_test(NAME roundtrip_setup COMMAND ${CMAKE_COMMAND}
        -DRELEASE_DIR=${RELEASE_DIR} -DSTAGE_DIR=${ROUNDTRIP_STAGE}
        -DTEST_EXE=$<TARGET_FILE:roundtripTest>
        -DTEST_EXE_2=$<TARGET_FILE:blockTest>
        -DTEST_EXE_3=$<TARGET_FILE:parallelTest>
        -DBUILD_DIR=${CMAKE_BINARY_DIR} -DCFG=$<CONFIG>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/tests/stage_roundtrip.cmake)
    set_tests_properties(roundtrip_setup PROPERTIES FIXTURES_SETUP piw_release)

    add_test(NAME roundtrip
        COMMAND "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:roundtripTest>"
                "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:parallelimariswriter>"
                "${CMAKE_CURRENT_BINARY_DIR}/roundtrip_work")
    set_tests_properties(roundtrip PROPERTIES FIXTURES_REQUIRED piw_release)

    # Verifies -C block processing yields byte-identical voxel data to a whole read.
    add_test(NAME blockEquivalence
        COMMAND "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:blockTest>"
                "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:parallelimariswriter>"
                "${CMAKE_CURRENT_BINARY_DIR}/blocktest_work")
    set_tests_properties(blockEquivalence PROPERTIES FIXTURES_REQUIRED piw_release)

    # Verifies the cluster subcommands assemble a single .ims with voxel data
    # byte-identical to the monolithic conversion.
    add_test(NAME parallelEquivalence
        COMMAND "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:parallelTest>"
                "${ROUNDTRIP_STAGE}/$<TARGET_FILE_NAME:parallelimariswriter>"
                "${CMAKE_CURRENT_BINARY_DIR}/paralleltest_work")
    set_tests_properties(parallelEquivalence PROPERTIES FIXTURES_REQUIRED piw_release)
endif()
