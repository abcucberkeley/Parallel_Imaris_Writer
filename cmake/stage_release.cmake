# Assembles the release folder (the `release` target of CMakeLists.txt): one flat folder holding the
# executable, libcppTiff and libcppZarr and (Linux, macOS) the OpenMP runtime, made from fresh copies of the
# build tree's files:
#   * RPATH: exactly $ORIGIN (Linux) or @loader_path (macOS) in the executable and the two libraries, none
#     in the OpenMP runtime (a compiler's own lib folder, which GCC installations can add to everything they
#     link, and the runtime's build folders are removed);
#   * stripped (STRIP given): no static symbol table and no debug information; the dynamic symbols (the
#     libraries' exports, every binary's imports) stay;
#   * macOS: the binaries load the bundled libgomp (macos_bundle_openmp.sh) and are signed again (ad hoc).
# The folder is emptied first, so it never holds a file of an earlier build.
#
#   cmake -DPLATFORM=<Linux|Darwin|Windows> -DRELEASE_DIR=<dir> -DEXECUTABLE=<file>
#         -DLIBRARIES=<file>|<file> [-DOPENMP_RUNTIME=<file> -DOPENMP_RUNTIME_SONAME=<name>]
#         [-DSTRIP=<strip>] [-DOBJDUMP=<objdump>] [-DMACOS_BUNDLE_OPENMP=<script>] [-DKEEP_RPATH=ON]
#         -P stage_release.cmake
# KEEP_RPATH (sanitizer builds): the RPATHs stay as linked. Lists use "|" as separators.

cmake_minimum_required(VERSION 3.18)

foreach(v PLATFORM RELEASE_DIR EXECUTABLE LIBRARIES)
    if(NOT DEFINED ${v} OR "${${v}}" STREQUAL "")
        message(FATAL_ERROR "stage_release.cmake: -D${v}=... is missing")
    endif()
endforeach()
string(REPLACE "|" ";" LIBRARIES "${LIBRARIES}")

function(run)
    execute_process(COMMAND ${ARGN} RESULT_VARIABLE rc OUTPUT_VARIABLE out ERROR_VARIABLE err)
    if(NOT rc EQUAL 0)
        string(REPLACE ";" " " cmd "${ARGN}")
        message(FATAL_ERROR "stage_release: failed (${rc}): ${cmd}\n${out}${err}")
    endif()
endfunction()

file(REMOVE_RECURSE "${RELEASE_DIR}")
file(MAKE_DIRECTORY "${RELEASE_DIR}")

# Copies, with their roles: "exe", "lib" (ours) or "runtime" (the OpenMP runtime)
set(staged "")
foreach(f "${EXECUTABLE}" ${LIBRARIES})
    get_filename_component(name "${f}" NAME)
    run("${CMAKE_COMMAND}" -E copy "${f}" "${RELEASE_DIR}/${name}")
    if(f STREQUAL EXECUTABLE)
        list(APPEND staged "exe=${RELEASE_DIR}/${name}")
    else()
        list(APPEND staged "lib=${RELEASE_DIR}/${name}")
    endif()
endforeach()
if(DEFINED OPENMP_RUNTIME AND NOT OPENMP_RUNTIME STREQUAL "")
    run("${CMAKE_COMMAND}" -E copy "${OPENMP_RUNTIME}" "${RELEASE_DIR}/${OPENMP_RUNTIME_SONAME}")
    list(APPEND staged "runtime=${RELEASE_DIR}/${OPENMP_RUNTIME_SONAME}")
endif()
if(NOT PLATFORM STREQUAL "Windows")
    # (a compiler's runtime can be installed read-only, as Homebrew's libgomp is)
    foreach(entry IN LISTS staged)
        string(REGEX REPLACE "^[a-z]+=" "" file "${entry}")
        run(chmod 755 "${file}")
    endforeach()
endif()

# Strip. GNU strip (Linux, MinGW): --strip-all for the executable, --strip-unneeded for libraries (both
# keep .dynsym and, on Windows, the import and export tables). macOS strip: all but the symbols the dynamic
# loader needs for the executable, the local symbols for libraries (-x; their exports stay).
if(DEFINED STRIP AND NOT STRIP STREQUAL "")
    foreach(entry IN LISTS staged)
        string(REGEX REPLACE "^([a-z]+)=(.*)$" "\\1;\\2" entry "${entry}")
        list(GET entry 0 role)
        list(GET entry 1 file)
        if(PLATFORM STREQUAL "Darwin")
            if(role STREQUAL "exe")
                run("${STRIP}" "${file}")
            else()
                run("${STRIP}" -x "${file}")
            endif()
        elseif(role STREQUAL "exe")
            run("${STRIP}" --strip-all "${file}")
        else()
            run("${STRIP}" --strip-unneeded "${file}")
        endif()
    endforeach()
else()
    message(STATUS "stage_release: the release binaries are not stripped")
endif()

# macOS: the bundled libgomp
if(PLATFORM STREQUAL "Darwin" AND DEFINED MACOS_BUNDLE_OPENMP AND NOT MACOS_BUNDLE_OPENMP STREQUAL "")
    run(bash "${MACOS_BUNDLE_OPENMP}" "${RELEASE_DIR}")
endif()

# RPATH
if(KEEP_RPATH)
    message(STATUS "stage_release: the RPATHs stay as linked")
elseif(PLATFORM STREQUAL "Linux")
    if(NOT DEFINED OBJDUMP OR OBJDUMP STREQUAL "")
        message(FATAL_ERROR "stage_release: objdump is needed to set the RPATH of the release binaries")
    endif()
    foreach(entry IN LISTS staged)
        string(REGEX REPLACE "^([a-z]+)=(.*)$" "\\1;\\2" entry "${entry}")
        list(GET entry 0 role)
        list(GET entry 1 file)
        execute_process(COMMAND "${OBJDUMP}" -p "${file}" OUTPUT_VARIABLE dyn RESULT_VARIABLE rc)
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "stage_release: ${OBJDUMP} -p ${file} failed")
        endif()
        set(current "")
        if(dyn MATCHES "\n *(RPATH|RUNPATH) +([^\n]*)")
            string(STRIP "${CMAKE_MATCH_2}" current)
        endif()
        # (CMake's own ELF editor, as at install time: the new RPATH replaces the old one in place)
        if(role STREQUAL "runtime")
            if(NOT current STREQUAL "")
                file(RPATH_REMOVE FILE "${file}")
            endif()
        elseif(current STREQUAL "")
            message(FATAL_ERROR "stage_release: ${file} has no RPATH (it is linked with $ORIGIN)")
        elseif(NOT current STREQUAL "$ORIGIN")
            file(RPATH_CHANGE FILE "${file}" OLD_RPATH "${current}" NEW_RPATH "$ORIGIN")
        endif()
    endforeach()
elseif(PLATFORM STREQUAL "Darwin")
    foreach(entry IN LISTS staged)
        string(REGEX REPLACE "^([a-z]+)=(.*)$" "\\1;\\2" entry "${entry}")
        list(GET entry 0 role)
        list(GET entry 1 file)
        execute_process(COMMAND otool -l "${file}" OUTPUT_VARIABLE cmds RESULT_VARIABLE rc)
        if(NOT rc EQUAL 0)
            message(FATAL_ERROR "stage_release: otool -l ${file} failed")
        endif()
        # "cmd LC_RPATH" is followed by "cmdsize <n>" and "path <path> (offset <n>)"
        string(REGEX MATCHALL "cmd LC_RPATH\n[^\n]*\n *path [^\n]*" rpaths "${cmds}")
        set(found FALSE)
        foreach(r IN LISTS rpaths)
            string(REGEX REPLACE ".*\n *path (.*) \\(offset [0-9]+\\)$" "\\1" p "${r}")
            if(p STREQUAL "@loader_path" AND NOT role STREQUAL "runtime" AND NOT found)
                set(found TRUE)
            else()   # (another path, or @loader_path a second time; the first one that matches is removed)
                run(install_name_tool -delete_rpath "${p}" "${file}")
            endif()
        endforeach()
        if(NOT found AND NOT role STREQUAL "runtime")
            run(install_name_tool -add_rpath "@loader_path" "${file}")
        endif()
    endforeach()
endif()
if(PLATFORM STREQUAL "Darwin")
    # strip and install_name_tool invalidate the signature, and arm64 loads no binary without a valid one
    foreach(entry IN LISTS staged)
        string(REGEX REPLACE "^[a-z]+=" "" file "${entry}")
        run(codesign --force --sign - "${file}")
    endforeach()
endif()

# (the executable is the custom command's output: newer than everything it was made from)
list(GET staged 0 exe)
string(REGEX REPLACE "^[a-z]+=" "" exe "${exe}")
file(TOUCH "${exe}")
