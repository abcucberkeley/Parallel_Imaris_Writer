# Assemble a self-contained folder to run the tests from.
#
# The test binaries and the executable they spawn all need their shared libraries
# co-located (the build-tree binaries resolve libs via $ORIGIN / @loader_path and
# can't reach _deps/). So: build the pristine release folder, then mirror it --
# plus the test binaries -- into a separate staging dir. This keeps the shipped
# release folder to exactly its runtime files (nothing test-related leaks in).
#
# Required -D args: RELEASE_DIR, STAGE_DIR, BUILD_DIR, CFG, and TEST_EXES (the test
# binaries, separated by "|" because a ";" would split the ctest argument).

if(NOT TEST_EXES)
    message(FATAL_ERROR "no test binaries given (TEST_EXES is empty)")
endif()

execute_process(
    COMMAND ${CMAKE_COMMAND} --build "${BUILD_DIR}" --config "${CFG}" --target release
    RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "could not assemble the release folder (exit ${rc})")
endif()

file(REMOVE_RECURSE "${STAGE_DIR}")
file(MAKE_DIRECTORY "${STAGE_DIR}")
file(GLOB _release_files "${RELEASE_DIR}/*")
file(COPY ${_release_files} DESTINATION "${STAGE_DIR}")   # executable + shared libs
string(REPLACE "|" ";" _test_exes "${TEST_EXES}")
foreach(_exe IN LISTS _test_exes)
    if(NOT _exe STREQUAL "")
        file(COPY "${_exe}" DESTINATION "${STAGE_DIR}")   # one test binary
    endif()
endforeach()
