# Assemble a self-contained folder to run the round-trip smoke test from.
#
# The test binary and the executable it spawns both need their shared libraries
# co-located (the build-tree binaries resolve libs via $ORIGIN / @loader_path and
# can't reach _deps/). So: build the pristine release folder, then mirror it --
# plus the test binary -- into a separate staging dir. This keeps the shipped
# release folder to exactly its runtime files (nothing test-related leaks in).
#
# Required -D args: RELEASE_DIR, STAGE_DIR, TEST_EXE, BUILD_DIR, CFG

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
file(COPY "${TEST_EXE}"     DESTINATION "${STAGE_DIR}")   # the smoke-test binary
