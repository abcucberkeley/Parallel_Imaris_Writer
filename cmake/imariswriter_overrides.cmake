# ImarisWriter with this repository's modified files (third_party/imariswriter, see its README.md).
#
#   piw_imariswriter_tree(<fetched source dir> <tree var> <sources var>)
#
# Copies the fetched ImarisWriter sources (c/, interface/, interfaceC/, writer/) into
# <build>/imariswriter-patched/ with every file of third_party/imariswriter/overrides/ in place of
# the upstream file of the same relative path, and returns that directory in <tree var> and its
# .cxx files (c/ and writer/) in <sources var>. Compiling from such a copy, rather than putting the
# overrides first on the include path, is what replaces a header for every upstream file too: a
# quoted #include looks in the including file's own directory first.
#
# third_party/imariswriter/upstream.sha256 records the SHA-256 of each overridden upstream file at
# the pinned commit. Configuring stops when a fetched file differs (the pin was moved: the changes
# must be carried over to the new upstream file first) and when an override has no recorded hash.
# Line endings are normalized to LF before hashing, so a checkout with CRLF endings matches too.
# The copies are made with configure_file, so editing an override re-runs CMake and rebuilds.

set(PIW_IW_THIRD_PARTY "${CMAKE_CURRENT_LIST_DIR}/../third_party/imariswriter")

function(piw_file_sha256_lf path out_var)
    file(READ "${path}" content)
    string(REPLACE "\r\n" "\n" content "${content}")
    string(SHA256 hash "${content}")
    set(${out_var} "${hash}" PARENT_SCOPE)
endfunction()

function(piw_imariswriter_tree upstream_dir tree_var sources_var)
    get_filename_component(third_party "${PIW_IW_THIRD_PARTY}" ABSOLUTE)
    set(overrides_dir "${third_party}/overrides")
    set(manifest "${third_party}/upstream.sha256")
    set(tree "${CMAKE_BINARY_DIR}/imariswriter-patched")
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${manifest}")

    # Check every recorded upstream file, and that each one has its override.
    file(STRINGS "${manifest}" lines)
    set(overridden "")
    foreach(line IN LISTS lines)
        string(STRIP "${line}" line)   # also a CR left by a CRLF checkout
        if(line MATCHES "^(#.*)?$")
            continue()
        endif()
        if(NOT line MATCHES "^([0-9a-f]+) [ *]?(.+)$")
            message(FATAL_ERROR "ImarisWriter overrides: cannot parse this line of ${manifest}:\n  ${line}")
        endif()
        set(expected "${CMAKE_MATCH_1}")
        set(rel "${CMAKE_MATCH_2}")
        if(NOT EXISTS "${upstream_dir}/${rel}")
            message(FATAL_ERROR "ImarisWriter overrides: the fetched ImarisWriter has no ${rel}, which "
                                "third_party/imariswriter/overrides/${rel} replaces.")
        endif()
        piw_file_sha256_lf("${upstream_dir}/${rel}" actual)
        if(NOT actual STREQUAL expected)
            message(FATAL_ERROR
                "ImarisWriter overrides: the fetched ${rel} is not the upstream file that "
                "third_party/imariswriter/overrides/${rel} was made from (SHA-256 ${actual}, "
                "expected ${expected}). The ImarisWriter pin in CMakeLists.txt has moved: carry the "
                "changes listed in third_party/imariswriter/README.md over to the new upstream file, then "
                "record its SHA-256 in third_party/imariswriter/upstream.sha256.")
        endif()
        if(NOT EXISTS "${overrides_dir}/${rel}")
            message(FATAL_ERROR "ImarisWriter overrides: ${manifest} lists ${rel}, but "
                                "third_party/imariswriter/overrides/${rel} does not exist.")
        endif()
        list(APPEND overridden "${rel}")
    endforeach()
    file(GLOB_RECURSE override_files LIST_DIRECTORIES false RELATIVE "${overrides_dir}" "${overrides_dir}/*")
    foreach(rel IN LISTS override_files)
        if(NOT rel IN_LIST overridden)
            message(FATAL_ERROR "ImarisWriter overrides: third_party/imariswriter/overrides/${rel} has no "
                                "SHA-256 of its upstream file in ${manifest}.")
        endif()
    endforeach()

    # The copy: upstream files, with the overrides in their place.
    file(GLOB_RECURSE upstream_files LIST_DIRECTORIES false RELATIVE "${upstream_dir}"
         "${upstream_dir}/c/*" "${upstream_dir}/interface/*" "${upstream_dir}/interfaceC/*"
         "${upstream_dir}/writer/*")
    set(sources "")
    foreach(rel IN LISTS upstream_files)
        if(rel IN_LIST overridden)
            configure_file("${overrides_dir}/${rel}" "${tree}/${rel}" COPYONLY)
        else()
            configure_file("${upstream_dir}/${rel}" "${tree}/${rel}" COPYONLY)
        endif()
        if(rel MATCHES "^(c|writer)/[^/]*\\.cxx$")
            list(APPEND sources "${tree}/${rel}")
        endif()
    endforeach()
    list(LENGTH overridden count)
    message(STATUS "ImarisWriter: compiling ${tree} (${count} files from third_party/imariswriter/overrides)")

    set(${tree_var} "${tree}" PARENT_SCOPE)
    set(${sources_var} "${sources}" PARENT_SCOPE)
endfunction()
