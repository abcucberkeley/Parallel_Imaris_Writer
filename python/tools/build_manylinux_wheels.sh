#!/usr/bin/env bash
# Builds, repairs and tests Linux wheels inside a manylinux container (docker or apptainer), e.g.
#   docker run --rm -v "$PWD":/io -w /io/python quay.io/pypa/manylinux_2_28_x86_64 bash tools/build_manylinux_wheels.sh cp312 cp311
#   apptainer exec --cleanenv --bind <repo> --pwd <repo>/python manylinux_2_28.sif bash tools/build_manylinux_wheels.sh
# Arguments: the Python versions (cp311, cp312, cp314t, ...; default every /opt/python/cp3*
# from cp39 on). For each: pip wheel (the CMake build of the library is made once, in build/, and
# reused by the next versions), auditwheel repair into wheelhouse/, then the wheel installed into a
# fresh venv (build/venv-<version>) and tests/test_python.py run from a temporary folder.
# Environment: PIW_BUILD_JOBS (the build's parallel jobs; default the CPUs this may run on),
# PIW_TEST_REQUIRES (more packages for the test venvs, e.g. "h5py hdf5plugin"; default none).
set -euo pipefail
cd "$(dirname "$0")/.."
here=$(pwd)

if [ $# -gt 0 ]; then
    tags=("$@")
else
    tags=()
    for d in /opt/python/cp3*; do
        t=$(basename "$d"); t=${t#*-}           # cp312-cp312 -> cp312, cp314-cp314t -> cp314t
        minor=${t#cp3}; minor=${minor%t}
        [ "$minor" -ge 9 ] && tags+=("$t")
    done
fi

export CC=${CC:-gcc} CXX=${CXX:-g++}
mkdir -p dist wheelhouse
failed=()
for tag in "${tags[@]}"; do
    abi=$tag pyver=${tag%t}                 # cp314t: /opt/python/cp314-cp314t, *-cp314-cp314t-*.whl
    py=/opt/python/$pyver-$abi/bin/python
    [ -x "$py" ] || { echo "== $tag: no /opt/python/$pyver-$abi"; failed+=("$tag"); continue; }
    echo "== $tag: $("$py" --version)"
    t0=$(date +%s)
    rm -f dist/*-"$pyver"-"$abi"-*.whl
    if ! "$py" -m pip wheel . --no-deps -w dist; then echo "== $tag: pip wheel FAILED"; failed+=("$tag"); continue; fi
    whl=$(ls dist/*-"$pyver"-"$abi"-*.whl | head -1)
    rm -f wheelhouse/*-"$pyver"-"$abi"-*.whl
    if ! LD_LIBRARY_PATH="$here/build/install/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
            auditwheel repair "$whl" -w wheelhouse; then
        echo "== $tag: auditwheel repair FAILED"; failed+=("$tag"); continue
    fi
    repaired=$(ls wheelhouse/*-"$pyver"-"$abi"-*.whl | head -1)
    echo "== $tag: $repaired built and repaired in $(( $(date +%s) - t0 )) s"

    venv=$here/build/venv-$tag
    rm -rf "$venv"
    "$py" -m venv "$venv"
    # shellcheck disable=SC2086
    if ! "$venv/bin/pip" install -q "$repaired" ${PIW_TEST_REQUIRES:-}; then
        echo "== $tag: pip install FAILED"; failed+=("$tag"); continue
    fi
    if (cd "$(mktemp -d)" && "$venv/bin/python" "$here/tests/test_python.py"); then
        echo "== $tag: tests passed"
    else
        echo "== $tag: tests FAILED"; failed+=("$tag")
    fi
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "FAILED: ${failed[*]}"
    exit 1
fi
echo "All wheels built, repaired and tested: ${tags[*]}"
