#!/usr/bin/env bash
# Make a macOS release folder self-contained w.r.t. the OpenMP runtime (libgomp).
#
# Homebrew's GCC records libgomp with an absolute install-name (e.g.
# /opt/homebrew/.../libgomp.1.dylib), so a machine without Homebrew GCC can't load it.
# This repoints every libgomp reference in the release binaries to the copy sitting
# next to them (@loader_path/libgomp.1.dylib), which the `release` target already
# placed in the folder.
#
# Usage: macos_bundle_openmp.sh <release-dir>
set -euo pipefail

REL="${1:?usage: macos_bundle_openmp.sh <release-dir>}"
GOMP="libgomp.1.dylib"

if [ ! -f "${REL}/${GOMP}" ]; then
    echo "macos_bundle_openmp: ${REL}/${GOMP} not found; nothing to fix." >&2
    exit 0
fi

# Give the bundled runtime a loader-relative id.
install_name_tool -id "@loader_path/${GOMP}" "${REL}/${GOMP}" || true

# Repoint each binary's libgomp dependency at the bundled copy.
for bin in parallelimariswriter libcppTiff.dylib libcppZarr.dylib "${GOMP}"; do
    target="${REL}/${bin}"
    [ -f "${target}" ] || continue
    ref="$(otool -L "${target}" | awk '/libgomp/{print $1; exit}')"
    if [ -n "${ref}" ] && [ "${ref}" != "@loader_path/${GOMP}" ]; then
        echo "  ${bin}: ${ref} -> @loader_path/${GOMP}"
        install_name_tool -change "${ref}" "@loader_path/${GOMP}" "${target}"
    fi
done
