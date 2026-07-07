#!/usr/bin/env bash
# Make a macOS release folder self-contained w.r.t. the OpenMP runtime (libgomp).
#
# Homebrew's GCC records libgomp with an absolute install-name (e.g.
# /opt/homebrew/.../libgomp.1.dylib), so a machine without Homebrew GCC can't load
# it. This repoints every libgomp reference in the release binaries at the copy
# sitting next to them (@loader_path/libgomp.1.dylib), which the `release` target
# already placed in the folder.
#
# On Apple Silicon, install_name_tool invalidates a binary's code signature, and
# dyld SIGKILLs any process that loads an invalidly-signed dylib ("Subprocess
# killed"). So every binary we touch is re-signed ad-hoc afterwards.
#
# Usage: macos_bundle_openmp.sh <release-dir>
set -euo pipefail

REL="${1:?usage: macos_bundle_openmp.sh <release-dir>}"
GOMP="libgomp.1.dylib"

# Re-apply an ad-hoc signature (arm64 requires a valid signature to load).
resign() { codesign --force --sign - "$1" 2>/dev/null || true; }

if [ ! -f "${REL}/${GOMP}" ]; then
    echo "macos_bundle_openmp: ${REL}/${GOMP} not found; nothing to fix." >&2
    exit 0
fi

# Give the bundled runtime a loader-relative id, then re-sign it.
install_name_tool -id "@loader_path/${GOMP}" "${REL}/${GOMP}"
resign "${REL}/${GOMP}"

# Repoint each binary's libgomp dependency at the bundled copy, then re-sign.
# (NR>1 skips otool's header line, which for libgomp.1.dylib itself would match.)
for bin in parallelimariswriter libcppTiff.dylib libcppZarr.dylib; do
    target="${REL}/${bin}"
    [ -f "${target}" ] || continue
    ref="$(otool -L "${target}" | awk 'NR>1 && /libgomp/ {print $1; exit}')"
    if [ -n "${ref}" ] && [ "${ref}" != "@loader_path/${GOMP}" ]; then
        echo "  ${bin}: ${ref} -> @loader_path/${GOMP}"
        install_name_tool -change "${ref}" "@loader_path/${GOMP}" "${target}"
        resign "${target}"
    fi
done
