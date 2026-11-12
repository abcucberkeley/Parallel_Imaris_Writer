#ifndef PARALLEL_IMARIS_MONOIDX_H
#define PARALLEL_IMARIS_MONOIDX_H

// .idx sidecar of the single-file cluster output: which chunks one writer placed into the mono file and
// where. Written last, atomically (tmp + fsync + rename): it is the writer's commit point (no sidecar = not
// done). The finisher reads every expected sidecar, checks it against the claims journal and builds the
// chunk index from it (monovfd.h).
//
// Binary, little-endian, version 1:
//   header  (64 B)  "PIWMIDX1", u32 version, u32 levels, u64 chunks, u64 claim offset, u64 claim size,
//                   u64 blob bytes, i32 timepoint, i32 channel, u32 owner length, u32 0
//   owner           owner bytes, zero padded to a multiple of 8
//   levels  (64 B)  u32 level, u32 dtype, u64 size[3], u32 chunk[3], u32 grid[3], u64 chunks
//   chunks  (32 B)  u32 cx, cy, cz, u16 level, u16 0, u64 offset, u32 size, u32 filter mask
//   trailer (8 B)   FNV-1a 64 of everything before it
// Sizes and grids are x/y/z like the manifest; chunk offsets are absolute file offsets.
#include "monovfd.h"
#include <cstdint>
#include <string>
#include <vector>

struct MonoIdxLevel {
    uint32_t level = 0, dtype = 0;   // resolution level, MONO_U8/U16/U32/F32
    uint64_t size[3] = {0,0,0};      // image size of the level (voxels)
    uint32_t chunk[3] = {0,0,0};     // chunk dims (voxels)
    uint32_t grid[3] = {0,0,0};      // chunks per axis of the whole level (ceil(size/chunk))
    uint64_t nChunks = 0;            // chunks of this level in this sidecar
};
struct MonoIdx {
    std::string owner;               // claim owner, e.g. P_C0_T12
    int32_t timepoint = -1, channel = -1;
    uint64_t claimOffset = 0;        // the owner's claimed range ...
    uint64_t claimSize = 0;          // ... (a multiple of 4096)
    uint64_t blobBytes = 0;          // bytes written at claimOffset; claimSize = blobBytes rounded up to 4096
    std::vector<MonoIdxLevel> levels;
    std::vector<MonoChunk> chunks;   // any order; chunk.level names one of `levels`
};

// Internal consistency: geometry, chunk coordinates in the grid, no chunk listed twice, every chunk inside
// [claimOffset, claimOffset+blobBytes) and no two chunks overlapping, per-level counts. requireComplete:
// every chunk of every level's grid is present (a part holds whole levels).
bool mono_idx_validate(const MonoIdx& x, bool requireComplete, std::string* err);
// Validates, then writes atomically and durably.
bool mono_idx_write(const std::string& path, const MonoIdx& x, std::string* err);
// Reads and validates (structure, checksum, mono_idx_validate without requireComplete).
bool mono_idx_read(const std::string& path, MonoIdx& x, std::string* err);
// The Data dataset geometry of one level.
MonoGeom mono_idx_geom(const MonoIdxLevel& l);

// Writes a whole file atomically: <path>.tmp.<host>.<pid>, fsync, rename over <path>.
bool mono_write_atomic(const std::string& path, const void* data, size_t n, std::string* err);
// Reads a whole file.
bool mono_read_file(const std::string& path, std::vector<uint8_t>& out, std::string* err);
// FNV-1a 64 (sidecar, claim and receipt checksums).
uint64_t mono_fnv1a(const void* data, size_t n, uint64_t h = 14695981039346656037ull);
// "<host>.<pid>" (file-name safe), to name temporary files uniquely across nodes.
std::string mono_process_tag();

#endif
