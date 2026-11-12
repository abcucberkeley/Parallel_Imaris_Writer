#ifndef PARALLEL_IMARIS_MONOVFD_H
#define PARALLEL_IMARIS_MONOVFD_H

// Placement VFD for the single-file ("mono") cluster output.
//
// Cluster jobs pwrite their compressed chunks straight into the final .ims, at byte ranges they claimed
// (monoclaim.h), and record where each chunk went in a sidecar (monoidx.h). The file is laid out as
//     [0, 1 MiB)        header: HDF5 superblock + first metadata block (written by the finisher)
//     [1 MiB, rawEnd)   raw region: the chunk bytes, placed by the jobs
//     [rawEnd, EOF)     tail: every other HDF5 object (groups, attributes, histograms, chunk B-trees)
// The finisher then builds the HDF5 structure in that same file through this driver. Its `alloc` callback
// hands HDF5 the address where an "armed" chunk already sits and its write callback drops that chunk's
// payload, so H5Dwrite_chunk only builds the real (libver V18, v1 B-tree) chunk index. All other HDF5 writes
// pass through and land in the header or the tail. The result is an ordinary HDF5 file for the stock driver.
//
// Driver rules (any deviation is a hard error: recorded in the session and returned to HDF5 as a failure):
//   features : AGGREGATE_METADATA | ACCUMULATE_METADATA | DEFAULT_VFD_COMPATIBLE. No small-data aggregation
//              and no data sieving, so every raw allocation reaches `alloc` on its own; no `free` callback.
//   open     : create only (H5F_ACC_TRUNC). Never truncates the raw region: zeroes the header region and
//              drops any old tail beyond rawEnd, so finishing again rebuilds the same structure.
//   alloc    : armed raw chunk of the armed size -> the armed address; first block (superblock) -> 0, and
//              the end of allocation (EOA) jumps to rawEnd; anything else -> appended at EOA (the tail).
//   write    : armed raw chunk -> must hit the armed address and size, payload discarded; anything else
//              must not touch [1 MiB, rawEnd) and is written.
//   truncate : requires EOA >= rawEnd; the file ends at EOA. Close fsyncs.
//
// HDF5 is pinned at 1.14.6. This relies on H5FD__alloc_real calling the driver's `alloc` for raw data and on
// raw allocations bypassing the aggregators (no AGGREGATE_SMALLDATA); monocoretest re-validates both, so
// run it after any HDF5 bump. Linux and macOS only: on Windows every function reports "not supported".
// Not thread-safe (HDF5 is not either): one session per file, used from one thread.
#include "hdf5.h"
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

static const uint64_t MONO_RAW_BEGIN = 1u << 20;   // first byte of the raw region; claims start here
static const uint64_t MONO_ALIGN = 4096;           // claims and blobs are multiples of this

// Sample types of a Data dataset (also stored in the .idx sidecar).
enum { MONO_U8 = 1, MONO_U16 = 2, MONO_U32 = 3, MONO_F32 = 4 };
unsigned mono_dtype_bytes(unsigned dtype);   // 1, 2 or 4; 0 for an unknown code

// One chunk whose compressed bytes are already in the raw region.
struct MonoChunk {
    uint64_t offset;       // absolute file offset of its compressed bytes
    uint32_t cx, cy, cz;   // chunk-grid coordinates (x fastest, like the manifest's blocks)
    uint32_t size;         // compressed bytes
    uint32_t mask;         // HDF5 filter mask (0: every filter applied)
    uint16_t level;        // resolution level (used by the .idx sidecar)
    uint16_t reserved;
};

// Geometry of one Data dataset, x/y/z like the manifest. The dataset holds whole chunks (grid*chunk voxels
// per axis, as writeOneDataset does); size is the image inside it.
struct MonoGeom { uint64_t size[3]; uint32_t chunk[3]; uint32_t grid[3]; unsigned dtype; };

struct MonoVfdStats {
    uint64_t rawEnd = 0;
    uint64_t eoa = 0;                        // end of allocation = file size after close
    uint64_t armed = 0;                      // chunk writes armed
    uint64_t placed = 0;                     // ... whose allocation got the armed address
    uint64_t discarded = 0;                  // ... whose payload write was dropped
    uint64_t allocs = 0;                     // other allocations (metadata, small contiguous datasets)
    uint64_t writes = 0, writeBytes = 0;     // pass-through writes (all types)
    uint64_t rawWrites = 0, rawWriteBytes = 0;   // ... of which raw data (histograms, thumbnail)
    uint64_t reads = 0, readBytes = 0;
    uint64_t failures = 0;                   // rule violations and I/O errors
};

// One placement session = one file being built. Owned by the caller; it must outlive the HDF5 file opened
// through its FAPL. The members are internal: use the functions below.
struct MonoVfd {
    explicit MonoVfd(uint64_t rawEnd);
    uint64_t rawEnd;
    int armState;
    uint64_t armAddr, armSize;
    bool inUse;
    MonoVfdStats st;
    std::string err;
    std::vector<char> zeros;
};

// New file-access property list bound to the session: placement driver, libver bounds V18..V18, no file
// locking. Use it with H5Fcreate(path, H5F_ACC_TRUNC, ...) only, with no other HDF5 handle open on that
// file and every writer done (the claims journal sealed: its seal offset is rawEnd). The caller closes the
// property list. -1 on error.
hid_t mono_vfd_fapl(MonoVfd& s, std::string* err);
// Arms the next raw allocation: it gets addr, and its write (size bytes) is dropped.
void mono_vfd_arm(MonoVfd& s, uint64_t addr, uint64_t size);
// True once the armed chunk was allocated at its address and its payload write was dropped.
bool mono_vfd_consumed(const MonoVfd& s);
void mono_vfd_disarm(MonoVfd& s);
const MonoVfdStats& mono_vfd_stats(const MonoVfd& s);
// False after any rule violation or I/O error; err gets the first one.
bool mono_vfd_ok(const MonoVfd& s, std::string* err);

// Creates <loc>/<name> as a Data dataset like writeOneDataset: whole chunks, shuffle for 2/4-byte types,
// LZ4 filter 32004 (registered as a no-op if no LZ4 filter is available). Intermediate groups are created.
hid_t mono_create_dataset(hid_t loc, const char* name, const MonoGeom& g, std::string* err);

// at(k, chunk) fills chunk k of n. Chunks must be sorted by (cz, cy, cx) with no duplicates.
typedef std::function<void(size_t, MonoChunk&)> MonoChunkAt;

// Creates the dataset and inserts its n chunks (H5Dwrite_chunk with the chunk armed; no payload is
// written). Every chunk must lie in [MONO_RAW_BEGIN, rawEnd) and inside the grid. Linear time.
bool mono_index_dataset(MonoVfd& s, hid_t loc, const char* name, const MonoGeom& g, size_t n,
                        const MonoChunkAt& at, std::string* err);
// Same for a list in any order (sorted here).
bool mono_index_dataset(MonoVfd& s, hid_t loc, const char* name, const MonoGeom& g,
                        std::vector<MonoChunk>& chunks, std::string* err);

// For a file reopened with the stock driver: checks the dataset's shape, chunking and type against g and
// its chunk index (H5Dchunk_iter: coordinates, address, size, filter mask) against the n expected chunks
// (sorted as above). *mismatches gets the number of wrong, missing or unexpected chunks.
bool mono_verify_dataset(hid_t dset, const MonoGeom& g, size_t n, const MonoChunkAt& at,
                         uint64_t* mismatches, std::string* err);
bool mono_verify_dataset(hid_t dset, const MonoGeom& g, std::vector<MonoChunk>& chunks,
                         uint64_t* mismatches, std::string* err);

#endif
