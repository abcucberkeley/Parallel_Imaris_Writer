# Modified ImarisWriter files

The converter is built on [ImarisWriter](https://github.com/imaris/ImarisWriter), which CMake
fetches at a pinned commit (`b128e6e7d1a147261e9d5caf24ebc6b5c9c63779`, see `CMakeLists.txt`).
`overrides/` holds modified copies of a few of its files, at the same relative paths as upstream.
They are Apache-2.0 like the rest of ImarisWriter, keep the original copyright and license header,
and say at the top that they were modified; `NOTICE` says the same for the directory.

## How the build uses them

`cmake/imariswriter_overrides.cmake` copies the fetched `c/`, `interface/`, `interfaceC/` and
`writer/` into `<build>/imariswriter-patched/`, with each file of `overrides/` in place of the
upstream file, and ImarisWriter is compiled from that copy. A copy of the whole tree, not an
include-path order, is needed because a quoted `#include` looks in the including file's directory
first: an upstream file would otherwise still see the upstream version of an overridden header.

`upstream.sha256` holds the SHA-256 of every overridden upstream file at the pinned commit (line
endings normalized to LF). Configuring stops with an error if a fetched file differs, if a listed
file has no override, or if an override has no hash, so moving the pin can never mix an override
with an upstream version it was not made from.

To add an override: copy the upstream file at the pinned commit to `overrides/<same path>`, add its
`sha256sum` line to `upstream.sha256`, add the modification line below the license header, change
it, and list the change here. To move the pin: for each override, apply the changes listed here to
the new upstream file, and record the new upstream file's hash.

## Changes

### A failed write ends the conversion

A failed write used to hang the conversion forever: the error of `H5Dwrite_chunk` (disk full, quota,
file-size limit, I/O error) was re-thrown in a compute-thread task and dropped by the thread pool,
the resample closures of the blocks that were never submitted were never run, and
`FinishWriteDataBlocks` polled their count forever. Errors of the resampling, compression and
histogram threads were dropped the same way, and errors of the final close of the file were ignored.
Now the first error of any thread ends the conversion: `bpImageConverterC_CopyBlock*` (from its next
call) and `bpImageConverterC_Finish` report it through `bpImageConverterC_GetLastException`, and
`bpImageConverterC_Destroy` returns after all threads have stopped. The data written is unchanged.

- `writer/bpMultiresolutionImsImage.h`, `.cxx`
  - A shared pipeline state (`bpPipelineState`) keeps the first error raised in any thread, and
    counts the resample closures and the lower-resolution compute tasks they queue that are not
    finished yet; it replaces `mResampleCount`. Each is counted for as long as a copy of it exists, so
    one that throws, or is dropped because its block's write could not be started, still counts as
    done. (The full-resolution compute tasks, one per copied slice, are not counted, to keep the copy
    path as it was; `WaitAll` of the compute thread waits for them.)
  - Compute-thread tasks, resample closures and histogram tasks catch their exceptions and record
    them; once something has failed, compute tasks and resamples skip their work.
  - `CopyData` throws the recorded error. `FinishWriteDataBlocks` waits for the compute thread and
    then on the pending count (condition variable) instead of polling the resample count, which kept
    the calling thread busy, never ended after a lost resample, and could see 0 while the compute task
    queued by the last resample had not run yet; it returns as soon as an error is recorded and
    throws it.
  - `WriteMetadata` closes the file (`bpWriter::Close`) after the metadata and the thumbnail.
  - The destructor stops new work and waits until every thread is idle before the members are
    destroyed (tasks still queued after a failure used the image and its thread pools).
- `writer/bpWriterThreads.cxx`
  - The first failed write (or failed resample/compression in a compression thread) is kept; every
    later `StartWrite` and `FinishWrite`, and the wait for free buffer memory, throw it, and the
    writes still queued are skipped (when creating the file failed, there is no HDF5 writer to call).
  - A block whose resample or compression throws still returns its buffer memory (otherwise the
    wait for free memory spun forever).
  - The free-memory count is atomic: the histogram threads return memory concurrently.
- `writer/bpWriter.h`, `writer/bpWriterCompressor.h`, `.cxx`
  - New `Close()`, forwarded to the HDF5 writer.
  - `bpWriterCompressor` throws "the output file was not created" instead of calling a missing
    HDF5 writer; its destructor catches every exception.
- `writer/bpWriterHDF5.h`, `.cxx`
  - `Close()` closes the cached datasets and groups, then calls `H5Fflush` and `H5Fclose` and
    throws if either fails (before, the destructor closed the file and ignored the result, so an
    error reported at close, typical of NFS, gave exit code 0 and a damaged file).
  - The messages of a failed write, a failed dataset creation, a failed close and a file that
    cannot be created end with the reason from HDF5's error stack (for a failed system call only
    the system's message, such as "No space left on device").

### Destroy at any point

`bpImageConverterC_Destroy` may be called at any time: before the first block, part-way (the
converter stops so after a read error, a cancel, Python's Ctrl-C or a failed write), after the last
block without `bpImageConverterC_Finish`, or after it. It returns once every thread of the converter
has stopped. Without `Finish` it stops the conversion, and the file it leaves is not a usable .ims:

- the blocks that were already handed to the compression threads are compressed and written; the
  compute tasks and resamplings still queued are skipped, so lower resolution levels are partial;
- no histograms, no `/DataSetInfo` (metadata) and no `/Thumbnail` are written;
- the HDF5 file is closed: a well-formed HDF5 file with the root attributes and the data written so
  far. The converter (`src/converter.cpp`) deletes it.

Destroying a converter without `Finish` used to corrupt the heap now and then ("free(): corrupted
unsorted chunks"; in about half of the test-suite runs, which stop a few conversions early), which is
why the converter called `Finish` before `Destroy` even for a file it was about to delete. A worker
of `bpThreadPool` destroyed a finished task's function, and the memory blocks it had captured, only
after counting the task as finished and waking the waiting threads. The destructor of
`bpWriterThreads` had just waited for its writer thread that way, freed its memory manager, and the
writer thread then returned its last task's blocks to the freed manager (AddressSanitizer:
heap-use-after-free in `bpMemoryManager::ReturnMemory`, from `bpThreadPool::TreadLoop`).

- `writer/bpThreadPool.cxx`
  - A worker destroys a task's function, with everything it captured, before the task counts as
    finished: a thread that waits for the pool (`WaitAll`, `WaitSome`, `WaitOne`) may destroy what
    the captures refer to as soon as it returns.
  - A finished task wakes every waiting thread (`notify_all`, was `notify_one`): several threads can
    wait on one pool at a time (the histogram threads, for instance, wait for the writer thread's
    buffer memory while `Finish` writes the histograms), and a single notification could wake one
    whose condition did not hold and leave the others waiting.
- `writer/bpWriterThreads.cxx`, `writer/bpWriterCompressor.h`, `.cxx`
  - The thread pools are declared last, so that they are joined, and the tasks still queued are
    destroyed, before the members their tasks use: the memory manager and the compression algorithm
    of `bpWriterThreads`, the progress callback and counters of `bpWriterCompressor`. Threads that
    queue work for others are joined first: compression, then writer, then progress callback.

### The output file exists before the first write

- `writer/bpWriterCompressor.cxx`
  - The constructor creates the HDF5 writer, and with it the file, on the calling thread. It was
    created by a task queued through the compression threads to the writer thread, so when the first
    block's compression finished before that task's, the block's write came first and found no
    writer: the conversion failed with "the output file was not created" (seen for volumes of one
    block, about once in 40 runs of the test suite; upstream dereferences the missing writer). A file
    that cannot be created is now reported by `bpImageConverterC_Create`.

### A larger in-flight budget

- `writer/bpWriterCompressor.cxx`
  - The memory for the blocks that are compressed but not written yet (raw and compressed bytes,
    the budget `bpWriterThreads` waits on) is at least 2 GB (`kMinInFlightBufferMB`), upstream
    `threads*3 + 32` MB (56 MB at 8 threads). With the small budget every stall of the HDF5 writer
    thread stopped the compression threads at once; the converter's early writeback
    (`src/converter.cpp`) makes such stalls more frequent, since the writer thread then shares the
    file with the kernel's writes. Measured on one 43 GB volume at 8 cores, the compute thread's
    wait for the budget dropped from 19 s to 0.2 s. The budget bounds the memory: it is taken only
    while blocks wait for the writer (and kept for reuse until the converter is destroyed). The
    data written is unchanged.
