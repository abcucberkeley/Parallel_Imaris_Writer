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
