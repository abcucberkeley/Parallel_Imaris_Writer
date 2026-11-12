#ifndef PARALLEL_IMARIS_MONO_H
#define PARALLEL_IMARIS_MONO_H

// Single-file ("mono") cluster output: one (timepoint, channel) per job (the time-series path, here), or the tile
// workers of setup's mono store (parallel.h: single enormous volumes), whose single file monofinish and monostatus
// also take (see "Tiles" below).
//
// Every (t,c) is converted on its own by the single-node converter (one timepoint and one channel per run,
// any input, cropped or not: a "part"); monoplace copies the part's compressed chunks verbatim into ONE .ims,
// at a byte range it claims (monoclaim.h), and leaves sidecars (monoidx.h + exact histograms); monofinish then
// builds the HDF5 structure over the placed chunks through the placement driver (monovfd.h) and writes the
// metadata of a single-node conversion of all the volumes (imscore.h's shared writers, ColorRange from the
// exact level-0 histograms of every timepoint). The result is one self-contained .ims (no links, no parts)
// whose data, metadata and /Thumbnail (imsthumb.h) equal the single-node converter's.
//
// Workdir:
//   manifest.json              timepoints, channels, voxel size [, output path], the metadata: monoinit (or setup)
//   output.ims.partial         the file being built; monofinish renames it to the output (same filesystem)
//   mono/layout.json           every level's geometry: written by the first placer, checked by every later one
//   mono/claims/, owners/ ...  the claims journal of the raw region (monoclaim.h)
//   mono/h_L<r>_C<c>_T<t>.hist the exact 65536-bin histogram (uint64, in-image voxels) of level r of each part
//                              (uint8 and uint16; uint8 uses bins 0..255)
//   mono/P_C<c>_T<t>.fhist     float32 parts instead: the part's stored histograms and ColorRange (see monoFinish)
//   mono/P_C<c>_T<t>.idx       the part's sidecar: written last, the part's commit point
//   mono/finished              the output path, once monofinish has renamed the file
//
// Commands (dispatched from src/cli.cpp; exit 0 ok, 1 error, 2 usage, 3 incomplete):
//   monoinit   <workdir> <nT> <nC> <vx,vy,vz> [out.ims] [--option value ...]
//                                                           the manifest, for parts made from any input; the
//                                                           options are the converter's metadata options
//                                                           (--channel-names, --colors, ...; imsmeta.h)
//   monoplace  <workdir> <t> <c> <part.ims> [--threads N]   place one part; re-runnable until the finish
//   monofinish <workdir> [out.ims] [--verify] [--threads N] refuses (exit 3, naming them) while parts are missing
//   monostatus <workdir>                                    placed / missing parts (exit 3 if any is missing)
// A failed or killed monoplace is simply run again: the same part gets the same range and writes the same
// bytes. monofinish seals the journal (later placements fail), builds the file, checks its chunk index with
// the stock HDF5 driver against the sidecars, and renames it; until the rename it can be run again.
//
// Tiles: in a workdir made by setup with the mono store (the default), every tile worker writes its tile's chunks
// straight into output.ims.partial at a range it claims (owner L<r>_C<c>_T<t>_<tile>), then its histogram
// chunks/h_L<r>_C<c>_T<t>_<tile>.hist and its sidecar chunks/L<r>_C<c>_T<t>_<tile>.idx (the commit point); the
// coarser levels read the finer levels' chunks back from the file. monofinish (which monitor runs once every
// sidecar exists) then works as for parts, with the geometry from the manifest (setup lays the blocks out for all
// nT timepoints, so no level is re-chunked) and each dataset's histogram summed from its tiles'. A workdir takes
// parts or tiles, never both: a plan's workdir (parallel.h's clusterPlan) says which in its manifest ("kind"); a
// setup workdir is a tile workdir once its first worker has marked it (mono/tiles) and takes parts until then.
//
// Every step is also a function here: it returns a Status (no exit, no printing; the command line prints the
// results), takes its thread count as an argument, and holds the library's HDF5 mutex only around its HDF5
// calls. Parts of uint8, uint16 or float32 data (every part of a workdir the same); tiles of uint8 or uint16. Linux and
// macOS: on Windows every step returns Unsupported.
#include "parallelimariswriter.h"
#include <cstdint>
#include <string>
#include <vector>

namespace piw {

struct MonoInitOptions {
    std::string workdir;
    int timepoints = 0, channels = 0;
    float voxelSize[3] = {0.108f, 0.108f, 0.108f};   // X, Y, Z, as the converter's -v
    std::string outputFile;                          // optional: monofinish's default output
    // The metadata monofinish writes: ConvertOptions's fields of the same names, with its defaults and checks.
    // files = the parts' inputs, [channel][timepoint] (optional): with them the time stamps are those of a
    // single-node conversion of these files (msecAbs stamps, or the first one's modification time); without
    // them and without startTime, time point 0 is the time the manifest is written.
    std::vector<std::vector<std::string>> files;
    std::vector<std::string> channelNames;           // default "Channel 0", "Channel 1", ...
    std::vector<Color> channelColors;
    std::vector<float> emissionWavelengths, excitationWavelengths;
    double timeInterval = 0;
    std::string startTime;
};
// Writes <workdir>/manifest.json (creating the workdir), with the metadata resolved once. An existing manifest
// with the same timepoints, channels and voxel size (e.g. setup's) is kept, metadata and all, when no metadata
// field is set; with one set, its metadata must also be what these fields give (a start time taken from the
// clock matches any). Other values are an error.
Status monoInit(const MonoInitOptions& options) noexcept;

struct MonoPlaceOptions {
    std::string workdir;
    int timepoint = -1, channel = -1;
    std::string partFile;   // the converter's .ims of this one (timepoint, channel): uint8, uint16 or float32
    int numThreads = 0;     // threads that read, decode and write; 0: defaultThreads()
};
struct MonoPlaceResult {
    std::string owner;                         // P_C<c>_T<t>
    int levels = 0;
    uint64_t chunks = 0, bytes = 0;            // chunks placed and their compressed bytes
    uint64_t claimOffset = 0, claimSize = 0;   // the byte range in the file
    double scanSeconds = 0;                    // open the part, list its chunks
    double decodeSeconds = 0;                  // read and decode the chunks, histograms (overlaps the writes)
    double placeSeconds = 0;                   // claim to the last byte synced
    double totalSeconds = 0;
    bool direct = false;                       // O_DIRECT (or F_NOCACHE) writes
    int threads = 0;
};
// Checks the part (one timepoint and one channel, uint8, uint16 or float32, every chunk of every level present and
// decodable, geometry and sample type equal to mono/layout.json), claims its range, writes its chunks there in
// (level, z, y, x) order, then its histograms and its sidecar.
Status monoPlace(const MonoPlaceOptions& options, MonoPlaceResult* result = nullptr) noexcept;

struct MonoProgress {
    int timepoints = 0, channels = 0;
    bool tiles = false;                         // a tile workdir (setup's mono store); levels: its levels
    int levels = 0;
    std::vector<std::string> placed, missing;   // owners P_C<c>_T<t>, in (t, c) order; tiles: L<r>_C<c>_T<t>_<tile>,
                                                // in (level, t, c, tile) order
    bool sealed = false;                        // monofinish has started: no more placements
    bool finished = false;                      // ... and renamed the file to finishedFile
    std::string finishedFile;
    uint64_t claimedBytes = 0;                  // end of the claimed raw region
};
// Which parts (or tiles) have their sidecar. Does not change anything.
Status monoStatus(const std::string& workdir, MonoProgress& progress) noexcept;

struct MonoFinishOptions {
    std::string workdir;
    std::string outputFile;   // empty: the manifest's, else <workdir>/output.ims
    bool verify = false;      // also read and decode every chunk from the file and check it against the
                              // part's histograms (reads the whole file)
    int numThreads = 0;       // 0: defaultThreads()
};
struct MonoFinishResult {
    std::string outputFile;
    std::vector<std::string> missing;   // owners (parts or tiles) without a sidecar: then the Status is NotFound
    bool alreadyFinished = false;       // an earlier run renamed the file (nothing was done)
    int timepoints = 0, channels = 0, levels = 0;
    uint64_t chunks = 0;                // chunks placed by the parts (or tiles)
    std::vector<int> rechunkedLevels;   // levels laid out for nT timepoints (see monoFinish)
    uint64_t rechunkedChunks = 0;       // chunks written for them
    std::string note;                   // set when no level could be re-chunked
    uint64_t rawEnd = 0;                // end of the raw region (= the sealed journal's end)
    uint64_t fileBytes = 0;
    double readSeconds = 0;             // sidecars, journal, seal, the thumbnail's voxels
    double indexSeconds = 0;            // HDF5 structure through the placement driver
    double checkSeconds = 0;            // stock-driver check of every chunk index
    double verifySeconds = 0;
    double totalSeconds = 0;
};
// Builds the file and renames it (parts or tiles, whichever the workdir holds). ImarisWriter's block layout
// depends on the timepoint count: a single-node conversion of nT timepoints gives some small levels smaller
// chunks (a block spanning timepoints pads less) than the 1-timepoint parts have. Those levels (the real
// 167-timepoint series: levels 5 and 6, at most a few million voxels per part) are decoded from the placed
// chunks, cut into the nT-timepoint chunks, encoded again (the same bytes ImarisWriter writes) and stored after
// the raw region, so the whole file equals the single-node conversion; their placed chunks stay unused in the
// raw region. Tiles are already laid out for nT timepoints.
//
// float32 parts: the data equals a single-node conversion's (the chunk bytes are copied, re-chunked levels are
// encoded as ImarisWriter encodes them). The histograms cannot be exact: ImarisWriter's float32 histogram
// (bpHistogramBuilderAdaptive) is 1000 bins whose range grows with the values added, so its bins depend on the order
// the values come in (for levels >= 1 even ImarisWriter's own runs differ by a bin now and then: the order of its
// resample threads). Each part's stored histograms are copied as they are: every level of every (t, c) holds what
// the part's own conversion wrote. ColorRange: with one timepoint it is the part's own (the text a single-node
// conversion of that volume writes); with more, ImarisWriter's multi-timepoint rule (channelColorHistMulti +
// adjustColorRange) on the parts' level-0 histograms, whose ranges are known only as the 3-decimal texts
// HistogramMin/Max. That error, d <= 0.0005 + half a float ulp of the larger |min| or |max|, can move a count into
// the next of the 65536 layout bins, and so (rarely) the mode or 99.8th-percentile bin by one of 1024 bins: then the
// ColorRange minimum differs by at most d + R/1024 and the maximum by at most 1.4 (d + R/1024) + 1 (its integer
// cut), R = the channel's value range over all timepoints; usually it is the same. The /Thumbnail is coloured with
// that ColorRange's value; with one timepoint that is the 3-decimal text, not ImarisWriter's exact float, so a few
// thumbnail bytes can differ by 1.
Status monoFinish(const MonoFinishOptions& options, MonoFinishResult* result = nullptr) noexcept;

namespace detail {
// Whether part (t, c)'s commit point is there: its sidecar (whole, of this part) and every histogram sidecar it
// needs (parallel.h's clusterTask skips a placed part).
bool monoPartPlaced(const std::string& workdir, int t, int c) noexcept;
}

}  // namespace piw

// True if cmd is one of the commands above; runMono runs it (argv[1] = the command), returns the exit code.
bool isMonoCommand(const char* cmd);
int runMono(int argc, char** argv);
// The lines monoplace and monofinish print (also printed by parallel.h's task and finish).
std::string monoPlaceLine(const piw::MonoPlaceResult& result);
void monoPrintFinish(const piw::MonoFinishResult& result, bool verify);

#endif
