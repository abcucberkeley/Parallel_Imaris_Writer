// Parallel Imaris Writer as a library: TIFF/Zarr volumes, or volumes in memory, to one Imaris
// (.ims) file, and the reading back of such files.
//
// Every function here is noexcept: errors come back as a Status, the text the command line
// prints goes to Callbacks::message, and nothing calls exit() or prints by itself. Options are
// structs passed by reference, so later fields (with defaults that keep today's output) do not
// change the function signatures.
#ifndef PARALLEL_IMARIS_WRITER_H
#define PARALLEL_IMARIS_WRITER_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#if defined(_WIN32)
#define PIW_API
#else
#define PIW_API __attribute__((visibility("default")))
#endif

namespace piw {

enum class Code : int { Ok = 0, InvalidArgument, NotFound, Unsupported, ReadFailed, WriteFailed, Cancelled, Internal };

struct Status {
    Code code = Code::Ok;
    std::string message;                 // one line, no "ERROR:" prefix, no newline
    Status() {}
    Status(Code aCode, const std::string& aMessage) : code(aCode), message(aMessage) {}
    bool ok() const { return code == Code::Ok; }
};

// All three are optional. message and cancel are called on the thread that called convert();
// progress on the Imaris writer's own thread, with a fraction that never decreases (1 when all
// blocks are written).
struct Callbacks {
    void (*message)(const char* text, void* user) = nullptr;   // the command line's stdout lines, newline included
    void (*progress)(float fraction, unsigned long long bytesWritten, void* user) = nullptr;
    int  (*cancel)(void* user) = nullptr;                       // polled between blocks; non-zero stops the conversion
    void* user = nullptr;
};

// A channel's base color: red, green and blue from 0 to 1.
struct Color {
    float red, green, blue;
};

// The per-axis values are along the input's axes 0, 1, 2: (row, column, page) of a TIFF, dims
// (0, 1, 2) of a Zarr. These are Imaris X, Y, Z, so the image's y (the TIFF row) is Imaris X, as in
// every file written so far; xOnImarisX puts the image's x there instead.
struct ConvertOptions {
    std::string reader = "tiff";                        // "tiff" or "zarr"
    std::vector<std::vector<std::string> > files;       // files[channel][timepoint]; every channel has as many
    std::string outputFile;                             // the .ims to write; an existing file is replaced
                                                        // (and left alone when the inputs are refused)
    float voxelSize[3] = {0.108f, 0.108f, 0.108f};      // along the input's axes 0, 1, 2
    bool hasBlockSize = false;    uint64_t blockSize[3] = {0, 0, 0};          // -b; default: the reader's chunks
    bool hasBoundingBox = false;  uint64_t boundingBox[6] = {0, 0, 0, 0, 0, 0}; // -B: a0,a1,a2,b0,b1,b2; 0-based, end excluded
    bool hasProcessBlock = false; uint64_t processBlock[3] = {0, 0, 0};       // -C, voxels
    int numThreads = 0;                                 // 0: the calling thread's OpenMP default
    // false: input axes 0, 1, 2 are Imaris X, Y, Z. true: axis 1 (the image's x) is Imaris X and
    // axis 0 Imaris Y; the values above stay along the input's axes.
    bool xOnImarisX = false;

    // Metadata (/DataSetInfo). Each list is empty (the default) or has one entry per channel.
    std::vector<std::string> channelNames;              // default "Channel 0", "Channel 1", ...; the command
                                                        // line passes its -P patterns
    std::vector<Color> channelColors;                   // default red, green, blue, red, green, ...
    std::vector<float> emissionWavelengths;             // nm, LSMEmissionWavelength; a value <= 0: none written
    std::vector<float> excitationWavelengths;           // nm, LSMExcitationWavelength; a value <= 0: none written
    // Time stamps (local wall-clock time, millisecond resolution). Time point t is at
    //   start + t * timeInterval                        when timeInterval > 0, else
    //   start + (stamp[t] - stamp[0]) ms                when every file of a channel has a "<n>msecAbs"
    //                                                   stamp in its name (the first such channel), else
    //   start + t seconds.
    // start = startTime, or else the modification time of files[0][0] (the first input).
    double timeInterval = 0;                            // seconds between time points
    std::string startTime;                              // "YYYY-MM-DD[ HH:MM[:SS[.mmm]]]" ('T' also separates)
};

// The sample types of an .ims file this library writes and reads.
enum class DataType : int { UInt8 = 1, UInt16 = 2, Float32 = 3, UInt32 = 4 };

// One 3-D volume in memory: data, and the distance in elements between neighbours along Imaris
// X, Y and Z (all positive). Any layout works; which array axis becomes Imaris X is the caller's
// choice of strides. A (z, y, x) C-contiguous array of nz x ny x nx with today's mapping (the
// image's y on Imaris X) is {nx, 1, nx*ny} and is read in place, as fast as {1, ny, ny*nx}.
struct Volume {
    const void* data = nullptr;
    int64_t stride[3] = {0, 0, 0};
};

struct WriteOptions {
    std::string outputFile;                             // the .ims to write; an existing file is replaced
    DataType dataType = DataType::UInt16;
    uint64_t size[3] = {0, 0, 0};                       // Imaris X, Y, Z of every volume (each below 2^32)
    unsigned channels = 1, timepoints = 1;
    float voxelSize[3] = {0.108f, 0.108f, 0.108f};      // along Imaris X, Y, Z
    bool hasBlockSize = false; uint64_t blockSize[3] = {64, 64, 64};  // pieces handed to the Imaris writer
    int numThreads = 0;                                 // 0: the calling thread's OpenMP default

    // Metadata (/DataSetInfo): as ConvertOptions's fields of the same names, except that without
    // startTime the first time point is the current time (there is no input file).
    std::vector<std::string> channelNames;
    std::vector<Color> channelColors;
    std::vector<float> emissionWavelengths;
    std::vector<float> excitationWavelengths;
    double timeInterval = 0;
    std::string startTime;
};

// The command line's input selection: in every folder, in the given order, the entries whose
// extension is .tif or .zarr and whose name contains the pattern; one channel per pattern, each
// sorted by full path (strcmp). A pattern without a match gives NotFound.
PIW_API Status findInputFiles(const std::vector<std::string>& folders, const std::vector<std::string>& patterns,
                              std::vector<std::vector<std::string> >& files) noexcept;

// Writes options.outputFile. Every input file is checked (shape, bit depth; with a bounding box,
// that it contains the box) before the output is created; after any later failure the partial
// output is removed. One conversion runs at a time in a process (calls from other threads wait).
PIW_API Status convert(const ConvertOptions& options, const Callbacks& callbacks = Callbacks()) noexcept;

// Writes options.outputFile from volumes in memory: volumes[channel*timepoints + timepoint] (the
// converter's file order), all of options.size. The file equals a conversion of files holding the
// same data with the same voxel size and metadata (the block size does not change the file). Fails
// with InvalidArgument before creating the output when an option or volume is unusable; after any
// later failure the partial output is removed. The volumes are only read.
PIW_API Status writeVolumes(const WriteOptions& options, const std::vector<Volume>& volumes,
                            const Callbacks& callbacks = Callbacks()) noexcept;

// Reading back .ims files this library writes (LZ4 chunks, with or without byte shuffle; files
// with other compression, such as gzip, give Unsupported). Axes are Imaris X, Y, Z.

// One /DataSet/ResolutionLevel l/TimePoint t/Channel c.
struct ImsDataset {
    unsigned level = 0, timepoint = 0, channel = 0;
    uint64_t size[3] = {0, 0, 0};         // ImageSizeX/Y/Z
    uint64_t chunk[3] = {0, 0, 0};        // HDF5 chunk size along X, Y, Z
    uint64_t chunks = 0;                  // chunks stored
    // With checksums (else 0). crc: zlib's crc32 over the stored chunks in ascending (z, y, x)
    // order, each as its offsets z, y, x and its byte count (8-byte little-endian each), then its
    // stored (compressed) bytes. histogramCrc: over the Histogram values, then the Histogram1024
    // values (8-byte little-endian each), then the group's attributes in name order (name, NUL,
    // value text, NUL).
    uint32_t crc = 0, histogramCrc = 0;
};

struct ImsInfo {
    uint64_t size[3] = {0, 0, 0};         // level 0, Imaris X, Y, Z
    unsigned channels = 0, timepoints = 0, levels = 0;
    DataType dataType = DataType::UInt16;
    float extentMin[3] = {0, 0, 0}, extentMax[3] = {0, 0, 0};   // /DataSetInfo/Image ExtMin0-2, ExtMax0-2
    float voxelSize[3] = {0, 0, 0};       // (extentMax - extentMin) / size
    std::vector<std::string> channelNames, channelColors;       // /DataSetInfo/Channel c: Name, Color
    std::vector<std::string> timePoints;  // /DataSetInfo/TimeInfo TimePoint1, TimePoint2, ...
    uint32_t thumbnailCrc = 0;            // crc32 of /Thumbnail/Data; only with checksums
    std::vector<ImsDataset> datasets;     // every (level, timepoint, channel), in that order
    // Every attribute of every group and dataset in the file, as ("<object path>/<name>", text),
    // objects in depth-first name order (e.g. "/DataSetInfo/Image/ExtMax0"). Numeric attributes
    // are their values separated by spaces.
    std::vector<std::pair<std::string, std::string> > attributes;
};

PIW_API Status imsInfo(const std::string& file, ImsInfo& info, bool checksums = false) noexcept;

// Reads the region [start, end) of one dataset into out: element (x, y, z) of the region goes to
// out[(x - start[0])*stride[0] + (y - start[1])*stride[1] + (z - start[2])*stride[2]] (strides
// positive, in elements). dataType must be the file's. numThreads > 0 sets the number of decoding
// threads of this call (no OpenMP setting changes); 0 uses the calling thread's OpenMP default.
PIW_API Status readIms(const std::string& file, unsigned level, unsigned timepoint, unsigned channel,
                       const uint64_t start[3], const uint64_t end[3], DataType dataType,
                       void* out, const int64_t stride[3], int numThreads = 0) noexcept;

// The whole command line, cluster subcommands included; returns the process exit code. Can be
// called more than once in a process.
PIW_API int runCli(int argc, char** argv) noexcept;

PIW_API const char* version() noexcept;                 // the CMake project version
PIW_API int defaultThreads() noexcept;                  // omp_get_max_threads() of the calling thread

}  // namespace piw

#endif
