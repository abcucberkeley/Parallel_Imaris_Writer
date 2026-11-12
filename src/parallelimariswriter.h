// Parallel Imaris Writer as a library: TIFF/Zarr volumes to one Imaris (.ims) file.
//
// Every function here is noexcept: errors come back as a Status, the text the command line
// prints goes to Callbacks::message, and nothing calls exit() or prints by itself. Options are
// structs passed by reference, so later fields (with defaults that keep today's output) do not
// change the function signatures.
#ifndef PARALLEL_IMARIS_WRITER_H
#define PARALLEL_IMARIS_WRITER_H

#include <cstdint>
#include <string>
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

// X, Y, Z are the Imaris axes: (row, column, page) of a TIFF, dims (0, 1, 2) of a Zarr.
struct ConvertOptions {
    std::string reader = "tiff";                        // "tiff" or "zarr"
    std::vector<std::vector<std::string> > files;       // files[channel][timepoint]; every channel has as many
    std::string outputFile;                             // the .ims to write; an existing file is replaced
                                                        // (and left alone when the inputs are refused)
    float voxelSize[3] = {0.108f, 0.108f, 0.108f};      // along X, Y, Z
    bool hasBlockSize = false;    uint64_t blockSize[3] = {0, 0, 0};          // -b; default: the reader's chunks
    bool hasBoundingBox = false;  uint64_t boundingBox[6] = {0, 0, 0, 0, 0, 0}; // -B: x0,y0,z0,x1,y1,z1; 0-based, end excluded
    bool hasProcessBlock = false; uint64_t processBlock[3] = {0, 0, 0};       // -C, voxels
    int numThreads = 0;                                 // 0: the calling thread's OpenMP default

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

// The command line's input selection: in every folder, in the given order, the entries whose
// extension is .tif or .zarr and whose name contains the pattern; one channel per pattern, each
// sorted by full path (strcmp). A pattern without a match gives NotFound.
PIW_API Status findInputFiles(const std::vector<std::string>& folders, const std::vector<std::string>& patterns,
                              std::vector<std::vector<std::string> >& files) noexcept;

// Writes options.outputFile. Every input file is checked (shape, bit depth; with a bounding box,
// that it contains the box) before the output is created; after any later failure the partial
// output is removed. One conversion runs at a time in a process (calls from other threads wait).
PIW_API Status convert(const ConvertOptions& options, const Callbacks& callbacks = Callbacks()) noexcept;

// The whole command line, cluster subcommands included; returns the process exit code. Can be
// called more than once in a process.
PIW_API int runCli(int argc, char** argv) noexcept;

PIW_API const char* version() noexcept;                 // the CMake project version
PIW_API int defaultThreads() noexcept;                  // omp_get_max_threads() of the calling thread

}  // namespace piw

#endif
