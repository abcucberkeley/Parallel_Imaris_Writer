// Python bindings of the Parallel Imaris Writer library (src/parallelimariswriter.h). The
// functions are named pybind11_*; the Python API is parallel_imaris_writer_wrappers.py.
//
// Every call that converts or runs the command line does so on a worker thread of its own while
// the calling thread waits without the GIL (callOnWorker): other Python threads keep running,
// Ctrl-C can stop a conversion, and the OpenMP threads belong to a thread that ends with the call
// (measured: a child made by fork() after a conversion on the calling thread hung in its first
// conversion; after conversions on worker threads it ran).
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "parallelimariswriter.h"

namespace {

// What a call's worker shares with the waiting thread. The library's callbacks run on the worker
// (and the Imaris writer's thread), so they only touch this; the waiting thread, holding the GIL,
// passes it on to Python.
struct CallState {
    std::atomic<bool> cancel{false};
    bool verbose = false;
    std::mutex textMutex;
    std::string text;          // the command line's messages, not printed yet (verbose)
};

void messageCallback(const char* aText, void* aUser)
{
    CallState* vState = static_cast<CallState*>(aUser);
    if(!vState->verbose) return;
    std::lock_guard<std::mutex> vLock(vState->textMutex);
    vState->text += aText;
}

int cancelCallback(void* aUser)
{
    return static_cast<CallState*>(aUser)->cancel.load() ? 1 : 0;
}

piw::Callbacks callbacksFor(CallState& aState)
{
    piw::Callbacks vCallbacks;
    vCallbacks.message = messageCallback;
    vCallbacks.cancel = cancelCallback;
    vCallbacks.user = &aState;
    return vCallbacks;
}

// Prints the queued messages through sys.stdout (GIL held).
void printQueued(CallState& aState)
{
    std::string vText;
    {
        std::lock_guard<std::mutex> vLock(aState.textMutex);
        vText.swap(aState.text);
    }
    if(vText.empty()) return;
    pybind11::object vStdout = pybind11::module_::import("sys").attr("stdout");
    vStdout.attr("write")(vText);
    vStdout.attr("flush")();
}

// Runs aWork() on a new thread and returns its result; the calling thread (GIL held on entry and
// on return) waits without the GIL. With aState, the waiting thread looks every 50 ms for a
// signal (Ctrl-C) or another Python error: it then asks the work to stop (aState->cancel), waits
// for it and raises that error. Without aState the work cannot be stopped, and a signal is
// handled by Python once the call has returned.
template <class Work>
auto callOnWorker(Work aWork, CallState* aState) -> decltype(aWork())
{
    typedef decltype(aWork()) Result;
    std::packaged_task<Result()> vTask(aWork);
    std::future<Result> vDone = vTask.get_future();
    std::thread vWorker(std::move(vTask));
    std::unique_ptr<pybind11::error_already_set> vError;   // the first Python error while waiting
    for(;;){
        std::future_status vWait;
        {
            pybind11::gil_scoped_release vRelease;
            vWait = vDone.wait_for(std::chrono::milliseconds(50));
        }
        if(aState){
            try{
                if(!vError && PyErr_CheckSignals() != 0) throw pybind11::error_already_set();
                if(aState->verbose) printQueued(*aState);
            }
            catch(pybind11::error_already_set& vPythonError){
                if(!vError) vError.reset(new pybind11::error_already_set(vPythonError));
                aState->cancel = true;
            }
        }
        if(vWait == std::future_status::ready) break;
    }
    vWorker.join();
    if(vError) throw *vError;
    return vDone.get();   // rethrows what the work threw
}

// A failed Status as the Python exception for it.
void throwIfFailed(const piw::Status& aStatus)
{
    switch(aStatus.code){
        case piw::Code::Ok:
            return;
        case piw::Code::InvalidArgument:
        case piw::Code::Unsupported:
            throw pybind11::value_error(aStatus.message);
        case piw::Code::NotFound:
            PyErr_SetString(PyExc_FileNotFoundError, aStatus.message.c_str());
            throw pybind11::error_already_set();
        case piw::Code::Cancelled:
            PyErr_SetString(PyExc_KeyboardInterrupt, aStatus.message.c_str());
            throw pybind11::error_already_set();
        default:
            throw std::runtime_error(aStatus.message);
    }
}

template <size_t N>
bool optionalSizes(const pybind11::object& aValues, const char* aName, uint64_t (&aOut)[N])
{
    if(aValues.is_none()) return false;
    std::vector<uint64_t> vValues = aValues.cast<std::vector<uint64_t> >();
    if(vValues.size() != N) throw pybind11::value_error(std::string(aName) + " needs " + std::to_string(N) + " values");
    for(size_t i = 0; i < N; ++i) aOut[i] = vValues[i];
    return true;
}

// The whole command line (args without the program name); returns its exit code.
int pybind11_run_cli(const std::vector<std::string>& aArgs)
{
    return callOnWorker([&aArgs]() {
        std::vector<std::string> vArgs;
        vArgs.reserve(aArgs.size() + 1);
        vArgs.push_back("parallelimariswriter");
        vArgs.insert(vArgs.end(), aArgs.begin(), aArgs.end());
        std::vector<char*> vArgv;
        for(size_t i = 0; i < vArgs.size(); ++i) vArgv.push_back(&vArgs[i][0]);
        vArgv.push_back(nullptr);
        return piw::runCli(static_cast<int>(vArgs.size()), vArgv.data());
    }, nullptr);
}

// piw::convert with every field of piw::ConvertOptions; sizes, voxel sizes and the box along the
// input's axes 0, 1, 2 (Imaris X, Y, Z; with aXOnImarisX axis 1 is Imaris X and axis 0 Imaris Y).
// None for a size or the box: not given.
void pybind11_convert(const std::string& aReader, const std::vector<std::vector<std::string> >& aFiles,
                      const std::string& aOutputFile, const std::array<float, 3>& aVoxelSize,
                      const pybind11::object& aBlockSize, const pybind11::object& aBoundingBox,
                      const pybind11::object& aProcessBlock, int aNumThreads, bool aXOnImarisX,
                      const std::vector<std::string>& aChannelNames,
                      const std::vector<std::array<float, 3> >& aChannelColors,
                      const std::vector<float>& aEmissionWavelengths,
                      const std::vector<float>& aExcitationWavelengths,
                      double aTimeInterval, const std::string& aStartTime, bool aVerbose)
{
    piw::ConvertOptions vOptions;
    vOptions.reader = aReader;
    vOptions.files = aFiles;
    vOptions.outputFile = aOutputFile;
    for(int i = 0; i < 3; ++i) vOptions.voxelSize[i] = aVoxelSize[i];
    vOptions.hasBlockSize = optionalSizes(aBlockSize, "block_size", vOptions.blockSize);
    vOptions.hasBoundingBox = optionalSizes(aBoundingBox, "bounding_box", vOptions.boundingBox);
    vOptions.hasProcessBlock = optionalSizes(aProcessBlock, "process_block", vOptions.processBlock);
    vOptions.numThreads = aNumThreads;
    vOptions.xOnImarisX = aXOnImarisX;
    vOptions.channelNames = aChannelNames;
    for(size_t i = 0; i < aChannelColors.size(); ++i){
        piw::Color vColor = {aChannelColors[i][0], aChannelColors[i][1], aChannelColors[i][2]};
        vOptions.channelColors.push_back(vColor);
    }
    vOptions.emissionWavelengths = aEmissionWavelengths;
    vOptions.excitationWavelengths = aExcitationWavelengths;
    vOptions.timeInterval = aTimeInterval;
    vOptions.startTime = aStartTime;

    CallState vState;
    vState.verbose = aVerbose;
    const piw::Callbacks vCallbacks = callbacksFor(vState);
    piw::Status vStatus = callOnWorker([&]() { return piw::convert(vOptions, vCallbacks); }, &vState);
    throwIfFailed(vStatus);
}

std::vector<std::vector<std::string> > pybind11_find_input_files(const std::vector<std::string>& aFolders,
                                                                  const std::vector<std::string>& aPatterns)
{
    std::vector<std::vector<std::string> > vFiles;
    piw::Status vStatus;
    {
        pybind11::gil_scoped_release vRelease;
        vStatus = piw::findInputFiles(aFolders, aPatterns, vFiles);
    }
    throwIfFailed(vStatus);
    return vFiles;
}

}  // namespace

PYBIND11_MODULE(parallel_imaris_writer, m) {
    m.doc() = "Parallel Imaris Writer python bindings";

    m.def("pybind11_version", []() { return std::string(piw::version()); }, "The library's version");

    // On a worker thread, as a call's threads are: OpenMP's default for a new thread
    // (OMP_NUM_THREADS, or every CPU the process may run on).
    m.def("pybind11_default_threads", []() { return callOnWorker([]() { return piw::defaultThreads(); }, nullptr); },
          "The number of threads a call uses by default");

    m.def("pybind11_run_cli", &pybind11_run_cli, pybind11::arg("args"),
          "Run the command line (arguments without the program name); returns its exit code");

    m.def("pybind11_convert", &pybind11_convert, pybind11::arg("reader"), pybind11::arg("files"),
          pybind11::arg("output_file"), pybind11::arg("voxel_size"),
          pybind11::arg("block_size") = pybind11::none(), pybind11::arg("bounding_box") = pybind11::none(),
          pybind11::arg("process_block") = pybind11::none(), pybind11::arg("num_threads") = 0,
          pybind11::arg("x_on_imaris_x") = false,
          pybind11::arg("channel_names") = std::vector<std::string>(),
          pybind11::arg("channel_colors") = std::vector<std::array<float, 3> >(),
          pybind11::arg("emission_wavelengths") = std::vector<float>(),
          pybind11::arg("excitation_wavelengths") = std::vector<float>(),
          pybind11::arg("time_interval") = 0.0, pybind11::arg("start_time") = std::string(),
          pybind11::arg("verbose") = false,
          "Convert files[channel][timepoint] to one .ims (piw::convert; sizes along the input's axes 0, 1, 2)");

    m.def("pybind11_find_input_files", &pybind11_find_input_files, pybind11::arg("folders"), pybind11::arg("patterns"),
          "The command line's input files: files[channel][timepoint]");
}
