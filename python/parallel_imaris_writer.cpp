// Python bindings of the Parallel Imaris Writer library (src/parallelimariswriter.h). The
// functions are named pybind11_*; the Python API is parallel_imaris_writer_wrappers.py.
//
// The cluster steps (src/parallel.h: plan, task, status, finish) are bound as they are: each a
// single-node call on the same worker thread, the job system being the caller's.
//
// Every call into the library runs on a worker thread of its own while the calling thread waits
// without the GIL (callOnWorker): other Python threads keep running, Ctrl-C (or an exception
// raised by a progress callback) stops a write, and the OpenMP threads belong to a thread that
// ends with the call, so a call's thread count changes no other thread's OpenMP setting
// (measured: a child made by fork() after a conversion on the calling thread hung in its first
// conversion; after conversions on worker threads it ran). No Python call ever happens on a
// worker thread: the library's callbacks only store into the call's CallState, and the waiting
// thread passes that on to Python.
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#if defined(_WIN32)
#include <thread>
#else
#include <pthread.h>
#endif
#include "parallelimariswriter.h"
#include "parallel.h"

namespace {

// What a call's worker shares with the waiting thread. The library's callbacks run on the worker
// (and on the Imaris writer's thread), so they only touch this; the waiting thread, holding the
// GIL, passes it on to Python.
struct CallState {
    std::atomic<bool> cancel{false};
    bool verbose = false;
    std::mutex textMutex;
    std::string text;                              // the command line's messages, not printed yet (verbose)
    std::atomic<float> fraction{-1.0f};            // the library's last progress (-1: none yet)
    std::atomic<unsigned long long> bytes{0};
    pybind11::object progress;                     // unset or None, or progress(fraction, bytes_written)
    float reported = -1.0f;                        // the last fraction progress was called with
    std::chrono::steady_clock::time_point reportedAt;
    bool hasProgress() const { return progress && !progress.is_none(); }
};

void messageCallback(const char* aText, void* aUser)
{
    CallState* vState = static_cast<CallState*>(aUser);
    if(!vState->verbose) return;
    std::lock_guard<std::mutex> vLock(vState->textMutex);
    vState->text += aText;
}

void progressCallback(float aFraction, unsigned long long aBytes, void* aUser)
{
    CallState* vState = static_cast<CallState*>(aUser);
    vState->bytes = aBytes;
    vState->fraction = aFraction;
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
    if(aState.hasProgress()) vCallbacks.progress = progressCallback;
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

// Calls the progress callback (GIL held) when the library's fraction grew, at most every 100 ms.
// Values below 1 only: aFinal (the call has succeeded) calls it once with 1.
void reportProgress(CallState& aState, bool aFinal)
{
    if(!aState.hasProgress() || aState.reported >= 1.0f) return;
    const std::chrono::steady_clock::time_point vNow = std::chrono::steady_clock::now();
    float vFraction = 1.0f;
    if(!aFinal){
        vFraction = aState.fraction.load();
        if(!(vFraction > aState.reported) || vFraction >= 1.0f) return;
        if(aState.reported >= 0.0f && vNow - aState.reportedAt < std::chrono::milliseconds(100)) return;
    }
    aState.reported = vFraction;
    aState.reportedAt = vNow;
    aState.progress(vFraction, aState.bytes.load());
}

// One call's worker thread: runs the work, keeps its result (or what it threw) and signals the
// end. On Linux and macOS it gets an 8 MiB stack, as the command line's main thread has (macOS
// gives other threads 512 KiB).
template <class Result>
class Worker {
public:
    explicit Worker(std::function<Result()> aWork) : mWork(std::move(aWork)) {}
    void start(){
        #if defined(_WIN32)
        mThread = std::thread(&Worker::body, this);
        #else
        pthread_attr_t vAttr;
        pthread_attr_init(&vAttr);
        pthread_attr_setstacksize(&vAttr, (size_t)8 << 20);
        const int vError = pthread_create(&mThread, &vAttr, &Worker::run, this);
        pthread_attr_destroy(&vAttr);
        if(vError) throw std::runtime_error("Could not start a thread for the call.");
        #endif
    }
    // True when the work has ended (waits at most aTime).
    bool waitFor(std::chrono::milliseconds aTime){
        std::unique_lock<std::mutex> vLock(mMutex);
        return mCondition.wait_for(vLock, aTime, [this]() { return mDone; });
    }
    void join(){
        #if defined(_WIN32)
        mThread.join();
        #else
        pthread_join(mThread, NULL);
        #endif
    }
    // After join: the work's result, or what it threw.
    Result result(){
        if(mError) std::rethrow_exception(mError);
        return std::move(mResult);
    }
private:
    #if !defined(_WIN32)
    static void* run(void* aSelf){ static_cast<Worker*>(aSelf)->body(); return NULL; }
    #endif
    void body(){
        try{ mResult = mWork(); }
        catch(...){ mError = std::current_exception(); }
        std::lock_guard<std::mutex> vLock(mMutex);
        mDone = true;
        mCondition.notify_all();
    }
    std::function<Result()> mWork;
    Result mResult;
    std::exception_ptr mError;
    std::mutex mMutex;
    std::condition_variable mCondition;
    bool mDone = false;
    #if defined(_WIN32)
    std::thread mThread;
    #else
    pthread_t mThread;
    #endif
};

// Runs aWork() on a new thread and returns its result; the calling thread (GIL held on entry and
// on return) waits without the GIL. With aState, the waiting thread wakes every 50 ms to look for
// a signal (Ctrl-C), print the queued messages and call the progress callback; a Python error
// from any of these asks the work to stop (aState->cancel), and once the work has ended that
// first error is raised (later ones, such as a second Ctrl-C, are dropped). Without aState the
// work cannot be stopped, and a signal is handled by Python once the call has returned.
template <class Work>
auto callOnWorker(Work aWork, CallState* aState) -> decltype(aWork())
{
    typedef decltype(aWork()) Result;
    Worker<Result> vWorker{std::function<Result()>(aWork)};
    vWorker.start();
    std::exception_ptr vError;
    for(bool vDone = false; !vDone; ){
        {
            pybind11::gil_scoped_release vRelease;
            vDone = vWorker.waitFor(std::chrono::milliseconds(50));
        }
        if(!aState) continue;
        try{
            if(PyErr_CheckSignals() != 0) throw pybind11::error_already_set();
            if(aState->verbose) printQueued(*aState);
            if(!vError) reportProgress(*aState, false);
        }
        catch(...){
            if(!vError) vError = std::current_exception();
            aState->cancel = true;
        }
    }
    vWorker.join();
    if(vError) std::rethrow_exception(vError);
    return vWorker.result();   // rethrows what the work threw
}

// Text from the library (file names, attribute values) as str: UTF-8, other bytes kept as
// surrogates (as os.fsdecode does).
pybind11::str text(const std::string& aText)
{
    PyObject* vText = PyUnicode_DecodeUTF8(aText.data(), (Py_ssize_t)aText.size(), "surrogateescape");
    if(!vText) throw pybind11::error_already_set();
    return pybind11::reinterpret_steal<pybind11::str>(vText);
}

[[noreturn]] void raise(PyObject* aType, const std::string& aMessage)
{
    PyObject* vText = PyUnicode_DecodeUTF8(aMessage.data(), (Py_ssize_t)aMessage.size(), "replace");
    if(vText){
        PyErr_SetObject(aType, vText);
        Py_DECREF(vText);
    }
    throw pybind11::error_already_set();
}

// A failed Status as the Python exception for it.
void throwIfFailed(const piw::Status& aStatus)
{
    switch(aStatus.code){
        case piw::Code::Ok:
            return;
        case piw::Code::InvalidArgument:
        case piw::Code::Unsupported:
            raise(PyExc_ValueError, aStatus.message);
        case piw::Code::NotFound:
            raise(PyExc_FileNotFoundError, aStatus.message);
        case piw::Code::Cancelled:
            // (a cancel the waiting thread asked for raises its own error before this)
            raise(PyExc_KeyboardInterrupt, aStatus.message);
        default:
            raise(PyExc_RuntimeError, aStatus.message);
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

const char* dataTypeName(piw::DataType aType)
{
    switch(aType){
        case piw::DataType::UInt8: return "uint8";
        case piw::DataType::UInt16: return "uint16";
        case piw::DataType::UInt32: return "uint32";
        case piw::DataType::Float32: return "float32";
    }
    return "unknown";
}

// The library's sample type of a NumPy data type in this machine's byte order (false: none).
bool dataTypeOf(const pybind11::dtype& aType, piw::DataType& aOut)
{
    if(!aType.attr("isnative").cast<bool>()) return false;
    const char vKind = aType.kind();
    const pybind11::ssize_t vSize = aType.itemsize();
    if(vKind == 'u' && vSize == 1) aOut = piw::DataType::UInt8;
    else if(vKind == 'u' && vSize == 2) aOut = piw::DataType::UInt16;
    else if(vKind == 'u' && vSize == 4) aOut = piw::DataType::UInt32;
    else if(vKind == 'f' && vSize == 4) aOut = piw::DataType::Float32;
    else return false;
    return true;
}

// The element strides of a 3-D array along Imaris X, Y, Z (its axes 0, 1, 2), checked for the
// library: positive multiples of the element size, aligned data (an axis of one element may have
// any stride).
void elementStrides(const pybind11::array& aArray, const char* aName, int64_t aStride[3])
{
    const pybind11::ssize_t vSize = aArray.itemsize();
    if(reinterpret_cast<uintptr_t>(aArray.data()) % (uintptr_t)vSize != 0){
        throw pybind11::value_error(std::string(aName) + " is not aligned to its element size");
    }
    for(int i = 0; i < 3; ++i){
        const pybind11::ssize_t vStride = aArray.strides(i);
        if(aArray.shape(i) == 1){ aStride[i] = 1; continue; }
        if(vStride <= 0 || vStride % vSize != 0){
            throw pybind11::value_error(std::string(aName) + " needs positive strides that are multiples of its element size");
        }
        aStride[i] = (int64_t)(vStride / vSize);
    }
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

// The metadata options of piw::ConvertOptions and piw::WriteOptions (same names).
template <class Options>
void setMetadata(Options& aOptions, const std::vector<std::string>& aChannelNames,
                 const std::vector<std::array<float, 3> >& aChannelColors,
                 const std::vector<float>& aEmissionWavelengths, const std::vector<float>& aExcitationWavelengths,
                 double aTimeInterval, const std::string& aStartTime)
{
    aOptions.channelNames = aChannelNames;
    for(size_t i = 0; i < aChannelColors.size(); ++i){
        piw::Color vColor = {aChannelColors[i][0], aChannelColors[i][1], aChannelColors[i][2]};
        aOptions.channelColors.push_back(vColor);
    }
    aOptions.emissionWavelengths = aEmissionWavelengths;
    aOptions.excitationWavelengths = aExcitationWavelengths;
    aOptions.timeInterval = aTimeInterval;
    aOptions.startTime = aStartTime;
}

// piw::convert with every field of piw::ConvertOptions; sizes, voxel sizes and the box along the
// input's axes 0, 1, 2: the image's (y, x, z) with aInputAxes "yxz", its (z, y, x) with "zyx"
// (Zarr only). Imaris X, Y, Z are the image's y, x, z (x, y, z with aXOnImarisX). None for a size
// or the box: not given.
void pybind11_convert(const std::string& aReader, const std::vector<std::vector<std::string> >& aFiles,
                      const std::string& aOutputFile, const std::array<float, 3>& aVoxelSize,
                      const pybind11::object& aBlockSize, const pybind11::object& aBoundingBox,
                      const pybind11::object& aProcessBlock, int aNumThreads, bool aXOnImarisX,
                      const std::string& aInputAxes,
                      const std::vector<std::string>& aChannelNames,
                      const std::vector<std::array<float, 3> >& aChannelColors,
                      const std::vector<float>& aEmissionWavelengths,
                      const std::vector<float>& aExcitationWavelengths,
                      double aTimeInterval, const std::string& aStartTime, bool aVerbose,
                      const pybind11::object& aProgress)
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
    if(aInputAxes == "zyx") vOptions.inputAxes = piw::InputAxes::ZYX;
    else if(aInputAxes != "yxz"){
        throw pybind11::value_error("input_axes must be 'yxz' or 'zyx', not '" + aInputAxes + "'");
    }
    setMetadata(vOptions, aChannelNames, aChannelColors, aEmissionWavelengths, aExcitationWavelengths,
                aTimeInterval, aStartTime);

    CallState vState;
    vState.verbose = aVerbose;
    vState.progress = aProgress;
    const piw::Callbacks vCallbacks = callbacksFor(vState);
    piw::Status vStatus = callOnWorker([&]() { return piw::convert(vOptions, vCallbacks); }, &vState);
    throwIfFailed(vStatus);
    reportProgress(vState, true);
}

// piw::writeVolumes: aVolumes[channel*timepoints + timepoint] are 3-D arrays along Imaris X, Y, Z
// (any positive strides; the wrapper gives views of the caller's arrays), all of one shape and
// type; they are read in place.
void pybind11_write_ims(const std::string& aOutputFile, const std::vector<pybind11::array>& aVolumes,
                        unsigned aChannels, unsigned aTimepoints, const std::array<float, 3>& aVoxelSize,
                        const pybind11::object& aBlockSize, int aNumThreads,
                        const std::vector<std::string>& aChannelNames,
                        const std::vector<std::array<float, 3> >& aChannelColors,
                        const std::vector<float>& aEmissionWavelengths,
                        const std::vector<float>& aExcitationWavelengths,
                        double aTimeInterval, const std::string& aStartTime, bool aVerbose,
                        const pybind11::object& aProgress)
{
    if(aVolumes.empty()) throw pybind11::value_error("No volumes were given");
    piw::WriteOptions vOptions;
    vOptions.outputFile = aOutputFile;
    std::vector<piw::Volume> vVolumes(aVolumes.size());
    for(size_t vI = 0; vI < aVolumes.size(); ++vI){
        const pybind11::array& vArray = aVolumes[vI];
        const std::string vName = "volume " + std::to_string(vI);
        if(vArray.ndim() != 3) throw pybind11::value_error(vName + " is not 3-D");
        piw::DataType vType;
        if(!dataTypeOf(vArray.dtype(), vType)){
            throw pybind11::type_error(vName + " is not uint8, uint16, uint32 or float32 in this machine's byte order");
        }
        if(!vI) vOptions.dataType = vType;
        else if(vType != vOptions.dataType) throw pybind11::type_error("The volumes have different data types");
        for(int i = 0; i < 3; ++i){
            const uint64_t vSize = (uint64_t)vArray.shape(i);
            if(!vI) vOptions.size[i] = vSize;
            else if(vSize != vOptions.size[i]) throw pybind11::value_error("The volumes have different shapes");
        }
        elementStrides(vArray, vName.c_str(), vVolumes[vI].stride);
        vVolumes[vI].data = vArray.data();
    }
    vOptions.channels = aChannels;
    vOptions.timepoints = aTimepoints;
    for(int i = 0; i < 3; ++i) vOptions.voxelSize[i] = aVoxelSize[i];
    vOptions.hasBlockSize = optionalSizes(aBlockSize, "block_size", vOptions.blockSize);
    vOptions.numThreads = aNumThreads;
    setMetadata(vOptions, aChannelNames, aChannelColors, aEmissionWavelengths, aExcitationWavelengths,
                aTimeInterval, aStartTime);

    CallState vState;
    vState.verbose = aVerbose;
    vState.progress = aProgress;
    const piw::Callbacks vCallbacks = callbacksFor(vState);
    piw::Status vStatus = callOnWorker([&]() { return piw::writeVolumes(vOptions, vVolumes, vCallbacks); }, &vState);
    throwIfFailed(vStatus);
    reportProgress(vState, true);
}

// piw::readIms of the region [aStart, aEnd) (Imaris X, Y, Z) into aOut, a writeable 3-D array of
// the region's shape along X, Y, Z (any positive strides) and the file's type.
void pybind11_read_ims(const std::string& aFile, unsigned aLevel, unsigned aTimepoint, unsigned aChannel,
                       const std::array<uint64_t, 3>& aStart, const std::array<uint64_t, 3>& aEnd,
                       pybind11::array aOut, int aNumThreads)
{
    if(aOut.ndim() != 3) throw pybind11::value_error("out is not 3-D");
    if(!aOut.writeable()) throw pybind11::value_error("out is not writeable");
    piw::DataType vType;
    if(!dataTypeOf(aOut.dtype(), vType)){
        throw pybind11::type_error("out is not uint8, uint16, uint32 or float32 in this machine's byte order");
    }
    for(int i = 0; i < 3; ++i){
        if(aEnd[i] > aStart[i] && (uint64_t)aOut.shape(i) != aEnd[i] - aStart[i]){
            throw pybind11::value_error("out does not have the region's shape");
        }
    }
    int64_t vStride[3];
    elementStrides(aOut, "out", vStride);
    void* vOut = aOut.mutable_data();
    CallState vState;
    piw::Status vStatus = callOnWorker([&]() {
        return piw::readIms(aFile, aLevel, aTimepoint, aChannel, aStart.data(), aEnd.data(), vType, vOut, vStride,
                            aNumThreads);
    }, &vState);
    throwIfFailed(vStatus);
}

pybind11::tuple triple(const uint64_t aValues[3])
{
    return pybind11::make_tuple(aValues[0], aValues[1], aValues[2]);
}

pybind11::tuple triple(const float aValues[3])
{
    return pybind11::make_tuple(aValues[0], aValues[1], aValues[2]);
}

// piw::imsInfo as a dict; sizes and extents along Imaris X, Y, Z; CRCs None without aChecksums.
pybind11::dict pybind11_ims_info(const std::string& aFile, bool aChecksums)
{
    CallState vState;
    std::pair<piw::Status, piw::ImsInfo> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::ImsInfo> vInfo;
        vInfo.first = piw::imsInfo(aFile, vInfo.second, aChecksums);
        return vInfo;
    }, &vState);
    throwIfFailed(vResult.first);
    const piw::ImsInfo& vInfo = vResult.second;
    pybind11::object vNone = pybind11::none();
    pybind11::dict vDict;
    vDict["size"] = triple(vInfo.size);
    vDict["channels"] = vInfo.channels;
    vDict["timepoints"] = vInfo.timepoints;
    vDict["levels"] = vInfo.levels;
    vDict["data_type"] = dataTypeName(vInfo.dataType);
    vDict["extent_min"] = triple(vInfo.extentMin);
    vDict["extent_max"] = triple(vInfo.extentMax);
    vDict["voxel_size"] = triple(vInfo.voxelSize);
    pybind11::list vNames, vColors, vTimes, vDatasets, vAttributes;
    for(size_t i = 0; i < vInfo.channelNames.size(); ++i) vNames.append(text(vInfo.channelNames[i]));
    for(size_t i = 0; i < vInfo.channelColors.size(); ++i) vColors.append(text(vInfo.channelColors[i]));
    for(size_t i = 0; i < vInfo.timePoints.size(); ++i) vTimes.append(text(vInfo.timePoints[i]));
    vDict["channel_names"] = vNames;
    vDict["channel_colors"] = vColors;
    vDict["time_points"] = vTimes;
    vDict["thumbnail_crc"] = aChecksums ? pybind11::object(pybind11::int_(vInfo.thumbnailCrc)) : vNone;
    for(size_t i = 0; i < vInfo.datasets.size(); ++i){
        const piw::ImsDataset& vSet = vInfo.datasets[i];
        pybind11::dict vEntry;
        vEntry["level"] = vSet.level;
        vEntry["timepoint"] = vSet.timepoint;
        vEntry["channel"] = vSet.channel;
        vEntry["size"] = triple(vSet.size);
        vEntry["chunk"] = triple(vSet.chunk);
        vEntry["chunks"] = vSet.chunks;
        vEntry["crc"] = aChecksums ? pybind11::object(pybind11::int_(vSet.crc)) : vNone;
        vEntry["histogram_crc"] = aChecksums ? pybind11::object(pybind11::int_(vSet.histogramCrc)) : vNone;
        vDatasets.append(vEntry);
    }
    vDict["datasets"] = vDatasets;
    for(size_t i = 0; i < vInfo.attributes.size(); ++i){
        vAttributes.append(pybind11::make_tuple(text(vInfo.attributes[i].first), text(vInfo.attributes[i].second)));
    }
    vDict["attributes"] = vAttributes;
    return vDict;
}

// piw::imsDataset: (size along Imaris X, Y, Z, type name) of one dataset.
pybind11::tuple pybind11_ims_dataset(const std::string& aFile, unsigned aLevel, unsigned aTimepoint, unsigned aChannel)
{
    uint64_t vSize[3] = {0, 0, 0};
    piw::DataType vType = piw::DataType::UInt16;
    CallState vState;
    piw::Status vStatus = callOnWorker([&]() {
        return piw::imsDataset(aFile, aLevel, aTimepoint, aChannel, vSize, vType);
    }, &vState);
    throwIfFailed(vStatus);
    return pybind11::make_tuple(triple(vSize), dataTypeName(vType));
}

// piw::findInputFiles: files[channel][timepoint] as str (names that are not UTF-8 with surrogates,
// as os.fsdecode gives them). aReader "tiff", "zarr", or "" for both (not both for one pattern).
pybind11::list pybind11_find_input_files(const std::vector<std::string>& aFolders,
                                         const std::vector<std::string>& aPatterns, const std::string& aReader)
{
    std::vector<std::vector<std::string> > vFiles;
    piw::Status vStatus;
    {
        pybind11::gil_scoped_release vRelease;
        vStatus = piw::findInputFiles(aFolders, aPatterns, aReader, vFiles);
    }
    throwIfFailed(vStatus);
    pybind11::list vChannels;
    for(size_t vC = 0; vC < vFiles.size(); ++vC){
        pybind11::list vChannel;
        for(size_t vT = 0; vT < vFiles[vC].size(); ++vT) vChannel.append(text(vFiles[vC][vT]));
        vChannels.append(vChannel);
    }
    return vChannels;
}

// ---- the cluster steps (src/parallel.h)

pybind11::list textList(const std::vector<std::string>& aValues)
{
    pybind11::list vList;
    for(size_t i = 0; i < aValues.size(); ++i) vList.append(text(aValues[i]));
    return vList;
}

// A plan as a dict: its size along Imaris X, Y, Z; input_axes and x_on_imaris_x those of a
// time-series plan's conversion.
pybind11::dict planDict(const piw::ClusterPlan& aPlan)
{
    pybind11::dict vDict;
    vDict["workdir"] = text(aPlan.workdir);
    vDict["kind"] = aPlan.kind;
    vDict["store"] = aPlan.store;
    vDict["output_file"] = text(aPlan.outputFile);
    vDict["timepoints"] = aPlan.timepoints;
    vDict["channels"] = aPlan.channels;
    vDict["levels"] = aPlan.levels;
    vDict["size"] = triple(aPlan.size);
    vDict["data_type"] = dataTypeName(aPlan.dataType);
    pybind11::list vStages;
    for(size_t i = 0; i < aPlan.stages.size(); ++i){
        pybind11::dict vStage;
        vStage["name"] = aPlan.stages[i].name;
        vStage["tasks"] = aPlan.stages[i].tasks;
        vStage["after"] = aPlan.stages[i].after.empty() ? pybind11::object(pybind11::none())
                                                        : pybind11::object(pybind11::str(aPlan.stages[i].after));
        vStages.append(vStage);
    }
    vDict["stages"] = vStages;
    vDict["input_axes"] = aPlan.inputAxes == piw::InputAxes::ZYX ? "zyx" : "yxz";
    vDict["x_on_imaris_x"] = aPlan.xOnImarisX;
    return vDict;
}

// piw::clusterPlan: a time-series plan, or with aTileSize (Imaris X, Y, Z) a tile plan. The
// conversion's values along the input's axes 0, 1, 2, as pybind11_convert takes them; aFiles the
// inputs (found by the caller).
pybind11::dict pybind11_cluster_plan(const std::string& aWorkdir, const std::string& aReader,
                                     const std::vector<std::vector<std::string> >& aFiles,
                                     const std::string& aOutputFile, const std::array<float, 3>& aVoxelSize,
                                     const pybind11::object& aBlockSize, const pybind11::object& aBoundingBox,
                                     const pybind11::object& aProcessBlock, bool aXOnImarisX,
                                     const std::string& aInputAxes, const pybind11::object& aTileSize,
                                     const std::string& aStore, long aTilesPerTask, uint64_t aChunkBytes,
                                     const std::vector<std::string>& aChannelNames,
                                     const std::vector<std::array<float, 3> >& aChannelColors,
                                     const std::vector<float>& aEmissionWavelengths,
                                     const std::vector<float>& aExcitationWavelengths,
                                     double aTimeInterval, const std::string& aStartTime)
{
    piw::ClusterPlanOptions vOptions;
    vOptions.workdir = aWorkdir;
    piw::ConvertOptions& vConvert = vOptions.convert;
    vConvert.reader = aReader;
    vConvert.files = aFiles;
    vConvert.outputFile = aOutputFile;
    for(int i = 0; i < 3; ++i) vConvert.voxelSize[i] = aVoxelSize[i];
    vConvert.hasBlockSize = optionalSizes(aBlockSize, "block_size", vConvert.blockSize);
    vConvert.hasBoundingBox = optionalSizes(aBoundingBox, "bounding_box", vConvert.boundingBox);
    vConvert.hasProcessBlock = optionalSizes(aProcessBlock, "process_block", vConvert.processBlock);
    vConvert.xOnImarisX = aXOnImarisX;
    if(aInputAxes == "zyx") vConvert.inputAxes = piw::InputAxes::ZYX;
    else if(aInputAxes != "yxz"){
        throw pybind11::value_error("input_axes must be 'yxz' or 'zyx', not '" + aInputAxes + "'");
    }
    setMetadata(vConvert, aChannelNames, aChannelColors, aEmissionWavelengths, aExcitationWavelengths,
                aTimeInterval, aStartTime);
    if(optionalSizes(aTileSize, "tile_size", vOptions.tileSize)) vOptions.kind = "tiles";
    vOptions.store = aStore;
    vOptions.tilesPerTask = aTilesPerTask;
    vOptions.chunkBytes = aChunkBytes;

    CallState vState;
    std::pair<piw::Status, piw::ClusterPlan> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::ClusterPlan> vPlan;
        vPlan.first = piw::clusterPlan(vOptions, &vPlan.second);
        return vPlan;
    }, &vState);
    throwIfFailed(vResult.first);
    return planDict(vResult.second);
}

// piw::clusterReadPlan: the plan of a workdir as pybind11_cluster_plan returns it.
pybind11::dict pybind11_cluster_read_plan(const std::string& aWorkdir)
{
    CallState vState;
    std::pair<piw::Status, piw::ClusterPlan> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::ClusterPlan> vPlan;
        vPlan.first = piw::clusterReadPlan(aWorkdir, vPlan.second);
        return vPlan;
    }, &vState);
    throwIfFailed(vResult.first);
    return planDict(vResult.second);
}

// piw::clusterTask: task aIndex of the stage (its name or number). Ctrl-C (or an exception from
// progress) stops a time-series task between blocks (its staged part removed); a tile task ends
// its tiles first. aProcessBlock along the input's axes (time series), or None for the plan's.
pybind11::dict pybind11_cluster_task(const std::string& aWorkdir, const std::string& aStage, long aIndex,
                                     int aNumThreads, const std::string& aStageDir,
                                     const pybind11::object& aProcessBlock, bool aVerbose,
                                     const pybind11::object& aProgress)
{
    piw::ClusterTaskOptions vOptions;
    vOptions.numThreads = aNumThreads;
    vOptions.stageDir = aStageDir;
    vOptions.hasProcessBlock = optionalSizes(aProcessBlock, "process_block", vOptions.processBlock);
    CallState vState;
    vState.verbose = aVerbose;
    vState.progress = aProgress;
    vOptions.callbacks = callbacksFor(vState);
    std::pair<piw::Status, piw::ClusterTaskResult> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::ClusterTaskResult> vTask;
        vTask.first = piw::clusterTask(aWorkdir, aStage, aIndex, vOptions, &vTask.second);
        return vTask;
    }, &vState);
    if(aVerbose) printQueued(vState);
    throwIfFailed(vResult.first);
    reportProgress(vState, true);
    const piw::ClusterTaskResult& vTask = vResult.second;
    pybind11::object vNone = pybind11::none();
    const bool vSeries = vTask.timepoint >= 0;
    pybind11::dict vDict;
    vDict["stage"] = vTask.stage;
    vDict["index"] = vTask.index;
    vDict["skipped"] = vTask.skipped;
    vDict["owners"] = textList(vTask.owners);
    vDict["timepoint"] = vSeries ? pybind11::object(pybind11::int_(vTask.timepoint)) : vNone;
    vDict["channel"] = vSeries ? pybind11::object(pybind11::int_(vTask.channel)) : vNone;
    vDict["memory_stage"] = vTask.memoryStage;
    vDict["chunks"] = vTask.place.chunks;
    vDict["bytes"] = vTask.place.bytes;
    vDict["convert_seconds"] = vTask.convertSeconds;
    vDict["place_seconds"] = vTask.placeSeconds;
    vDict["total_seconds"] = vTask.totalSeconds;
    vDict["lines"] = textList(vTask.lines);
    return vDict;
}

// piw::clusterStatus as a dict: per stage the task indices not done.
pybind11::dict pybind11_cluster_status(const std::string& aWorkdir)
{
    CallState vState;
    std::pair<piw::Status, piw::ClusterStatus> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::ClusterStatus> vStatus;
        vStatus.first = piw::clusterStatus(aWorkdir, vStatus.second);
        return vStatus;
    }, &vState);
    throwIfFailed(vResult.first);
    const piw::ClusterStatus& vStatus = vResult.second;
    pybind11::dict vDict;
    vDict["kind"] = vStatus.kind;
    vDict["store"] = vStatus.store;
    pybind11::list vStages;
    for(size_t i = 0; i < vStatus.stages.size(); ++i){
        pybind11::dict vStage;
        vStage["name"] = vStatus.stages[i].name;
        vStage["tasks"] = vStatus.stages[i].tasks;
        vStage["missing"] = vStatus.stages[i].missing;
        vStages.append(vStage);
    }
    vDict["stages"] = vStages;
    vDict["sealed"] = vStatus.sealed;
    vDict["finished"] = vStatus.finished;
    vDict["finished_file"] = vStatus.finished ? pybind11::object(text(vStatus.finishedFile))
                                              : pybind11::object(pybind11::none());
    return vDict;
}

// piw::clusterFinish: FileNotFoundError naming the tasks not done; else the output's dict.
pybind11::dict pybind11_cluster_finish(const std::string& aWorkdir, const std::string& aOutputFile, bool aVerify,
                                       int aNumThreads)
{
    piw::ClusterFinishOptions vOptions;
    vOptions.verify = aVerify;
    vOptions.numThreads = aNumThreads;
    CallState vState;
    std::pair<piw::Status, piw::MonoFinishResult> vResult = callOnWorker([&]() {
        std::pair<piw::Status, piw::MonoFinishResult> vFinish;
        vFinish.first = piw::clusterFinish(aWorkdir, aOutputFile, vOptions, &vFinish.second);
        return vFinish;
    }, &vState);
    throwIfFailed(vResult.first);
    const piw::MonoFinishResult& vFinish = vResult.second;
    pybind11::dict vDict;
    vDict["output_file"] = text(vFinish.outputFile);
    vDict["already_finished"] = vFinish.alreadyFinished;
    vDict["timepoints"] = vFinish.timepoints;
    vDict["channels"] = vFinish.channels;
    vDict["levels"] = vFinish.levels;
    vDict["chunks"] = vFinish.chunks;
    vDict["file_bytes"] = vFinish.fileBytes;
    vDict["total_seconds"] = vFinish.totalSeconds;
    return vDict;
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
          pybind11::arg("x_on_imaris_x") = false, pybind11::arg("input_axes") = std::string("yxz"),
          pybind11::arg("channel_names") = std::vector<std::string>(),
          pybind11::arg("channel_colors") = std::vector<std::array<float, 3> >(),
          pybind11::arg("emission_wavelengths") = std::vector<float>(),
          pybind11::arg("excitation_wavelengths") = std::vector<float>(),
          pybind11::arg("time_interval") = 0.0, pybind11::arg("start_time") = std::string(),
          pybind11::arg("verbose") = false, pybind11::arg("progress") = pybind11::none(),
          "Convert files[channel][timepoint] to one .ims (piw::convert; sizes along the input's axes 0, 1, 2)");

    m.def("pybind11_write_ims", &pybind11_write_ims, pybind11::arg("output_file"), pybind11::arg("volumes"),
          pybind11::arg("channels"), pybind11::arg("timepoints"), pybind11::arg("voxel_size"),
          pybind11::arg("block_size") = pybind11::none(), pybind11::arg("num_threads") = 0,
          pybind11::arg("channel_names") = std::vector<std::string>(),
          pybind11::arg("channel_colors") = std::vector<std::array<float, 3> >(),
          pybind11::arg("emission_wavelengths") = std::vector<float>(),
          pybind11::arg("excitation_wavelengths") = std::vector<float>(),
          pybind11::arg("time_interval") = 0.0, pybind11::arg("start_time") = std::string(),
          pybind11::arg("verbose") = false, pybind11::arg("progress") = pybind11::none(),
          "Write volumes[channel*timepoints + timepoint] (3-D arrays along Imaris X, Y, Z) to one .ims "
          "(piw::writeVolumes)");

    m.def("pybind11_read_ims", &pybind11_read_ims, pybind11::arg("file"), pybind11::arg("level"),
          pybind11::arg("timepoint"), pybind11::arg("channel"), pybind11::arg("start"), pybind11::arg("end"),
          pybind11::arg("out"), pybind11::arg("num_threads") = 0,
          "Read the region [start, end) (Imaris X, Y, Z) of one dataset into out, viewed along X, Y, Z "
          "(piw::readIms)");

    m.def("pybind11_ims_info", &pybind11_ims_info, pybind11::arg("file"), pybind11::arg("checksums") = false,
          "The sizes, datasets and attributes of an .ims file, along Imaris X, Y, Z (piw::imsInfo)");

    m.def("pybind11_ims_dataset", &pybind11_ims_dataset, pybind11::arg("file"), pybind11::arg("level"),
          pybind11::arg("timepoint"), pybind11::arg("channel"),
          "(size along Imaris X, Y, Z, data type name) of one dataset, without listing chunks (piw::imsDataset)");

    m.def("pybind11_find_input_files", &pybind11_find_input_files, pybind11::arg("folders"), pybind11::arg("patterns"),
          pybind11::arg("reader") = std::string(),
          "The command line's input files: files[channel][timepoint] (reader 'tiff', 'zarr' or '' for both)");

    m.def("pybind11_cluster_plan", &pybind11_cluster_plan, pybind11::arg("workdir"), pybind11::arg("reader"),
          pybind11::arg("files"), pybind11::arg("output_file"), pybind11::arg("voxel_size"),
          pybind11::arg("block_size") = pybind11::none(), pybind11::arg("bounding_box") = pybind11::none(),
          pybind11::arg("process_block") = pybind11::none(), pybind11::arg("x_on_imaris_x") = false,
          pybind11::arg("input_axes") = std::string("yxz"), pybind11::arg("tile_size") = pybind11::none(),
          pybind11::arg("store") = std::string(), pybind11::arg("tiles_per_task") = 1L,
          pybind11::arg("chunk_bytes") = (uint64_t)0,
          pybind11::arg("channel_names") = std::vector<std::string>(),
          pybind11::arg("channel_colors") = std::vector<std::array<float, 3> >(),
          pybind11::arg("emission_wavelengths") = std::vector<float>(),
          pybind11::arg("excitation_wavelengths") = std::vector<float>(),
          pybind11::arg("time_interval") = 0.0, pybind11::arg("start_time") = std::string(),
          "Plan files[channel][timepoint] as stages of independent tasks in workdir (piw::clusterPlan; a tile "
          "plan with tile_size, Imaris X, Y, Z)");

    m.def("pybind11_cluster_read_plan", &pybind11_cluster_read_plan, pybind11::arg("workdir"),
          "The plan of a workdir (piw::clusterReadPlan)");

    m.def("pybind11_cluster_task", &pybind11_cluster_task, pybind11::arg("workdir"), pybind11::arg("stage"),
          pybind11::arg("index"), pybind11::arg("num_threads") = 0, pybind11::arg("stage_dir") = std::string(),
          pybind11::arg("process_block") = pybind11::none(), pybind11::arg("verbose") = false,
          pybind11::arg("progress") = pybind11::none(),
          "Run one task of a plan (piw::clusterTask; stage: its name or number)");

    m.def("pybind11_cluster_status", &pybind11_cluster_status, pybind11::arg("workdir"),
          "The tasks of a plan not done yet (piw::clusterStatus)");

    m.def("pybind11_cluster_finish", &pybind11_cluster_finish, pybind11::arg("workdir"),
          pybind11::arg("output_file") = std::string(), pybind11::arg("verify") = false,
          pybind11::arg("num_threads") = 0,
          "Write the plan's output once every task is done (piw::clusterFinish)");
}
