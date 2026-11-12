/***************************************************************************
 *   Copyright (c) 2020-present Bitplane AG Zuerich                        *
 *                                                                         *
 *   Licensed under the Apache License, Version 2.0 (the "License");       *
 *   you may not use this file except in compliance with the License.      *
 *   You may obtain a copy of the License at                               *
 *                                                                         *
 *       http://www.apache.org/licenses/LICENSE-2.0                        *
 *                                                                         *
 *   Unless required by applicable law or agreed to in writing, software   *
 *   distributed under the License is distributed on an "AS IS" BASIS,     *
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or imp   *
 *   See the License for the specific language governing permissions and   *
 *   limitations under the License.                                        *
 ***************************************************************************/

// The converter core: TIFF/Zarr volumes to one Imaris .ims through ImarisWriter's C interface.
// No printing and no exit here: the command line's text goes to Callbacks::message, errors come
// back as a Status, and src/cli.cpp turns both into the command line's output and exit codes.
// Include order as in the original main.cpp: cpp-tiff's helperfunctions.h must win over cpp-zarr's.
#include "bpImageConverterInterfaceC.h"
#include "helperfunctions.h"
#include "parallelreadtiff.h"
#include "zarr.h"
#include "parallelreadzarr.h"
#include "parallelimariswriter.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#include <omp.h>
#include <algorithm>
#include <atomic>
#include <climits>
#include <exception>
#include <mutex>
#include <thread>
#include <vector>

#ifndef PIW_VERSION
#define PIW_VERSION "unknown"
#endif

namespace piw {
namespace detail {
// The one Unsupported case the command line still reports on stdout with exit code 0.
extern const char* const kNo64BitData;
const char* const kNo64BitData = "64 bit data is not yet supported by the Imaris Converter";
}

namespace {

// Fill one Imaris block-sized buffer from the read-block data. Row-wise
// memcpy/memset instead of per-voxel index math + a per-voxel type switch --
// profiling showed the old per-voxel loop was ~1/4 of the whole conversion's CPU.
// src==NULL (a pure zero-pad read-block) yields all zeros. Indexing reduces to the
// whole-image indices when there is a single read-block (x0=y0=z0=0, xN=shapeX,
// yN=shapeY).
// No OpenMP here: an OpenMP region per block (thousands per volume) kept the idle
// workers spinning between regions for the whole conversion, on the cores that
// ImarisWriter's compression threads need. Blocks of 8 MiB or more are copied by
// threads that live only for this call; smaller ones on the caller's thread.
template<typename T>
static void fillBlock(T* buf, const T* src,
        uint64_t vX, uint64_t vY, uint64_t vZ,
        uint64_t bx, uint64_t by, uint64_t bz,
        uint64_t shapeX, uint64_t shapeY, uint64_t shapeZ,
        uint64_t x0, uint64_t y0, uint64_t z0, uint64_t xN, uint64_t yN){
    const uint64_t gxBase = vX*bx, gyBase = vY*by, gzBase = vZ*bz;
    const uint64_t avail  = (gxBase < shapeX) ? (shapeX - gxBase) : 0;
    const uint64_t xValid = !src ? 0 : (avail < bx ? avail : bx);   // in-image cols of this block
    const uint64_t nRows  = by*bz;
    auto fillRows = [&](uint64_t r0, uint64_t r1){                  // rows r = y + z*by
        for(uint64_t r = r0; r < r1; ++r){
            const uint64_t z = r / by, y = r % by;
            T* brow = buf + y*bx + z*bx*by;
            const uint64_t gy = gyBase + y, gz = gzBase + z;
            if(xValid == 0 || gy >= shapeY || gz >= shapeZ){
                memset(brow, 0, bx*sizeof(T));                       // row entirely outside the image
            } else {
                const T* srow = src + (gxBase - x0) + (gy - y0)*xN + (gz - z0)*xN*yN;
                memcpy(brow, srow, xValid*sizeof(T));               // contiguous in-image run
                if(xValid < bx) memset(brow + xValid, 0, (bx - xValid)*sizeof(T));  // zero-pad tail
            }
        }
    };
    const int nThreads = std::min(omp_get_max_threads(), 16);
    if(nRows*bx*sizeof(T) < (8ull << 20) || nThreads <= 1){
        fillRows(0, nRows);
        return;
    }
    const uint64_t rowsPerChunk = 64;
    std::atomic<uint64_t> next(0);
    auto worker = [&](){
        for(;;){
            const uint64_t r0 = next.fetch_add(rowsPerChunk);
            if(r0 >= nRows) break;
            fillRows(r0, std::min(r0 + rowsPerChunk, nRows));
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(nThreads - 1);
    for(int i = 0; i < nThreads - 1; ++i) threads.emplace_back(worker);
    worker();
    for(auto& t : threads) t.join();
}

// HDF5 in this build is not thread-safe, and every Imaris writer has its own HDF5-calling thread:
// one conversion at a time per process.
std::mutex& libraryMutex(){
    static std::mutex vMutex;
    return vMutex;
}

// OpenMP's thread count of the calling thread for the duration of a call (numThreads > 0).
class ThreadCount {
public:
    explicit ThreadCount(int aThreads) : mSet(aThreads > 0), mPrevious(omp_get_max_threads()) {
        if(mSet) omp_set_num_threads(aThreads);
    }
    ~ThreadCount(){ if(mSet) omp_set_num_threads(mPrevious); }
private:
    bool mSet;
    int mPrevious;
};

// Formats one line of the command line's output and hands it to Callbacks::message.
void say(const Callbacks& aCallbacks, const char* aFormat, ...)
#if defined(__GNUC__) && !defined(_WIN32)
    __attribute__((format(printf, 2, 3)))
#endif
    ;
void say(const Callbacks& aCallbacks, const char* aFormat, ...){
    if(!aCallbacks.message) return;
    char vSmall[512];
    va_list vArgs;
    va_start(vArgs, aFormat);
    const int vLength = vsnprintf(vSmall, sizeof(vSmall), aFormat, vArgs);
    va_end(vArgs);
    if(vLength < 0) return;
    if((size_t)vLength < sizeof(vSmall)){ aCallbacks.message(vSmall, aCallbacks.user); return; }
    std::vector<char> vText((size_t)vLength + 1);
    va_start(vArgs, aFormat);
    vsnprintf(vText.data(), vText.size(), aFormat, vArgs);
    va_end(vArgs);
    aCallbacks.message(vText.data(), aCallbacks.user);
}

bool cancelled(const Callbacks& aCallbacks){
    return aCallbacks.cancel && aCallbacks.cancel(aCallbacks.user);
}

Status cancelledStatus(){ return Status(Code::Cancelled, "The conversion was cancelled."); }

std::string sizeText(const uint64_t aSize[3]){
    return std::to_string(aSize[0]) + "x" + std::to_string(aSize[1]) + "x" + std::to_string(aSize[2]);
}

// Removes the output after a failure (only a regular file: never a folder of that name).
void removeOutput(const std::string& aFile){
    struct stat vInfo;
    if(stat(aFile.c_str(), &vInfo) == 0 && (vInfo.st_mode & S_IFMT) == S_IFREG) remove(aFile.c_str());
}

// Owns a buffer the readers allocated with malloc.
class ReadBuffer {
public:
    explicit ReadBuffer(void* aData) : mData(aData) {}
    ~ReadBuffer(){ free(mData); }   // free(NULL) is a no-op for pure zero-pad read-blocks
private:
    ReadBuffer(const ReadBuffer&);
    ReadBuffer& operator=(const ReadBuffer&);
    void* mData;
};

// Shape (X, Y, Z), default block size and bits of one input volume. The readers report errors
// with NULL or 0, and cpp-zarr's constructor throws std::string: all become ReadFailed.
Status volumeInfo(bool aTiff, const std::string& aFile, uint64_t aShape[3], uint64_t aChunks[3], uint64_t& aBits){
    const std::string vFailed = "could not read " + aFile;
    try{
        if(aTiff){
            uint64_t* vSize = getImageSize(aFile.c_str());
            if(!vSize) return Status(Code::ReadFailed, vFailed);
            aShape[0] = vSize[0];
            aShape[1] = vSize[1];
            aShape[2] = vSize[2];
            free(vSize);
            aChunks[0] = 64;
            aChunks[1] = 64;
            aChunks[2] = 78;
            aBits = getDataType(aFile.c_str());
            if(!aBits) return Status(Code::ReadFailed, vFailed);
        }
        else{
            zarr Zarr(aFile);
            if(Zarr.get_ndims() != 3) return Status(Code::Unsupported, aFile + " is not a 3D array.");
            aChunks[0] = Zarr.get_chunks(0);
            aChunks[1] = Zarr.get_chunks(1);
            aChunks[2] = Zarr.get_chunks(2);
            aBits = Zarr.dtypeBytes()*8;
            aShape[0] = Zarr.get_shape(0);
            aShape[1] = Zarr.get_shape(1);
            aShape[2] = Zarr.get_shape(2);
        }
    }
    catch(const std::string& vError){ return Status(Code::ReadFailed, vFailed + ": " + vError); }
    catch(const std::exception& vError){ return Status(Code::ReadFailed, vFailed + ": " + vError.what()); }
    catch(...){ return Status(Code::ReadFailed, vFailed); }
    return Status();
}

// Every input must match the first (same bits; same shape, or with a bounding box: contain it).
// Runs before the output is created, so a bad file fails at the start and leaves no output.
Status checkInputs(const ConvertOptions& aOptions, bool aTiff, const uint64_t aShape[3], uint64_t aBits){
    const std::string& vFirst = aOptions.files[0][0];
    const uint64_t* vBox = aOptions.boundingBox;
    for(size_t vC = 0; vC < aOptions.files.size(); ++vC){
        for(size_t vT = 0; vT < aOptions.files[vC].size(); ++vT){
            const std::string& vFile = aOptions.files[vC][vT];
            uint64_t vShape[3] = {aShape[0], aShape[1], aShape[2]};
            uint64_t vChunks[3];
            uint64_t vBits = aBits;
            if(vC || vT){
                Status vStatus = volumeInfo(aTiff, vFile, vShape, vChunks, vBits);
                if(!vStatus.ok()) return vStatus;
            }
            if(vBits != aBits){
                return Status(Code::InvalidArgument, vFile + " holds " + std::to_string(vBits) + "-bit data but " +
                              vFirst + " holds " + std::to_string(aBits) + "-bit data.");
            }
            if(aOptions.hasBoundingBox){
                if(vBox[3] > vShape[0] || vBox[4] > vShape[1] || vBox[5] > vShape[2]){
                    const uint64_t vEnd[3] = {vBox[3], vBox[4], vBox[5]};
                    return Status(Code::InvalidArgument, "The bounding box (end " + sizeText(vEnd) + ") does not fit in " +
                                  vFile + " (" + sizeText(vShape) + ").");
                }
            }
            else if(vShape[0] != aShape[0] || vShape[1] != aShape[1] || vShape[2] != aShape[2]){
                return Status(Code::InvalidArgument, vFile + " is " + sizeText(vShape) + " but " + vFirst + " is " +
                              sizeText(aShape) + "; every input must have the same size.");
            }
        }
    }
    return Status();
}

Status validate(const ConvertOptions& aOptions){
    if(aOptions.reader != "tiff" && aOptions.reader != "zarr"){
        return Status(Code::InvalidArgument, "Reader type \"" + aOptions.reader + "\" is not a supported reader type.");
    }
    if(aOptions.files.empty() || aOptions.files[0].empty()){
        return Status(Code::InvalidArgument, "No input files were given.");
    }
    for(size_t vC = 1; vC < aOptions.files.size(); ++vC){
        if(aOptions.files[vC].size() != aOptions.files[0].size()){
            return Status(Code::InvalidArgument, "Channel " + std::to_string(vC) + " has " + std::to_string(aOptions.files[vC].size()) +
                          " input files but channel 0 has " + std::to_string(aOptions.files[0].size()) +
                          "; every channel needs one file per time point.");
        }
    }
    if(aOptions.outputFile.empty()) return Status(Code::InvalidArgument, "No output file was given.");
    if(aOptions.hasBlockSize && (!aOptions.blockSize[0] || !aOptions.blockSize[1] || !aOptions.blockSize[2])){
        return Status(Code::InvalidArgument, "Block sizes must be at least 1.");
    }
    if(aOptions.hasBoundingBox){
        const uint64_t* vBox = aOptions.boundingBox;
        if(vBox[3] <= vBox[0] || vBox[4] <= vBox[1] || vBox[5] <= vBox[2]){
            return Status(Code::InvalidArgument, "The bounding box must end after it starts along every axis.");
        }
    }
    if(aOptions.numThreads < 0) return Status(Code::InvalidArgument, "The number of threads cannot be negative.");
    return Status();
}

// One output file through the ImarisWriter C interface. open, writeRegion and finish make the
// file; without a successful finish the destructor stops the writer and removes the partial file.
class ImsWriter {
public:
    explicit ImsWriter(const Callbacks& aCallbacks)
        : mCallbacks(aCallbacks), mConverter(NULL), mBits(0), mBuffer(NULL), mFinished(false), mWriterFailed(false) {}
    ~ImsWriter();

    Status open(uint64_t aBits, const bpConverterTypesC_Size5D& aImageSize, const bpConverterTypesC_Size5D& aBlockSize,
                const float aVoxelSize[3], const std::string& aOutputFile);
    // Copies the Imaris blocks [aBlockBase, aBlockLimit) of (aC, aT) from one read-block: aSrc holds
    // the image region that starts at (aX0, aY0, aZ0) and is aXN x aYN wide (NULL: all zero padding).
    Status writeRegion(uint64_t aC, uint64_t aT, const uint64_t aBlockBase[3], const uint64_t aBlockLimit[3],
                       const void* aSrc, uint64_t aX0, uint64_t aY0, uint64_t aZ0, uint64_t aXN, uint64_t aYN);
    Status finish();

private:
    ImsWriter(const ImsWriter&);
    ImsWriter& operator=(const ImsWriter&);

    Status checkErrors();
    void writeMetadata();
    static void onProgress(bpConverterTypesC_Float aProgress, bpConverterTypesC_UInt64 aTotalBytesWritten, void* aUserData);

    const Callbacks& mCallbacks;
    bpImageConverterCPtr mConverter;
    uint64_t mBits;
    bpConverterTypesC_Size5D mImageSize;
    bpConverterTypesC_Size5D mBlockSize;
    float mVoxelSize[3];
    std::string mOutputFile;
    void* mBuffer;
    bool mFinished;
    bool mWriterFailed;
};

ImsWriter::~ImsWriter(){
    if(!mConverter) return;
    if(!mFinished && !mWriterFailed){
        // Destroying the writer while it still processes blocks is not safe: let it finish what
        // it holds first (the file is removed below anyway).
        try{ writeMetadata(); } catch(...){}
    }
    bpImageConverterC_Destroy(mConverter);
    free(mBuffer);
    if(!mFinished) removeOutput(mOutputFile);
}

void ImsWriter::onProgress(bpConverterTypesC_Float aProgress, bpConverterTypesC_UInt64 aTotalBytesWritten, void* aUserData){
    const Callbacks* vCallbacks = (const Callbacks*)aUserData;
    vCallbacks->progress(aProgress, aTotalBytesWritten, vCallbacks->user);
}

Status ImsWriter::checkErrors(){
    const char* vException = bpImageConverterC_GetLastException(mConverter);
    if(!vException) return Status();
    mWriterFailed = true;
    return Status(Code::WriteFailed, vException);
}

Status ImsWriter::open(uint64_t aBits, const bpConverterTypesC_Size5D& aImageSize, const bpConverterTypesC_Size5D& aBlockSize,
                       const float aVoxelSize[3], const std::string& aOutputFile){
    mBits = aBits;
    mImageSize = aImageSize;
    mBlockSize = aBlockSize;
    mVoxelSize[0] = aVoxelSize[0];
    mVoxelSize[1] = aVoxelSize[1];
    mVoxelSize[2] = aVoxelSize[2];
    mOutputFile = aOutputFile;

    bpConverterTypesC_DataType aDataType = aBits == 8 ? bpConverterTypesC_UInt8Type :
                                           aBits == 16 ? bpConverterTypesC_UInt16Type : bpConverterTypesC_FloatType;
    bpConverterTypesC_Size5D aSample = { 1, 1, 1, 1, 1 };

    bpConverterTypesC_DimensionSequence5D aDimensionSequence = {
        bpConverterTypesC_DimensionX,
        bpConverterTypesC_DimensionY,
        bpConverterTypesC_DimensionZ,
        bpConverterTypesC_DimensionC,
        bpConverterTypesC_DimensionT
    };

    bpConverterTypesC_Options aOptions;
    aOptions.mThumbnailSizeXY = 256;
    aOptions.mFlipDimensionX = false;
    aOptions.mFlipDimensionY = false;
    aOptions.mFlipDimensionZ = false;
    aOptions.mForceFileBlockSizeZ1 = false;
    aOptions.mEnableLogProgress = true;
    aOptions.mNumberOfThreads = omp_get_max_threads();
    aOptions.mCompressionAlgorithmType = eCompressionAlgorithmLShuffleLZ4;

    bpConverterTypesC_String aApplicationName = "TestC";
    bpConverterTypesC_String aApplicationVersion = "1.0.0";
    bpConverterTypesC_ProgressCallback aProgressCallback = mCallbacks.progress ? onProgress : NULL;

    mConverter = bpImageConverterC_Create(
                aDataType, &mImageSize, &aSample,
                &aDimensionSequence, &mBlockSize,
                mOutputFile.c_str(), &aOptions,
                aApplicationName, aApplicationVersion,
                aProgressCallback, (void*)&mCallbacks
                );
    Status vStatus = checkErrors();
    if(!vStatus.ok()) return vStatus;

    unsigned long long vBlockSize =
            (unsigned long long)mBlockSize.mValueX *
            mBlockSize.mValueY * mBlockSize.mValueZ *
            mBlockSize.mValueC * mBlockSize.mValueT;
    mBuffer = malloc(vBlockSize*(mBits/8));
    if(!mBuffer) return Status(Code::Internal, "Out of memory for one block of " + std::to_string(vBlockSize) + " voxels.");
    return Status();
}

Status ImsWriter::writeRegion(uint64_t aC, uint64_t aT, const uint64_t aBlockBase[3], const uint64_t aBlockLimit[3],
                              const void* aSrc, uint64_t aX0, uint64_t aY0, uint64_t aZ0, uint64_t aXN, uint64_t aYN){
    const uint64_t bx = mBlockSize.mValueX, by = mBlockSize.mValueY, bz = mBlockSize.mValueZ;
    const uint64_t shapeX = mImageSize.mValueX, shapeY = mImageSize.mValueY, shapeZ = mImageSize.mValueZ;
    bpConverterTypesC_Index5D aBlockIndex = { 0, 0, 0, (unsigned int)aC, (unsigned int)aT };
    for (uint64_t vZ = aBlockBase[2]; vZ < aBlockLimit[2]; ++vZ) {
        aBlockIndex.mValueZ = (unsigned int)vZ;
        for (uint64_t vY = aBlockBase[1]; vY < aBlockLimit[1]; ++vY) {
            aBlockIndex.mValueY = (unsigned int)vY;
            for (uint64_t vX = aBlockBase[0]; vX < aBlockLimit[0]; ++vX) {
                aBlockIndex.mValueX = (unsigned int)vX;
                switch(mBits){
                    case 8:
                        fillBlock<uint8_t> ((uint8_t*)mBuffer, (const uint8_t*)aSrc, vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, aXN,aYN);
                        bpImageConverterC_CopyBlockUInt8(mConverter, (uint8_t*)mBuffer, &aBlockIndex);
                        break;
                    case 16:
                        fillBlock<uint16_t>((uint16_t*)mBuffer, (const uint16_t*)aSrc, vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, aXN,aYN);
                        bpImageConverterC_CopyBlockUInt16(mConverter, (uint16_t*)mBuffer, &aBlockIndex);
                        break;
                    case 32:
                        fillBlock<float>   ((float*)mBuffer,    (const float*)aSrc,    vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, aXN,aYN);
                        bpImageConverterC_CopyBlockFloat(mConverter, (float*)mBuffer, &aBlockIndex);
                        break;
                }
                Status vStatus = checkErrors();
                if(!vStatus.ok()) return vStatus;
                if(cancelled(mCallbacks)) return cancelledStatus();
            }
        }
    }
    return Status();
}

// The placeholder metadata of ImarisWriter's example program (kept for byte parity with the
// files written so far), then bpImageConverterC_Finish.
void ImsWriter::writeMetadata(){
    bpConverterTypesC_ImageExtent aImageExtent = {
        0, 0, 0,
        mImageSize.mValueX * mVoxelSize[0],
        mImageSize.mValueY * mVoxelSize[1],
        mImageSize.mValueZ * mVoxelSize[2]
    };

    unsigned int vNumberOfOtherSections = 1; // Image
    unsigned int vNumberOfSections = vNumberOfOtherSections + mImageSize.mValueC;
    std::vector<bpConverterTypesC_ParameterSection> vParameterSections(vNumberOfSections);

    bpConverterTypesC_Parameter vUnitParameter = { "Unit", "um" };
    bpConverterTypesC_ParameterSection* vImageSection = &vParameterSections[0];
    vImageSection->mName = "Image";
    vImageSection->mValuesCount = 1;
    vImageSection->mValues = &vUnitParameter;

    std::vector<std::string> vChannelNames(mImageSize.mValueC);
    unsigned int vNumberOfParametersPerChannel = 3;
    std::vector<bpConverterTypesC_Parameter> vChannelParameters(mImageSize.mValueC * vNumberOfParametersPerChannel);
    for (unsigned int vC = 0; vC < mImageSize.mValueC; ++vC) {
        bpConverterTypesC_Parameter* vThisChannelParameters = &vChannelParameters[vNumberOfParametersPerChannel * vC];
        vThisChannelParameters[0].mName = "Name";
        vThisChannelParameters[0].mValue = vC == 0 ? "First channel" : vC == 1 ? "Second channel" : vC == 2 ? "Third channel" : "Other channel";
        vThisChannelParameters[1].mName = "LSMEmissionWavelength";
        vThisChannelParameters[1].mValue = "700";
        vThisChannelParameters[2].mName = "OtherChannelParameter";
        vThisChannelParameters[2].mValue = "OtherChannelValue";
        bpConverterTypesC_ParameterSection* vChannelSection = &vParameterSections[vNumberOfOtherSections + vC];
        vChannelNames[vC] = "Channel " + std::to_string(vC);
        vChannelSection->mName = vChannelNames[vC].c_str();
        vChannelSection->mValues = vThisChannelParameters;
        vChannelSection->mValuesCount = vNumberOfParametersPerChannel;
    }

    bpConverterTypesC_Parameters aParameters;
    aParameters.mValuesCount = vNumberOfSections;
    aParameters.mValues = vParameterSections.data();

    std::vector<bpConverterTypesC_TimeInfo> vTimeInfos(mImageSize.mValueT);
    for (unsigned int vT = 0; vT < mImageSize.mValueT; ++vT) {
        vTimeInfos[vT].mJulianDay = 2458885; // 5 feb 2020
        unsigned long long vSeconds = vT + 4 + 60 * (27 + 60 * 15); // 3:27.04 PM + 1 sec per time point
        vTimeInfos[vT].mNanosecondsOfDay = vSeconds * 1000000000;
    }
    bpConverterTypesC_TimeInfos aTimeInfoPerTimePoint;
    aTimeInfoPerTimePoint.mValuesCount = mImageSize.mValueT;
    aTimeInfoPerTimePoint.mValues = vTimeInfos.data();

    std::vector<bpConverterTypesC_ColorInfo> vColorInfos(mImageSize.mValueC);
    for (unsigned int vC = 0; vC < mImageSize.mValueC; ++vC) {
        bpConverterTypesC_ColorInfo* vColor = &vColorInfos[vC];
        vColor->mIsBaseColorMode = true;
        vColor->mBaseColor.mRed = (vC % 3) == 0 ? 1 : 0;
        vColor->mBaseColor.mGreen = (vC % 3) == 1 ? 1 : 0;
        vColor->mBaseColor.mBlue = (vC % 3) == 2 ? 1 : 0;
        vColor->mBaseColor.mAlpha = 1;
        vColor->mColorTable = NULL;
        vColor->mColorTableSize = 0;
        vColor->mOpacity = 0;
        vColor->mRangeMin = 0;
        vColor->mRangeMax = 255;
        vColor->mGammaCorrection = 1;
    }
    bpConverterTypesC_ColorInfos aColorInfoPerChannel;
    aColorInfoPerChannel.mValuesCount = mImageSize.mValueC;
    aColorInfoPerChannel.mValues = vColorInfos.data();

    bool aAutoAdjustColorRange = true;

    bpImageConverterC_Finish(mConverter,
                             &aImageExtent, &aParameters, &aTimeInfoPerTimePoint,
                             &aColorInfoPerChannel, aAutoAdjustColorRange);
}

Status ImsWriter::finish(){
    writeMetadata();
    Status vStatus = checkErrors();
    if(vStatus.ok()) mFinished = true;
    return vStatus;
}

uint64_t numBlocks(uint64_t aSize, uint64_t aBlockSize){
    return (aSize + aBlockSize - 1) / aBlockSize;
}

// The conversion itself (options validated, library mutex held).
Status convertValidated(const ConvertOptions& aOptions, const Callbacks& aCallbacks){
    const bool vTiff = aOptions.reader == "tiff";
    const uint64_t channels = aOptions.files.size();
    const uint64_t timepoints = aOptions.files[0].size();

    uint64_t vShape[3] = {0, 0, 0};
    uint64_t vChunks[3] = {0, 0, 0};
    uint64_t bits = 1;
    Status vStatus = volumeInfo(vTiff, aOptions.files[0][0], vShape, vChunks, bits);
    if(!vStatus.ok()) return vStatus;
    vStatus = checkInputs(aOptions, vTiff, vShape, bits);
    if(!vStatus.ok()) return vStatus;

    // Use custom chunk sizes if provided
    if(aOptions.hasBlockSize){
        vChunks[0] = aOptions.blockSize[0];
        vChunks[1] = aOptions.blockSize[1];
        vChunks[2] = aOptions.blockSize[2];
    }
    say(aCallbacks, "Block Sizes: %llu,%llu,%llu\n",
        (unsigned long long)vChunks[0], (unsigned long long)vChunks[1], (unsigned long long)vChunks[2]);
    // TODO: 64 BIT NOT YET SUPPORTED
    if(bits == 64) return Status(Code::Unsupported, detail::kNo64BitData);
    if(bits != 8 && bits != 16 && bits != 32){
        return Status(Code::Unsupported, std::to_string(bits) + "-bit data is not supported.");
    }

    // Bounding Box
    uint64_t boundingBox[6] = {0, 0, 0, vShape[0], vShape[1], vShape[2]};
    if(aOptions.hasBoundingBox){
        for(int i = 0; i < 6; i++) boundingBox[i] = aOptions.boundingBox[i];
        vShape[0] = boundingBox[3]-boundingBox[0];
        vShape[1] = boundingBox[4]-boundingBox[1];
        vShape[2] = boundingBox[5]-boundingBox[2];
        say(aCallbacks, "Bounding Box: %llu %llu %llu %llu %llu %llu\n",
            (unsigned long long)boundingBox[0], (unsigned long long)boundingBox[1], (unsigned long long)boundingBox[2],
            (unsigned long long)boundingBox[3], (unsigned long long)boundingBox[4], (unsigned long long)boundingBox[5]);
        say(aCallbacks, "New Shape: %llu %llu %llu\n",
            (unsigned long long)vShape[0], (unsigned long long)vShape[1], (unsigned long long)vShape[2]);
    }
    const uint64_t shapeX = vShape[0], shapeY = vShape[1], shapeZ = vShape[2];
    if(shapeX > UINT_MAX || shapeY > UINT_MAX || shapeZ > UINT_MAX || channels > UINT_MAX || timepoints > UINT_MAX ||
       vChunks[0] > UINT_MAX || vChunks[1] > UINT_MAX || vChunks[2] > UINT_MAX){
        return Status(Code::Unsupported, "The image or block size does not fit the Imaris writer (at most " +
                      std::to_string(UINT_MAX) + " per axis).");
    }

    // The Imaris image is exactly the data size; the writer pads each resolution
    // level up to its chunk boundary internally. (Earlier code added shapeX %
    // chunkXSize here to "round up" to a chunk multiple, but that formula was
    // wrong -- it added the remainder, not its complement -- inflating a
    // non-chunk-aligned dimension into extra zero-filled planes and skewing the
    // pyramid. It also made the output depend on the arbitrary zarr chunk size,
    // so the same data chunked differently produced different .ims files.)
    bpConverterTypesC_Size5D aImageSize = {
        (unsigned int)shapeX, (unsigned int)shapeY, (unsigned int)shapeZ, (unsigned int)channels, (unsigned int)timepoints
    };

    say(aCallbacks, "Voxel Sizes: %f,%f,%f\n", aOptions.voxelSize[0], aOptions.voxelSize[1], aOptions.voxelSize[2]);

    bpConverterTypesC_Size5D aBlockSize = {
        (unsigned int)vChunks[0], (unsigned int)vChunks[1], (unsigned int)vChunks[2], 1, 1
    };

    ImsWriter vWriter(aCallbacks);
    vStatus = vWriter.open(bits, aImageSize, aBlockSize, aOptions.voxelSize, aOptions.outputFile);
    if(!vStatus.ok()) return vStatus;

    const uint64_t vNBlocksX = numBlocks(aImageSize.mValueX, aBlockSize.mValueX);
    const uint64_t vNBlocksY = numBlocks(aImageSize.mValueY, aBlockSize.mValueY);
    const uint64_t vNBlocksZ = numBlocks(aImageSize.mValueZ, aBlockSize.mValueZ);
    const uint64_t vNBlocksC = numBlocks(aImageSize.mValueC, aBlockSize.mValueC);
    const uint64_t vNBlocksT = numBlocks(aImageSize.mValueT, aBlockSize.mValueT);

    // Processing block (-C): how much of a timepoint to hold in RAM at once,
    // expressed in whole Imaris blocks so an output block never straddles a read.
    // Default (no -C) = the whole timepoint in one read (prior behavior). For tiff
    // only Z is blocked (full X/Y slabs); zarr blocks in all three.
    uint64_t cbX = vNBlocksX, cbY = vNBlocksY, cbZ = vNBlocksZ;
    if(aOptions.hasProcessBlock){
        const uint64_t userX = aOptions.processBlock[0];
        const uint64_t userY = aOptions.processBlock[1];
        const uint64_t userZ = aOptions.processBlock[2];
        cbZ = userZ < aBlockSize.mValueZ ? 1 : (userZ + aBlockSize.mValueZ - 1) / aBlockSize.mValueZ;
        if(!vTiff){   // non-tiff (zarr): also block X and Y
            cbX = userX < aBlockSize.mValueX ? 1 : (userX + aBlockSize.mValueX - 1) / aBlockSize.mValueX;
            cbY = userY < aBlockSize.mValueY ? 1 : (userY + aBlockSize.mValueY - 1) / aBlockSize.mValueY;
        }
        say(aCallbacks, "Processing Block (chunks XYZ): %llu,%llu,%llu\n",
            (unsigned long long)cbX, (unsigned long long)cbY, (unsigned long long)cbZ);
    }

    for (uint64_t vC = 0; vC < vNBlocksC; ++vC) {
        for (uint64_t vT = 0; vT < vNBlocksT; ++vT) {
            const std::string& vFile = aOptions.files[vC][vT];
            const char* fileName = vFile.c_str();
            say(aCallbacks, "Current File: %s\n", fileName);
            // Iterate the Imaris block grid in read-block-sized groups and read only
            // that sub-region of the timepoint at a time (bounded RAM). Walking the
            // SAME grid as a whole read keeps the output byte-for-byte identical.
            for (uint64_t vZbase = 0; vZbase < vNBlocksZ; vZbase += cbZ) {
              uint64_t vZlimit = (vZbase+cbZ < vNBlocksZ) ? vZbase+cbZ : vNBlocksZ;
              uint64_t z0 = vZbase*aBlockSize.mValueZ;
              uint64_t zEnd = vZlimit*aBlockSize.mValueZ; if(zEnd > shapeZ) zEnd = shapeZ;
              uint64_t zN = (z0 < shapeZ) ? zEnd-z0 : 0;
            for (uint64_t vYbase = 0; vYbase < vNBlocksY; vYbase += cbY) {
              uint64_t vYlimit = (vYbase+cbY < vNBlocksY) ? vYbase+cbY : vNBlocksY;
              uint64_t y0 = vYbase*aBlockSize.mValueY;
              uint64_t yEnd = vYlimit*aBlockSize.mValueY; if(yEnd > shapeY) yEnd = shapeY;
              uint64_t yN = (y0 < shapeY) ? yEnd-y0 : 0;
            for (uint64_t vXbase = 0; vXbase < vNBlocksX; vXbase += cbX) {
              uint64_t vXlimit = (vXbase+cbX < vNBlocksX) ? vXbase+cbX : vNBlocksX;
              uint64_t x0 = vXbase*aBlockSize.mValueX;
              uint64_t xEnd = vXlimit*aBlockSize.mValueX; if(xEnd > shapeX) xEnd = shapeX;
              uint64_t xN = (x0 < shapeX) ? xEnd-x0 : 0;

            if(cancelled(aCallbacks)) return cancelledStatus();

            // Read this read-block. NULL when it is entirely in the zero-pad region
            // (nothing to read) -- the fill loop zero-fills those blocks.
            void* vData = NULL;
            if(xN && yN && zN){
                std::string vWhy;
                try{
                    if(vTiff){
                        // full X/Y, a Z-slab; a whole-Z read uses the original call so the
                        // default (no -C) path stays byte-identical (and ImageJ-safe).
                        if(zN == shapeZ) vData = readTiffParallelWrapper(fileName);
                        else vData = readTiffParallelWrapper(fileName, {z0, z0+zN});
                    }
                    else vData = readZarrParallelHelper(fileName,
                                     boundingBox[0]+x0, boundingBox[1]+y0, boundingBox[2]+z0,
                                     boundingBox[0]+xEnd, boundingBox[1]+yEnd, boundingBox[2]+zEnd, 0);
                }
                catch(const std::string& vError){ vWhy = vError; }
                catch(const std::exception& vError){ vWhy = vError.what(); }
                catch(...){ vWhy = "unknown error"; }
                // The readers return NULL on failure; don't let it zero-fill like padding.
                if(!vData) return Status(Code::ReadFailed, "could not read " + vFile + (vWhy.empty() ? "" : ": " + vWhy));
            }
            ReadBuffer vHold(vData);

            const uint64_t vBase[3] = {vXbase, vYbase, vZbase};
            const uint64_t vLimit[3] = {vXlimit, vYlimit, vZlimit};
            vStatus = vWriter.writeRegion(vC, vT, vBase, vLimit, vData, x0, y0, z0, xN, yN);
            if(!vStatus.ok()) return vStatus;
            }
            }
            }
        }
    }

    return vWriter.finish();
}

Status internalError(const char* aWhat){
    return Status(Code::Internal, std::string("Internal error: ") + aWhat);
}

// One folder's entries for one pattern (today's selection), appended to aFiles.
void listFiles(const std::string& aFolder, const std::string& aPattern, std::vector<std::string>& aFiles){
    DIR* dir = opendir(aFolder.c_str());
    // Unable to open directory stream
    if (!dir)
        return;
    struct dirent* dp;
    while ((dp = readdir(dir)) != NULL)
    {
        if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0 && strrchr(dp->d_name, '.'))
        {
            if(((!strcmp(strrchr(dp->d_name, '.'),".tif")) || (!strcmp(strrchr(dp->d_name, '.'),".zarr"))) && strstr(dp->d_name,aPattern.c_str()) != NULL){
                #ifdef _WIN32
                aFiles.push_back(aFolder + "\\" + dp->d_name);
                #else
                aFiles.push_back(aFolder + "/" + dp->d_name);
                #endif
            }
        }
    }
    closedir(dir);
}

// strcmp order, with Windows' '\' compared as '/' so files sort the same on every platform.
bool lessPath(const std::string& aA, const std::string& aB){
    #ifdef _WIN32
    const size_t n = std::min(aA.size(), aB.size());
    for(size_t i = 0; i < n; ++i){
        const unsigned char a = aA[i] == '\\' ? '/' : (unsigned char)aA[i];
        const unsigned char b = aB[i] == '\\' ? '/' : (unsigned char)aB[i];
        if(a != b) return a < b;
    }
    return aA.size() < aB.size();
    #else
    return strcmp(aA.c_str(), aB.c_str()) < 0;
    #endif
}

}  // namespace

Status findInputFiles(const std::vector<std::string>& folders, const std::vector<std::string>& patterns,
                      std::vector<std::vector<std::string> >& files) noexcept{
    try{
        std::vector<std::vector<std::string> > vFiles;
        for(size_t vP = 0; vP < patterns.size(); ++vP){
            std::vector<std::string> vChannel;
            for(size_t vF = 0; vF < folders.size(); ++vF) listFiles(folders[vF], patterns[vP], vChannel);
            if(vChannel.empty()){
                return Status(Code::NotFound, "Given pattern \"" + patterns[vP] + "\" was not found in any of the provided folders.");
            }
            std::sort(vChannel.begin(), vChannel.end(), lessPath);
            vFiles.push_back(vChannel);
        }
        files.swap(vFiles);
        return Status();
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status convert(const ConvertOptions& options, const Callbacks& callbacks) noexcept{
    try{
        Status vStatus = validate(options);
        if(!vStatus.ok()) return vStatus;
        std::lock_guard<std::mutex> vLock(libraryMutex());
        ThreadCount vThreads(options.numThreads);
        return convertValidated(options, callbacks);
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

const char* version() noexcept{
    return PIW_VERSION;
}

int defaultThreads() noexcept{
    return omp_get_max_threads();
}

}  // namespace piw
