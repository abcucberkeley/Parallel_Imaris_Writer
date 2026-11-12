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
#include "imsmeta.h"
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>
#include <omp.h>
#if defined(__SSE2__)
#include <emmintrin.h>
#endif
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
// HDF5 in this build is not thread-safe, and every Imaris writer has its own HDF5-calling thread:
// one library call (conversion, write or read) at a time per process (src/imsread.cpp and the single-file
// cluster steps in src/mono.cpp take it too).
std::mutex& libraryMutex();
std::mutex& libraryMutex(){
    static std::mutex vMutex;
    return vMutex;
}
}
using detail::libraryMutex;

namespace {

// Runs fn(r0, r1) over the rows [0, nRows) of one block. No OpenMP here: an OpenMP region per
// block (thousands per volume) kept the idle workers spinning between regions for the whole
// conversion, on the cores that ImarisWriter's compression threads need. Blocks of 8 MiB or more
// are copied by threads that live only for this call; smaller ones on the caller's thread.
template<typename F>
static void forBlockRows(uint64_t nRows, uint64_t blockBytes, const F& fn){
    const int nThreads = std::min(omp_get_max_threads(), 16);
    if(blockBytes < (8ull << 20) || nThreads <= 1){
        fn(0, nRows);
        return;
    }
    const uint64_t rowsPerChunk = 64;
    std::atomic<uint64_t> next(0);
    auto worker = [&](){
        for(;;){
            const uint64_t r0 = next.fetch_add(rowsPerChunk);
            if(r0 >= nRows) break;
            fn(r0, std::min(r0 + rowsPerChunk, nRows));
        }
    };
    std::vector<std::thread> threads;
    threads.reserve(nThreads - 1);
    for(int i = 0; i < nThreads - 1; ++i) threads.emplace_back(worker);
    worker();
    for(auto& t : threads) t.join();
}

// Copies one K x K tile, transposed: d[i + j*ds] = s[i*ss + j] for i, j < K. Scalar here; SSE2
// versions (always there on x86-64) for 2- and 4-byte samples below.
template<typename T>
struct TransposeTile {
    static const int K = 8;
    static void copy(const T* s, int64_t ss, T* d, uint64_t ds){
        for(int i = 0; i < K; ++i)
            for(int j = 0; j < K; ++j) d[i + j*ds] = s[i*ss + j];
    }
};
#if defined(__SSE2__)
template<>
struct TransposeTile<uint16_t> {
    static const int K = 8;
    static void copy(const uint16_t* s, int64_t ss, uint16_t* d, uint64_t ds){
        __m128i r[8], a[8], b[8];
        for(int i = 0; i < 8; ++i) r[i] = _mm_loadu_si128((const __m128i*)(s + i*ss));
        for(int i = 0; i < 4; ++i){
            a[2*i] = _mm_unpacklo_epi16(r[2*i], r[2*i + 1]);
            a[2*i + 1] = _mm_unpackhi_epi16(r[2*i], r[2*i + 1]);
        }
        for(int i = 0; i < 2; ++i){
            b[4*i] = _mm_unpacklo_epi32(a[4*i], a[4*i + 2]);
            b[4*i + 1] = _mm_unpackhi_epi32(a[4*i], a[4*i + 2]);
            b[4*i + 2] = _mm_unpacklo_epi32(a[4*i + 1], a[4*i + 3]);
            b[4*i + 3] = _mm_unpackhi_epi32(a[4*i + 1], a[4*i + 3]);
        }
        for(int j = 0; j < 4; ++j){
            _mm_storeu_si128((__m128i*)(d + (2*j)*ds), _mm_unpacklo_epi64(b[j], b[j + 4]));
            _mm_storeu_si128((__m128i*)(d + (2*j + 1)*ds), _mm_unpackhi_epi64(b[j], b[j + 4]));
        }
    }
};
template<typename T>
struct TransposeTile4 {
    static const int K = 4;
    static void copy(const T* s, int64_t ss, T* d, uint64_t ds){
        const __m128i r0 = _mm_loadu_si128((const __m128i*)(s)), r1 = _mm_loadu_si128((const __m128i*)(s + ss));
        const __m128i r2 = _mm_loadu_si128((const __m128i*)(s + 2*ss)), r3 = _mm_loadu_si128((const __m128i*)(s + 3*ss));
        const __m128i t0 = _mm_unpacklo_epi32(r0, r1), t1 = _mm_unpacklo_epi32(r2, r3);
        const __m128i t2 = _mm_unpackhi_epi32(r0, r1), t3 = _mm_unpackhi_epi32(r2, r3);
        _mm_storeu_si128((__m128i*)(d), _mm_unpacklo_epi64(t0, t1));
        _mm_storeu_si128((__m128i*)(d + ds), _mm_unpackhi_epi64(t0, t1));
        _mm_storeu_si128((__m128i*)(d + 2*ds), _mm_unpacklo_epi64(t2, t3));
        _mm_storeu_si128((__m128i*)(d + 3*ds), _mm_unpackhi_epi64(t2, t3));
    }
};
template<> struct TransposeTile<uint32_t> : TransposeTile4<uint32_t> {};
template<> struct TransposeTile<float> : TransposeTile4<float> {};
#endif

// fillBlock for sources whose X stride is not 1: the in-image part of the block is copied in
// lines along the source axis with the smallest stride, so the source is read in contiguous runs
// (for a (z, y, x) C-contiguous array: along its x, which is Imaris Y) and never element by
// element with a large stride. When that axis is contiguous and the block's X runs across the
// lines (the (z, y, x) case), K x K tiles are transposed at once, which keeps the writes into the
// block contiguous too: 1.4 s -> 0.5 s for the 757 M voxels of a 1 GB tile, against 0.37 s for
// the row memcpy of an X-contiguous source (one thread, 64^3 uint16 blocks).
template<typename T>
static void fillBlockStrided(T* buf, const T* origin, const uint64_t aBlock[3], const uint64_t aValid[3],
                             const int64_t aStride[3]){
    const uint64_t blockElems = aBlock[0]*aBlock[1]*aBlock[2];
    if(aValid[0] < aBlock[0] || aValid[1] < aBlock[1] || aValid[2] < aBlock[2]) memset(buf, 0, blockElems*sizeof(T));
    const uint64_t bufStride[3] = {1, aBlock[0], aBlock[0]*aBlock[1]};
    int inner = 0;
    for(int a = 1; a < 3; ++a) if(aStride[a] < aStride[inner]) inner = a;
    const int middle = inner == 0 ? 1 : 0;              // of the other two, the one with the
    const int outer = inner == 2 ? 1 : 2;               // smaller block stride runs faster
    const uint64_t n = aValid[inner], nMiddle = aValid[middle], nOuter = aValid[outer];
    const int64_t sIn = aStride[inner], sMid = aStride[middle], sOut = aStride[outer];
    const uint64_t dIn = bufStride[inner], dMid = bufStride[middle], dOut = bufStride[outer];
    if(sIn == 1 && middle == 0){
        const uint64_t K = TransposeTile<T>::K;
        const uint64_t nTiles = (nMiddle + K - 1)/K;
        forBlockRows(nTiles*nOuter, blockElems*sizeof(T), [&](uint64_t r0, uint64_t r1){   // rows: (tile of X, outer)
            for(uint64_t r = r0; r < r1; ++r){
                const uint64_t m0 = (r % nTiles)*K, o = r / nTiles;
                const uint64_t mEnd = std::min(m0 + K, nMiddle);
                const T* s = origin + (int64_t)o*sOut;
                T* d = buf + o*dOut;
                uint64_t j = 0;
                if(mEnd - m0 == K){
                    for(; j + K <= n; j += K) TransposeTile<T>::copy(s + (int64_t)m0*sMid + j, sMid, d + m0 + j*dIn, dIn);
                }
                for(uint64_t m = m0; m < mEnd; ++m){             // what is left of the lines
                    const T* sl = s + (int64_t)m*sMid;
                    T* dl = d + m;
                    for(uint64_t i = j; i < n; ++i) dl[i*dIn] = sl[i];
                }
            }
        });
        return;
    }
    forBlockRows(nMiddle*nOuter, blockElems*sizeof(T), [&](uint64_t r0, uint64_t r1){
        for(uint64_t r = r0; r < r1; ++r){
            const uint64_t m = r % nMiddle, o = r / nMiddle;
            const T* s = origin + (int64_t)m*sMid + (int64_t)o*sOut;
            T* d = buf + m*dMid + o*dOut;
            if(sIn == 1){
                for(uint64_t i = 0; i < n; ++i) d[i*dIn] = s[i];
            } else {
                for(uint64_t i = 0; i < n; ++i) d[i*dIn] = s[(int64_t)i*sIn];
            }
        }
    });
}

// Fill one Imaris block-sized buffer from the read-block data: image voxel (gx, gy, gz) is
// src[(gx-x0)*sx + (gy-y0)*sy + (gz-z0)*sz] (element strides; the readers' buffers have
// sx = 1, sy = xN, sz = xN*yN). With sx == 1, row-wise memcpy/memset instead of per-voxel index
// math + a per-voxel type switch -- profiling showed the old per-voxel loop was ~1/4 of the
// whole conversion's CPU. src==NULL (a pure zero-pad read-block) yields all zeros. Indexing
// reduces to the whole-image indices when there is a single read-block (x0=y0=z0=0).
template<typename T>
static void fillBlock(T* buf, const T* src,
        uint64_t vX, uint64_t vY, uint64_t vZ,
        uint64_t bx, uint64_t by, uint64_t bz,
        uint64_t shapeX, uint64_t shapeY, uint64_t shapeZ,
        uint64_t x0, uint64_t y0, uint64_t z0, int64_t sx, int64_t sy, int64_t sz){
    const uint64_t gxBase = vX*bx, gyBase = vY*by, gzBase = vZ*bz;
    const uint64_t avail  = (gxBase < shapeX) ? (shapeX - gxBase) : 0;
    const uint64_t xValid = !src ? 0 : (avail < bx ? avail : bx);   // in-image cols of this block
    if(src && sx != 1){
        const uint64_t yAvail = (gyBase < shapeY) ? (shapeY - gyBase) : 0;
        const uint64_t zAvail = (gzBase < shapeZ) ? (shapeZ - gzBase) : 0;
        const uint64_t vBlock[3] = {bx, by, bz};
        const uint64_t vValid[3] = {xValid, yAvail < by ? yAvail : by, zAvail < bz ? zAvail : bz};
        if(!vValid[0] || !vValid[1] || !vValid[2]){
            memset(buf, 0, bx*by*bz*sizeof(T));
            return;
        }
        const int64_t vStride[3] = {sx, sy, sz};
        const T* origin = src + (int64_t)(gxBase - x0)*sx + (int64_t)(gyBase - y0)*sy + (int64_t)(gzBase - z0)*sz;
        fillBlockStrided(buf, origin, vBlock, vValid, vStride);
        return;
    }
    const uint64_t nRows  = by*bz;
    forBlockRows(nRows, nRows*bx*sizeof(T), [&](uint64_t r0, uint64_t r1){   // rows r = y + z*by
        for(uint64_t r = r0; r < r1; ++r){
            const uint64_t z = r / by, y = r % by;
            T* brow = buf + y*bx + z*bx*by;
            const uint64_t gy = gyBase + y, gz = gzBase + z;
            if(xValid == 0 || gy >= shapeY || gz >= shapeZ){
                memset(brow, 0, bx*sizeof(T));                       // row entirely outside the image
            } else {
                const T* srow = src + (gxBase - x0) + (int64_t)(gy - y0)*sy + (int64_t)(gz - z0)*sz;
                memcpy(brow, srow, xValid*sizeof(T));               // contiguous in-image run
                if(xValid < bx) memset(brow + xValid, 0, (bx - xValid)*sizeof(T));  // zero-pad tail
            }
        }
    });
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

// The sample type of a Zarr from cpp-zarr's dtype: NumPy's typestr after a byte-order mark
// ("<u2", "|u1", ">f4", "|b1", "<c8"), or "bfloat16". Big-endian data is swapped by the reader, so
// the byte order does not matter.
std::string zarrSampleType(const std::string& aDtype){
    const bool vMark = !aDtype.empty() && (aDtype[0] == '<' || aDtype[0] == '>' || aDtype[0] == '|');
    const std::string vRest = vMark ? aDtype.substr(1) : aDtype;
    bool vNumbered = vRest.size() >= 2 && vRest.size() <= 3;
    for(size_t i = 1; vNumbered && i < vRest.size(); ++i) vNumbered = vRest[i] >= '0' && vRest[i] <= '9';
    if(!vNumbered) return vRest;
    const std::string vBits = std::to_string(strtoul(vRest.c_str() + 1, NULL, 10)*8);
    switch(vRest[0]){
        case 'u': return "uint" + vBits;
        case 'i': return "int" + vBits;
        case 'f': return "float" + vBits;
        case 'c': return "complex" + vBits;
        case 'b': return "bool";
        default: return vRest;
    }
}

// The sample type of a TIFF from its BitsPerSample, SampleFormat and SamplesPerPixel tags.
std::string tiffSampleType(const TiffInfo& aInfo){
    const std::string vBits = std::to_string((unsigned long long)aInfo.bits);
    std::string vType = aInfo.sampleFormat == 1 ? "uint" + vBits : aInfo.sampleFormat == 2 ? "int" + vBits :
                        aInfo.sampleFormat == 3 ? "float" + vBits :
                        "SampleFormat " + std::to_string((unsigned)aInfo.sampleFormat) + " (" + vBits + "-bit)";
    if(aInfo.samples > 1) vType += " with " + std::to_string((unsigned long long)aInfo.samples) + " samples per pixel";
    return vType;
}

// The sample types an .ims gets, by the name the two functions above give them. Everything else
// (signed integers, float16 and float64, 64-bit integers, bool, complex, several samples per
// pixel) is refused before any output exists.
bool imsDataType(const std::string& aSampleType, DataType& aType){
    if(aSampleType == "uint8") aType = DataType::UInt8;
    else if(aSampleType == "uint16") aType = DataType::UInt16;
    else if(aSampleType == "uint32") aType = DataType::UInt32;
    else if(aSampleType == "float32") aType = DataType::Float32;
    else return false;
    return true;
}

Status unsupportedType(const std::string& aFile, const std::string& aSampleType){
    return Status(Code::Unsupported, aFile + " has sample type " + aSampleType + "; supported are uint8, uint16, "
                  "uint32 and float32, one sample per pixel.");
}

// Shape (X, Y, Z), default block size and sample type of one input volume. The readers report
// errors with NULL or 1, and cpp-zarr's constructor throws std::string: all become ReadFailed.
Status volumeInfo(bool aTiff, const std::string& aFile, uint64_t aShape[3], uint64_t aChunks[3], std::string& aSampleType){
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
            TiffInfo vInfo;
            if(getTiffInfo(aFile.c_str(), vInfo)) return Status(Code::ReadFailed, vFailed);
            aSampleType = tiffSampleType(vInfo);
        }
        else{
            zarr Zarr(aFile);
            if(Zarr.get_ndims() != 3){
                return Status(Code::Unsupported, aFile + " has " + std::to_string((unsigned long long)Zarr.get_ndims()) +
                              " dimensions; only 3-D arrays are supported.");
            }
            aChunks[0] = Zarr.get_chunks(0);
            aChunks[1] = Zarr.get_chunks(1);
            aChunks[2] = Zarr.get_chunks(2);
            aSampleType = zarrSampleType(Zarr.get_dtype());
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

std::string boxText(const uint64_t aBox[6]){
    return std::to_string(aBox[0]) + "," + std::to_string(aBox[1]) + "," + std::to_string(aBox[2]) + "," +
           std::to_string(aBox[3]) + "," + std::to_string(aBox[4]) + "," + std::to_string(aBox[5]);
}

// Every input must match the first (same sample type; same shape, or with a bounding box: contain
// it). Runs before the output is created, so a bad file fails at the start and leaves no output.
Status checkInputs(const ConvertOptions& aOptions, bool aTiff, const uint64_t aShape[3], const std::string& aSampleType){
    const std::string& vFirst = aOptions.files[0][0];
    const uint64_t* vBox = aOptions.boundingBox;
    for(size_t vC = 0; vC < aOptions.files.size(); ++vC){
        for(size_t vT = 0; vT < aOptions.files[vC].size(); ++vT){
            const std::string& vFile = aOptions.files[vC][vT];
            uint64_t vShape[3] = {aShape[0], aShape[1], aShape[2]};
            uint64_t vChunks[3];
            std::string vSampleType = aSampleType;
            if(vC || vT){
                Status vStatus = volumeInfo(aTiff, vFile, vShape, vChunks, vSampleType);
                if(!vStatus.ok()) return vStatus;
            }
            if(vSampleType != aSampleType){
                return Status(Code::InvalidArgument, vFile + " has sample type " + vSampleType + " but " + vFirst + " has " +
                              aSampleType + "; every input must have the same sample type.");
            }
            if(aOptions.hasBoundingBox){
                if(vBox[3] > vShape[0] || vBox[4] > vShape[1] || vBox[5] > vShape[2]){
                    const uint64_t vBoxSize[3] = {vBox[3] - vBox[0], vBox[4] - vBox[1], vBox[5] - vBox[2]};
                    return Status(Code::InvalidArgument, "The bounding box " + boxText(vBox) + " (" + sizeText(vBoxSize) +
                                  ") does not fit in " + vFile + " (" + sizeText(vShape) + ").");
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
            return Status(Code::InvalidArgument, "The bounding box " + boxText(vBox) + " must end after it starts along every axis.");
        }
    }
    if(aOptions.numThreads < 0) return Status(Code::InvalidArgument, "The number of threads cannot be negative.");
    return Status();
}

size_t dataTypeBytes(DataType aType){
    return aType == DataType::UInt8 ? 1 : aType == DataType::UInt16 ? 2 : 4;
}

bool validDataType(DataType aType){
    return aType == DataType::UInt8 || aType == DataType::UInt16 || aType == DataType::Float32 || aType == DataType::UInt32;
}

// One output file through the ImarisWriter C interface. open, writeRegion and finish make the
// file; without a successful finish the destructor stops the writer and removes the partial file.
class ImsWriter {
public:
    explicit ImsWriter(const Callbacks& aCallbacks)
        : mCallbacks(aCallbacks), mConverter(NULL), mDataType(DataType::UInt16), mBuffer(NULL), mFinished(false), mWriterFailed(false) {}
    ~ImsWriter();

    Status open(DataType aDataType, const bpConverterTypesC_Size5D& aImageSize, const bpConverterTypesC_Size5D& aBlockSize,
                const float aVoxelSize[3], const imsmeta::Metadata& aMetadata, const std::string& aOutputFile);
    // Copies the Imaris blocks [aBlockBase, aBlockLimit) of (aC, aT) from one read-block: aSrc holds
    // the image region that starts at (aX0, aY0, aZ0), with element strides aStride along X, Y, Z
    // (NULL: all zero padding).
    Status writeRegion(uint64_t aC, uint64_t aT, const uint64_t aBlockBase[3], const uint64_t aBlockLimit[3],
                       const void* aSrc, uint64_t aX0, uint64_t aY0, uint64_t aZ0, const int64_t aStride[3]);
    Status finish();

private:
    ImsWriter(const ImsWriter&);
    ImsWriter& operator=(const ImsWriter&);

    Status checkErrors();
    void writeMetadata();
    static void onProgress(bpConverterTypesC_Float aProgress, bpConverterTypesC_UInt64 aTotalBytesWritten, void* aUserData);

    const Callbacks& mCallbacks;
    bpImageConverterCPtr mConverter;
    DataType mDataType;
    bpConverterTypesC_Size5D mImageSize;
    bpConverterTypesC_Size5D mBlockSize;
    float mVoxelSize[3];
    imsmeta::Metadata mMetadata;
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

Status ImsWriter::open(DataType aDataType, const bpConverterTypesC_Size5D& aImageSize, const bpConverterTypesC_Size5D& aBlockSize,
                       const float aVoxelSize[3], const imsmeta::Metadata& aMetadata, const std::string& aOutputFile){
    mDataType = aDataType;
    mImageSize = aImageSize;
    mBlockSize = aBlockSize;
    mVoxelSize[0] = aVoxelSize[0];
    mVoxelSize[1] = aVoxelSize[1];
    mVoxelSize[2] = aVoxelSize[2];
    mMetadata = aMetadata;
    mOutputFile = aOutputFile;

    bpConverterTypesC_DataType vType = aDataType == DataType::UInt8 ? bpConverterTypesC_UInt8Type :
                                       aDataType == DataType::UInt16 ? bpConverterTypesC_UInt16Type :
                                       aDataType == DataType::UInt32 ? bpConverterTypesC_UInt32Type : bpConverterTypesC_FloatType;
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

    bpConverterTypesC_String aApplicationName = imsmeta::creatorName();   // ImarisDataSet Creator, Version
    bpConverterTypesC_String aApplicationVersion = PIW_VERSION;
    bpConverterTypesC_ProgressCallback aProgressCallback = mCallbacks.progress ? onProgress : NULL;

    mConverter = bpImageConverterC_Create(
                vType, &mImageSize, &aSample,
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
    mBuffer = malloc(vBlockSize*dataTypeBytes(mDataType));
    if(!mBuffer) return Status(Code::Internal, "Out of memory for one block of " + std::to_string(vBlockSize) + " voxels.");
    return Status();
}

Status ImsWriter::writeRegion(uint64_t aC, uint64_t aT, const uint64_t aBlockBase[3], const uint64_t aBlockLimit[3],
                              const void* aSrc, uint64_t aX0, uint64_t aY0, uint64_t aZ0, const int64_t aStride[3]){
    const int64_t sx = aStride[0], sy = aStride[1], sz = aStride[2];
    const uint64_t bx = mBlockSize.mValueX, by = mBlockSize.mValueY, bz = mBlockSize.mValueZ;
    const uint64_t shapeX = mImageSize.mValueX, shapeY = mImageSize.mValueY, shapeZ = mImageSize.mValueZ;
    bpConverterTypesC_Index5D aBlockIndex = { 0, 0, 0, (unsigned int)aC, (unsigned int)aT };
    for (uint64_t vZ = aBlockBase[2]; vZ < aBlockLimit[2]; ++vZ) {
        aBlockIndex.mValueZ = (unsigned int)vZ;
        for (uint64_t vY = aBlockBase[1]; vY < aBlockLimit[1]; ++vY) {
            aBlockIndex.mValueY = (unsigned int)vY;
            for (uint64_t vX = aBlockBase[0]; vX < aBlockLimit[0]; ++vX) {
                aBlockIndex.mValueX = (unsigned int)vX;
                switch(mDataType){
                    case DataType::UInt8:
                        fillBlock<uint8_t> ((uint8_t*)mBuffer, (const uint8_t*)aSrc, vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, sx,sy,sz);
                        bpImageConverterC_CopyBlockUInt8(mConverter, (uint8_t*)mBuffer, &aBlockIndex);
                        break;
                    case DataType::UInt16:
                        fillBlock<uint16_t>((uint16_t*)mBuffer, (const uint16_t*)aSrc, vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, sx,sy,sz);
                        bpImageConverterC_CopyBlockUInt16(mConverter, (uint16_t*)mBuffer, &aBlockIndex);
                        break;
                    case DataType::UInt32:
                        fillBlock<uint32_t>((uint32_t*)mBuffer, (const uint32_t*)aSrc, vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, sx,sy,sz);
                        bpImageConverterC_CopyBlockUInt32(mConverter, (uint32_t*)mBuffer, &aBlockIndex);
                        break;
                    case DataType::Float32:
                        fillBlock<float>   ((float*)mBuffer,    (const float*)aSrc,    vX,vY,vZ, bx,by,bz,
                                            shapeX,shapeY,shapeZ, aX0,aY0,aZ0, sx,sy,sz);
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

// The metadata resolved by imsmeta::resolveMetadata (src/imscore.h's writeImsMeta writes the
// same for the cluster path), then bpImageConverterC_Finish.
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

    // Name, and the wavelengths that were given (ImarisWriter adds Description and the colors).
    std::vector<std::string> vSectionNames(mImageSize.mValueC);
    std::vector<std::string> vTexts(2 * mImageSize.mValueC);
    std::vector<bpConverterTypesC_Parameter> vChannelParameters(3 * mImageSize.mValueC);
    for (unsigned int vC = 0; vC < mImageSize.mValueC; ++vC) {
        bpConverterTypesC_Parameter* vThisChannelParameters = &vChannelParameters[3 * vC];
        unsigned int vCount = 0;
        vThisChannelParameters[vCount].mName = "Name";
        vThisChannelParameters[vCount++].mValue = mMetadata.names[vC].c_str();
        if (mMetadata.emission[vC] > 0) {
            vTexts[2 * vC] = imsmeta::wavelengthString(mMetadata.emission[vC]);
            vThisChannelParameters[vCount].mName = "LSMEmissionWavelength";
            vThisChannelParameters[vCount++].mValue = vTexts[2 * vC].c_str();
        }
        if (mMetadata.excitation[vC] > 0) {
            vTexts[2 * vC + 1] = imsmeta::wavelengthString(mMetadata.excitation[vC]);
            vThisChannelParameters[vCount].mName = "LSMExcitationWavelength";
            vThisChannelParameters[vCount++].mValue = vTexts[2 * vC + 1].c_str();
        }
        bpConverterTypesC_ParameterSection* vChannelSection = &vParameterSections[vNumberOfOtherSections + vC];
        vSectionNames[vC] = "Channel " + std::to_string(vC);
        vChannelSection->mName = vSectionNames[vC].c_str();
        vChannelSection->mValues = vThisChannelParameters;
        vChannelSection->mValuesCount = vCount;
    }

    bpConverterTypesC_Parameters aParameters;
    aParameters.mValuesCount = vNumberOfSections;
    aParameters.mValues = vParameterSections.data();

    std::vector<bpConverterTypesC_TimeInfo> vTimeInfos(mImageSize.mValueT);
    for (unsigned int vT = 0; vT < mImageSize.mValueT; ++vT) {
        imsmeta::julianTime(mMetadata.times[vT], vTimeInfos[vT].mJulianDay, vTimeInfos[vT].mNanosecondsOfDay);
    }
    bpConverterTypesC_TimeInfos aTimeInfoPerTimePoint;
    aTimeInfoPerTimePoint.mValuesCount = mImageSize.mValueT;
    aTimeInfoPerTimePoint.mValues = vTimeInfos.data();

    std::vector<bpConverterTypesC_ColorInfo> vColorInfos(mImageSize.mValueC);
    for (unsigned int vC = 0; vC < mImageSize.mValueC; ++vC) {
        bpConverterTypesC_ColorInfo* vColor = &vColorInfos[vC];
        vColor->mIsBaseColorMode = true;
        vColor->mBaseColor.mRed = mMetadata.colors[vC].red;
        vColor->mBaseColor.mGreen = mMetadata.colors[vC].green;
        vColor->mBaseColor.mBlue = mMetadata.colors[vC].blue;
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

// The conversion itself (options validated, library mutex held). The options and the printed
// lines are along the input's axes 0, 1, 2; the writer gets Imaris X, Y, Z, and vAxis[i] is the
// input axis that becomes Imaris axis i.
Status convertValidated(const ConvertOptions& aOptions, const Callbacks& aCallbacks){
    const bool vTiff = aOptions.reader == "tiff";
    const uint64_t channels = aOptions.files.size();
    const uint64_t timepoints = aOptions.files[0].size();
    const int vAxis[3] = {aOptions.xOnImarisX ? 1 : 0, aOptions.xOnImarisX ? 0 : 1, 2};

    // The metadata first: a bad metadata option fails before any input is read.
    imsmeta::Metadata vMetadata;
    const std::string vMetadataError = imsmeta::resolveMetadata(aOptions, vMetadata);
    if(!vMetadataError.empty()) return Status(Code::InvalidArgument, vMetadataError);

    uint64_t vShape[3] = {0, 0, 0};
    uint64_t vChunks[3] = {0, 0, 0};
    std::string vSampleType;
    Status vStatus = volumeInfo(vTiff, aOptions.files[0][0], vShape, vChunks, vSampleType);
    if(!vStatus.ok()) return vStatus;
    // The type of the first input decides the .ims's (every other input must have the same).
    DataType vType = DataType::UInt16;
    if(!imsDataType(vSampleType, vType)) return unsupportedType(aOptions.files[0][0], vSampleType);
    vStatus = checkInputs(aOptions, vTiff, vShape, vSampleType);
    if(!vStatus.ok()) return vStatus;

    // Use custom chunk sizes if provided
    if(aOptions.hasBlockSize){
        vChunks[0] = aOptions.blockSize[0];
        vChunks[1] = aOptions.blockSize[1];
        vChunks[2] = aOptions.blockSize[2];
    }
    say(aCallbacks, "Block Sizes: %llu,%llu,%llu\n",
        (unsigned long long)vChunks[0], (unsigned long long)vChunks[1], (unsigned long long)vChunks[2]);
    const size_t vBytes = dataTypeBytes(vType);

    // Bounding Box
    const uint64_t vFull[3] = {vShape[0], vShape[1], vShape[2]};   // the whole input
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
    const uint64_t shapeX = vShape[vAxis[0]], shapeY = vShape[vAxis[1]], shapeZ = vShape[vAxis[2]];
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
    const float vVoxelSize[3] = {aOptions.voxelSize[vAxis[0]], aOptions.voxelSize[vAxis[1]], aOptions.voxelSize[vAxis[2]]};

    bpConverterTypesC_Size5D aBlockSize = {
        (unsigned int)vChunks[vAxis[0]], (unsigned int)vChunks[vAxis[1]], (unsigned int)vChunks[vAxis[2]], 1, 1
    };

    ImsWriter vWriter(aCallbacks);
    vStatus = vWriter.open(vType, aImageSize, aBlockSize, vVoxelSize, vMetadata, aOptions.outputFile);
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
        const uint64_t userX = aOptions.processBlock[vAxis[0]];
        const uint64_t userY = aOptions.processBlock[vAxis[1]];
        const uint64_t userZ = aOptions.processBlock[vAxis[2]];
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
            const void* vSrc = NULL;
            int64_t vStride[3] = {1, 1, 1};
            if(xN && yN && zN){
                // The region along the input's axes (inside the bounding box).
                const uint64_t vStart[3] = {x0, y0, z0}, vEnd[3] = {xEnd, yEnd, zEnd};
                uint64_t inStart[3], inEnd[3];
                for(int i = 0; i < 3; ++i){
                    inStart[vAxis[i]] = boundingBox[vAxis[i]] + vStart[i];
                    inEnd[vAxis[i]] = boundingBox[vAxis[i]] + vEnd[i];
                }
                std::string vWhy;
                try{
                    if(vTiff){
                        // whole pages, the region's Z-slab; a read of every page uses the original
                        // call so the default (no -C) path stays byte-identical (and ImageJ-safe).
                        if(inStart[2] == 0 && inEnd[2] == vFull[2]) vData = readTiffParallelWrapper(fileName);
                        else vData = readTiffParallelWrapper(fileName, {inStart[2], inEnd[2]});
                    }
                    else vData = readZarrParallelHelper(fileName,
                                     inStart[0], inStart[1], inStart[2], inEnd[0], inEnd[1], inEnd[2], 0);
                }
                catch(const std::string& vError){ vWhy = vError; }
                catch(const std::exception& vError){ vWhy = vError.what(); }
                catch(...){ vWhy = "unknown error"; }
                // The readers return NULL on failure; don't let it zero-fill like padding.
                if(!vData) return Status(Code::ReadFailed, "could not read " + vFile + (vWhy.empty() ? "" : ": " + vWhy));
                // The buffer runs along the input's axes, axis 0 fastest. A TIFF read holds whole
                // pages, so the region starts at the box's row and column inside them.
                const uint64_t vRows = vTiff ? vFull[0] : inEnd[0] - inStart[0];
                const uint64_t vCols = vTiff ? vFull[1] : inEnd[1] - inStart[1];
                const int64_t vInStride[3] = {1, (int64_t)vRows, (int64_t)(vRows*vCols)};
                for(int i = 0; i < 3; ++i) vStride[i] = vInStride[vAxis[i]];
                const uint64_t vOffset = vTiff ? inStart[0] + inStart[1]*vRows : 0;
                vSrc = (const char*)vData + vOffset*vBytes;
            }
            ReadBuffer vHold(vData);

            const uint64_t vBase[3] = {vXbase, vYbase, vZbase};
            const uint64_t vLimit[3] = {vXlimit, vYlimit, vZlimit};
            vStatus = vWriter.writeRegion(vC, vT, vBase, vLimit, vSrc, x0, y0, z0, vStride);
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

// The inputs a reader takes, by extension: tiff .tif and .tiff in any case, zarr .zarr; an empty
// reader takes both.
bool isTiffName(const char* aName){
    const char* vDot = strrchr(aName, '.');
    if(!vDot) return false;
    std::string vExt(vDot + 1);
    for(size_t i = 0; i < vExt.size(); ++i) vExt[i] = (char)tolower((unsigned char)vExt[i]);
    return vExt == "tif" || vExt == "tiff";
}
bool isZarrName(const char* aName){
    const char* vDot = strrchr(aName, '.');
    return vDot && !strcmp(vDot, ".zarr");
}

// One folder's entries that the reader takes, appended to aEntries as (name, path).
Status listFolder(const std::string& aFolder, const std::string& aReader,
                  std::vector<std::pair<std::string, std::string> >& aEntries){
    DIR* dir = opendir(aFolder.c_str());
    if (!dir){
        const int vErrno = errno;
        if(vErrno == ENOENT || vErrno == ENOTDIR){
            return Status(Code::NotFound, "Input folder \"" + aFolder + "\" does not exist or is not a folder.");
        }
        return Status(Code::ReadFailed, "Could not open input folder \"" + aFolder + "\": " + strerror(vErrno) + ".");
    }
    struct dirent* dp;
    while ((dp = readdir(dir)) != NULL)
    {
        if(!strcmp(dp->d_name, ".") || !strcmp(dp->d_name, "..")) continue;
        const bool vTake = aReader == "tiff" ? isTiffName(dp->d_name) : aReader == "zarr" ? isZarrName(dp->d_name) :
                           isTiffName(dp->d_name) || isZarrName(dp->d_name);
        if(!vTake) continue;
        #ifdef _WIN32
        aEntries.push_back(std::make_pair(std::string(dp->d_name), aFolder + "\\" + dp->d_name));
        #else
        aEntries.push_back(std::make_pair(std::string(dp->d_name), aFolder + "/" + dp->d_name));
        #endif
    }
    closedir(dir);
    return Status();
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

Status validateWrite(const WriteOptions& aOptions, const std::vector<Volume>& aVolumes){
    if(aOptions.outputFile.empty()) return Status(Code::InvalidArgument, "No output file was given.");
    if(!validDataType(aOptions.dataType)) return Status(Code::InvalidArgument, "Unknown data type " +
                                                        std::to_string((int)aOptions.dataType) + ".");
    for(int i = 0; i < 3; ++i){
        if(!aOptions.size[i] || aOptions.size[i] > UINT_MAX){
            return Status(Code::InvalidArgument, "The volume size " + sizeText(aOptions.size) + " must be 1 to " +
                          std::to_string(UINT_MAX) + " along every axis.");
        }
        if(aOptions.hasBlockSize && (!aOptions.blockSize[i] || aOptions.blockSize[i] > UINT_MAX)){
            return Status(Code::InvalidArgument, "Block sizes must be 1 to " + std::to_string(UINT_MAX) + ".");
        }
    }
    if(!aOptions.channels || !aOptions.timepoints){
        return Status(Code::InvalidArgument, "At least one channel and one time point are needed.");
    }
    const uint64_t vCount = (uint64_t)aOptions.channels*aOptions.timepoints;
    if(aVolumes.size() != vCount){
        return Status(Code::InvalidArgument, std::to_string(aVolumes.size()) + " volumes were given for " +
                      std::to_string(aOptions.channels) + " channels x " + std::to_string(aOptions.timepoints) +
                      " time points; one per channel and time point is needed.");
    }
    if(aOptions.numThreads < 0) return Status(Code::InvalidArgument, "The number of threads cannot be negative.");
    for(size_t vI = 0; vI < aVolumes.size(); ++vI){
        const Volume& vVolume = aVolumes[vI];
        if(!vVolume.data) return Status(Code::InvalidArgument, "Volume " + std::to_string(vI) + " has no data.");
        // Positive strides, and the farthest element's offset must fit in 63 bits.
        uint64_t vLast = 0;
        for(int i = 0; i < 3; ++i){
            if(vVolume.stride[i] <= 0){
                return Status(Code::InvalidArgument, "Volume " + std::to_string(vI) + " has stride " +
                              std::to_string((long long)vVolume.stride[i]) + " along axis " + std::to_string(i) +
                              "; strides must be positive.");
            }
            const uint64_t vSpan = aOptions.size[i] - 1, vStep = (uint64_t)vVolume.stride[i];
            if(vSpan && vStep > ((uint64_t)INT64_MAX - vLast)/vSpan){
                return Status(Code::InvalidArgument, "Volume " + std::to_string(vI) + ": its strides overflow.");
            }
            vLast += vSpan*vStep;
        }
    }
    return Status();
}

// The write itself (options validated, library mutex held).
Status writeValidated(const WriteOptions& aOptions, const std::vector<Volume>& aVolumes, const Callbacks& aCallbacks){
    // The metadata first: a bad metadata option fails before the output is created.
    imsmeta::Metadata vMetadata;
    const std::string vMetadataError = imsmeta::resolveMetadataFields(aOptions, aOptions.channels, aOptions.timepoints, NULL, vMetadata);
    if(!vMetadataError.empty()) return Status(Code::InvalidArgument, vMetadataError);
    const uint64_t vBlock[3] = {aOptions.hasBlockSize ? aOptions.blockSize[0] : 64,
                                aOptions.hasBlockSize ? aOptions.blockSize[1] : 64,
                                aOptions.hasBlockSize ? aOptions.blockSize[2] : 64};
    say(aCallbacks, "Block Sizes: %llu,%llu,%llu\n",
        (unsigned long long)vBlock[0], (unsigned long long)vBlock[1], (unsigned long long)vBlock[2]);
    say(aCallbacks, "Voxel Sizes: %f,%f,%f\n", aOptions.voxelSize[0], aOptions.voxelSize[1], aOptions.voxelSize[2]);
    const bpConverterTypesC_Size5D aImageSize = {
        (unsigned int)aOptions.size[0], (unsigned int)aOptions.size[1], (unsigned int)aOptions.size[2],
        aOptions.channels, aOptions.timepoints
    };
    const bpConverterTypesC_Size5D aBlockSize = {(unsigned int)vBlock[0], (unsigned int)vBlock[1], (unsigned int)vBlock[2], 1, 1};

    ImsWriter vWriter(aCallbacks);
    Status vStatus = vWriter.open(aOptions.dataType, aImageSize, aBlockSize, aOptions.voxelSize, vMetadata, aOptions.outputFile);
    if(!vStatus.ok()) return vStatus;
    const uint64_t vBase[3] = {0, 0, 0};
    const uint64_t vLimit[3] = {numBlocks(aOptions.size[0], vBlock[0]), numBlocks(aOptions.size[1], vBlock[1]),
                                numBlocks(aOptions.size[2], vBlock[2])};
    // Channels outside, time points inside, as the converter walks its files.
    for(unsigned vC = 0; vC < aOptions.channels; ++vC){
        for(unsigned vT = 0; vT < aOptions.timepoints; ++vT){
            say(aCallbacks, "Current Volume: channel %u, time point %u\n", vC, vT);
            if(cancelled(aCallbacks)) return cancelledStatus();
            const Volume& vVolume = aVolumes[(size_t)vC*aOptions.timepoints + vT];
            vStatus = vWriter.writeRegion(vC, vT, vBase, vLimit, vVolume.data, 0, 0, 0, vVolume.stride);
            if(!vStatus.ok()) return vStatus;
        }
    }
    return vWriter.finish();
}

}  // namespace

Status findInputFiles(const std::vector<std::string>& folders, const std::vector<std::string>& patterns,
                      const std::string& reader, std::vector<std::vector<std::string> >& files) noexcept{
    try{
        if(!reader.empty() && reader != "tiff" && reader != "zarr"){
            return Status(Code::InvalidArgument, "Reader type \"" + reader + "\" is not a supported reader type.");
        }
        std::vector<std::pair<std::string, std::string> > vEntries;   // (name, path) in every folder
        for(size_t vF = 0; vF < folders.size(); ++vF){
            Status vStatus = listFolder(folders[vF], reader, vEntries);
            if(!vStatus.ok()) return vStatus;
        }
        std::vector<std::vector<std::string> > vFiles(patterns.size());
        for(size_t vE = 0; vE < vEntries.size(); ++vE){
            size_t vMatch = patterns.size();
            for(size_t vP = 0; vP < patterns.size(); ++vP){
                if(!strstr(vEntries[vE].first.c_str(), patterns[vP].c_str())) continue;
                if(vMatch != patterns.size()){
                    return Status(Code::InvalidArgument, vEntries[vE].second + " matches both pattern \"" + patterns[vMatch] +
                                  "\" and pattern \"" + patterns[vP] + "\"; every input must match one pattern.");
                }
                vMatch = vP;
            }
            if(vMatch != patterns.size()) vFiles[vMatch].push_back(vEntries[vE].second);
        }
        for(size_t vP = 0; vP < patterns.size(); ++vP){
            std::vector<std::string>& vChannel = vFiles[vP];
            if(vChannel.empty()){
                return Status(Code::NotFound, "Given pattern \"" + patterns[vP] + "\" was not found in any of the provided folders.");
            }
            std::sort(vChannel.begin(), vChannel.end(), lessPath);
            for(size_t vT = 1; vT < vChannel.size(); ++vT){
                if(vChannel[vT] == vChannel[vT - 1]){
                    return Status(Code::InvalidArgument, vChannel[vT] + " was found twice (is a folder given twice?).");
                }
            }
            if(reader.empty()){
                const bool vTiff = isTiffName(vChannel[0].c_str());
                for(size_t vT = 1; vT < vChannel.size(); ++vT){
                    if(isTiffName(vChannel[vT].c_str()) != vTiff){
                        return Status(Code::InvalidArgument, "Pattern \"" + patterns[vP] + "\" matches both TIFF and Zarr inputs (" +
                                      vChannel[0] + ", " + vChannel[vT] + "); give the reader.");
                    }
                }
            }
        }
        for(size_t vP = 1; vP < patterns.size(); ++vP){
            if(vFiles[vP].size() == vFiles[0].size()) continue;
            std::string vCounts;
            for(size_t vQ = 0; vQ < patterns.size(); ++vQ){
                vCounts += (vQ ? ", \"" : "\"") + patterns[vQ] + "\" " + std::to_string(vFiles[vQ].size());
            }
            return Status(Code::InvalidArgument, "The channels have different numbers of files: " + vCounts +
                          "; every channel needs one file per time point.");
        }
        files.swap(vFiles);
        return Status();
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status findInputFiles(const std::vector<std::string>& folders, const std::vector<std::string>& patterns,
                      std::vector<std::vector<std::string> >& files) noexcept{
    return findInputFiles(folders, patterns, std::string(), files);
}

Status convert(const ConvertOptions& options, const Callbacks& callbacks) noexcept{
    try{
        Status vStatus = validate(options);
        if(!vStatus.ok()) return vStatus;
        std::lock_guard<std::mutex> vLock(detail::libraryMutex());
        ThreadCount vThreads(options.numThreads);
        return convertValidated(options, callbacks);
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status writeVolumes(const WriteOptions& options, const std::vector<Volume>& volumes, const Callbacks& callbacks) noexcept{
    try{
        Status vStatus = validateWrite(options, volumes);
        if(!vStatus.ok()) return vStatus;
        std::lock_guard<std::mutex> vLock(libraryMutex());
        ThreadCount vThreads(options.numThreads);
        return writeValidated(options, volumes, callbacks);
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
