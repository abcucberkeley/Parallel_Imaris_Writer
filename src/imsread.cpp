// Reading back .ims files: piw::imsInfo and piw::readIms (src/parallelimariswriter.h).
//
// An Imaris 5.5 .ims file is HDF5: /DataSet/ResolutionLevel l/TimePoint t/Channel c holds a
// chunked 3-D Data dataset (dims z, y, x) next to its Histogram datasets, and /DataSetInfo holds
// the metadata as string attributes. ImarisWriter compresses every chunk itself (byte shuffle for
// 2- and 4-byte integers, then LZ4 in the HDF5 lz4 filter format, id 32004) and writes it with
// H5Dwrite_chunk, so the chunks are read raw here (H5Dread_chunk) and decoded here: no HDF5
// filter needs to be registered. Raw reads are serial; decoding runs in an OpenMP loop.
// Like the rest of the core: no printing (HDF5's error stack is silenced for each call), no exit,
// errors come back as a Status, and every call holds the library mutex (HDF5 here is not
// thread-safe).
#include "parallelimariswriter.h"
#include "hdf5.h"
#include <lz4.h>
#include <zlib.h>
#include <omp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <algorithm>
#include <exception>
#include <mutex>
#include <string>
#include <vector>

namespace piw {
namespace detail {
std::mutex& libraryMutex();   // src/converter.cpp
}

namespace {

// Owns one HDF5 id and closes it with the matching H5?close.
class Hid {
public:
    Hid() : mId(-1), mClose(NULL) {}
    Hid(hid_t aId, herr_t (*aClose)(hid_t)) : mId(aId), mClose(aClose) {}
    ~Hid(){ reset(-1, NULL); }
    void reset(hid_t aId, herr_t (*aClose)(hid_t)){
        if(mId >= 0 && mClose) mClose(mId);
        mId = aId;
        mClose = aClose;
    }
    bool valid() const { return mId >= 0; }
    operator hid_t() const { return mId; }
private:
    Hid(const Hid&);
    Hid& operator=(const Hid&);
    hid_t mId;
    herr_t (*mClose)(hid_t);
};

// Turns off HDF5's error-stack printing for one call (missing objects are probed on purpose).
class QuietErrors {
public:
    QuietErrors() : mFunc(NULL), mData(NULL) {
        H5Eget_auto2(H5E_DEFAULT, &mFunc, &mData);
        H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
    }
    ~QuietErrors(){ H5Eset_auto2(H5E_DEFAULT, mFunc, mData); }
private:
    QuietErrors(const QuietErrors&);
    QuietErrors& operator=(const QuietErrors&);
    H5E_auto2_t mFunc;
    void* mData;
};

const H5Z_filter_t kFilterLZ4 = 32004;

Status internalError(const char* aWhat){
    return Status(Code::Internal, std::string("Internal error: ") + aWhat);
}

std::string datasetGroup(unsigned aLevel, unsigned aTimepoint, unsigned aChannel){
    return "/DataSet/ResolutionLevel " + std::to_string(aLevel) + "/TimePoint " + std::to_string(aTimepoint) +
           "/Channel " + std::to_string(aChannel);
}

// True if every component of an absolute path exists (H5Lexists fails on a missing parent).
bool pathExists(hid_t aFile, const std::string& aPath){
    if(aPath.empty() || aPath == "/") return true;
    size_t vPos = 0;
    while(vPos != std::string::npos){
        vPos = aPath.find('/', vPos + 1);
        if(H5Lexists(aFile, aPath.substr(0, vPos).c_str(), H5P_DEFAULT) <= 0) return false;
    }
    return H5Oexists_by_name(aFile, aPath.c_str(), H5P_DEFAULT) > 0;
}

Status openFile(const std::string& aFile, Hid& aId){
    struct stat vInfo;
    if(stat(aFile.c_str(), &vInfo) != 0) return Status(Code::NotFound, aFile + " does not exist.");
    hid_t vId = H5Fopen(aFile.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
    if(vId < 0) return Status(Code::ReadFailed, "could not open " + aFile + " as an HDF5 file");
    aId.reset(vId, H5Fclose);
    return Status();
}

void appendNumber(std::string& aText, const char* aFormat, ...)
#if defined(__GNUC__) && !defined(_WIN32)
    __attribute__((format(printf, 2, 3)))
#endif
    ;
void appendNumber(std::string& aText, const char* aFormat, ...){
    char vBuffer[64];
    va_list vArgs;
    va_start(vArgs, aFormat);
    vsnprintf(vBuffer, sizeof(vBuffer), aFormat, vArgs);
    va_end(vArgs);
    if(!aText.empty()) aText += ' ';
    aText += vBuffer;
}

// An attribute as text: strings as they are (.ims attributes are 1-D arrays of 1-byte strings
// without a NUL; trailing NUL padding is dropped), numbers separated by spaces.
bool attributeText(hid_t aObject, const char* aName, std::string& aText){
    aText.clear();
    Hid vAttr(H5Aopen(aObject, aName, H5P_DEFAULT), H5Aclose);
    if(!vAttr.valid()) return false;
    Hid vType(H5Aget_type(vAttr), H5Tclose), vSpace(H5Aget_space(vAttr), H5Sclose);
    if(!vType.valid() || !vSpace.valid()) return false;
    hssize_t vCount = H5Sget_simple_extent_npoints(vSpace);
    if(vCount < 0) return false;
    const H5T_class_t vClass = H5Tget_class(vType);
    if(vClass == H5T_STRING && H5Tis_variable_str(vType) > 0){
        std::vector<char*> vStrings((size_t)vCount, (char*)NULL);
        Hid vMemType(H5Tcopy(H5T_C_S1), H5Tclose);
        H5Tset_size(vMemType, H5T_VARIABLE);
        if(vCount && H5Aread(vAttr, vMemType, vStrings.data()) < 0) return false;
        for(size_t i = 0; i < vStrings.size(); ++i){
            if(i) aText += '\n';
            if(vStrings[i]) aText += vStrings[i];
        }
        if(vCount) H5Treclaim(vMemType, vSpace, H5P_DEFAULT, vStrings.data());
        return true;
    }
    if(vClass == H5T_STRING){
        std::vector<char> vBytes((size_t)vCount*H5Tget_size(vType) + 1, 0);
        if(vCount && H5Aread(vAttr, vType, vBytes.data()) < 0) return false;
        aText.assign(vBytes.data(), vBytes.size() - 1);
        const size_t vNul = aText.find('\0');
        if(vNul != std::string::npos) aText.erase(vNul);
        return true;
    }
    if(vClass == H5T_INTEGER){
        const bool vSigned = H5Tget_sign(vType) == H5T_SGN_2;
        if(vSigned){
            std::vector<long long> vValues((size_t)vCount);
            if(vCount && H5Aread(vAttr, H5T_NATIVE_LLONG, vValues.data()) < 0) return false;
            for(size_t i = 0; i < vValues.size(); ++i) appendNumber(aText, "%lld", vValues[i]);
        } else {
            std::vector<unsigned long long> vValues((size_t)vCount);
            if(vCount && H5Aread(vAttr, H5T_NATIVE_ULLONG, vValues.data()) < 0) return false;
            for(size_t i = 0; i < vValues.size(); ++i) appendNumber(aText, "%llu", vValues[i]);
        }
        return true;
    }
    if(vClass == H5T_FLOAT){
        const bool vSingle = H5Tget_size(vType) <= 4;
        std::vector<double> vValues((size_t)vCount);
        if(vCount && H5Aread(vAttr, H5T_NATIVE_DOUBLE, vValues.data()) < 0) return false;
        for(size_t i = 0; i < vValues.size(); ++i) appendNumber(aText, vSingle ? "%.9g" : "%.17g", vValues[i]);
        return true;
    }
    aText = "<attribute of HDF5 type class " + std::to_string((int)vClass) + ">";
    return true;
}

// The names of an object's attributes, or of a group's links, in name order.
std::vector<std::string> attributeNames(hid_t aObject){
    std::vector<std::string> vNames;
    H5O_info2_t vInfo;
    if(H5Oget_info3(aObject, &vInfo, H5O_INFO_NUM_ATTRS) < 0) return vNames;
    for(hsize_t i = 0; i < vInfo.num_attrs; ++i){
        ssize_t vLength = H5Aget_name_by_idx(aObject, ".", H5_INDEX_NAME, H5_ITER_INC, i, NULL, 0, H5P_DEFAULT);
        if(vLength < 0) continue;
        std::vector<char> vName((size_t)vLength + 1, 0);
        H5Aget_name_by_idx(aObject, ".", H5_INDEX_NAME, H5_ITER_INC, i, vName.data(), vName.size(), H5P_DEFAULT);
        vNames.push_back(vName.data());
    }
    return vNames;
}

std::vector<std::string> linkNames(hid_t aGroup){
    std::vector<std::string> vNames;
    H5G_info_t vInfo;
    if(H5Gget_info(aGroup, &vInfo) < 0) return vNames;
    for(hsize_t i = 0; i < vInfo.nlinks; ++i){
        ssize_t vLength = H5Lget_name_by_idx(aGroup, ".", H5_INDEX_NAME, H5_ITER_INC, i, NULL, 0, H5P_DEFAULT);
        if(vLength < 0) continue;
        std::vector<char> vName((size_t)vLength + 1, 0);
        H5Lget_name_by_idx(aGroup, ".", H5_INDEX_NAME, H5_ITER_INC, i, vName.data(), vName.size(), H5P_DEFAULT);
        vNames.push_back(vName.data());
    }
    return vNames;
}

// Every attribute of the object at aPath and below it, depth first in name order. Hard and
// external links are followed (a linkparts .ims holds its data behind external links); soft
// links are not (they could make a cycle).
void collectAttributes(hid_t aFile, const std::string& aPath, std::vector<std::pair<std::string, std::string> >& aOut){
    Hid vObject(H5Oopen(aFile, aPath.c_str(), H5P_DEFAULT), H5Oclose);
    if(!vObject.valid()) return;
    const std::string vPrefix = aPath == "/" ? "/" : aPath + "/";
    const std::vector<std::string> vAttributes = attributeNames(vObject);
    for(size_t i = 0; i < vAttributes.size(); ++i){
        std::string vText;
        if(!attributeText(vObject, vAttributes[i].c_str(), vText)) vText = "<unreadable>";
        aOut.push_back(std::make_pair(vPrefix + vAttributes[i], vText));
    }
    H5O_info2_t vInfo;
    if(H5Oget_info3(vObject, &vInfo, H5O_INFO_BASIC) < 0 || vInfo.type != H5O_TYPE_GROUP) return;
    const std::vector<std::string> vLinks = linkNames(vObject);
    for(size_t i = 0; i < vLinks.size(); ++i){
        H5L_info2_t vLink;
        if(H5Lget_info2(vObject, vLinks[i].c_str(), &vLink, H5P_DEFAULT) < 0 || vLink.type == H5L_TYPE_SOFT) continue;
        if(H5Oexists_by_name(vObject, vLinks[i].c_str(), H5P_DEFAULT) <= 0) continue;
        collectAttributes(aFile, vPrefix + vLinks[i], aOut);
    }
}

std::string objectAttribute(hid_t aFile, const std::string& aPath, const char* aName){
    std::string vText;
    if(!pathExists(aFile, aPath)) return vText;
    Hid vObject(H5Oopen(aFile, aPath.c_str(), H5P_DEFAULT), H5Oclose);
    if(vObject.valid() && H5Aexists(vObject, aName) > 0) attributeText(vObject, aName, vText);
    return vText;
}

Status dataTypeOf(hid_t aDataset, const std::string& aPath, DataType& aType){
    Hid vType(H5Dget_type(aDataset), H5Tclose);
    if(!vType.valid()) return Status(Code::ReadFailed, "could not read the type of " + aPath);
    const H5T_class_t vClass = H5Tget_class(vType);
    const size_t vSize = H5Tget_size(vType);
    if(vClass == H5T_INTEGER && H5Tget_sign(vType) == H5T_SGN_NONE){
        if(vSize == 1){ aType = DataType::UInt8; return Status(); }
        if(vSize == 2){ aType = DataType::UInt16; return Status(); }
        if(vSize == 4){ aType = DataType::UInt32; return Status(); }
    }
    if(vClass == H5T_FLOAT && vSize == 4){ aType = DataType::Float32; return Status(); }
    return Status(Code::Unsupported, aPath + " holds a sample type other than uint8, uint16, uint32 or float32.");
}

const char* dataTypeName(DataType aType){
    switch(aType){
        case DataType::UInt8: return "uint8";
        case DataType::UInt16: return "uint16";
        case DataType::UInt32: return "uint32";
        case DataType::Float32: return "float32";
    }
    return "unknown";
}

size_t dataTypeBytes(DataType aType){
    return aType == DataType::UInt8 ? 1 : aType == DataType::UInt16 ? 2 : 4;
}

// ImageSizeX/Y/Z of a dataset group.
Status imageSize(hid_t aFile, const std::string& aGroup, uint64_t aSize[3]){
    static const char* const kNames[3] = {"ImageSizeX", "ImageSizeY", "ImageSizeZ"};
    for(int i = 0; i < 3; ++i){
        const std::string vText = objectAttribute(aFile, aGroup, kNames[i]);
        char* vEnd = NULL;
        aSize[i] = strtoull(vText.c_str(), &vEnd, 10);
        if(vText.empty() || *vEnd || !aSize[i]) return Status(Code::ReadFailed, aGroup + " has no usable " + kNames[i] + ".");
    }
    return Status();
}

// The filter pipeline of a Data dataset: only shuffle and LZ4 are decoded here.
struct Pipeline {
    std::vector<H5Z_filter_t> filters;   // in the order they were applied when writing
    size_t shuffleBytes = 0;
};

Status pipelineOf(hid_t aDataset, const std::string& aPath, Pipeline& aPipeline){
    Hid vPlist(H5Dget_create_plist(aDataset), H5Pclose);
    if(!vPlist.valid()) return Status(Code::ReadFailed, "could not read the creation properties of " + aPath);
    if(H5Pget_layout(vPlist) != H5D_CHUNKED) return Status(Code::Unsupported, aPath + " is not chunked.");
    const int vCount = H5Pget_nfilters(vPlist);
    for(int i = 0; i < vCount; ++i){
        unsigned vFlags = 0, vConfig = 0;
        size_t vNValues = 8;
        unsigned vValues[8] = {0};
        char vName[64];
        const H5Z_filter_t vId = H5Pget_filter2(vPlist, (unsigned)i, &vFlags, &vNValues, vValues, sizeof(vName), vName, &vConfig);
        if(vId == H5Z_FILTER_SHUFFLE) aPipeline.shuffleBytes = vNValues && vValues[0] ? vValues[0] : 1;
        else if(vId != kFilterLZ4){
            return Status(Code::Unsupported, aPath + " uses HDF5 filter " + std::to_string((int)vId) +
                          "; only files with LZ4 chunks (as this library writes) can be read.");
        }
        aPipeline.filters.push_back(vId);
    }
    return Status();
}

uint64_t readBigEndian(const uint8_t* aBytes, int aCount){
    uint64_t vValue = 0;
    for(int i = 0; i < aCount; ++i) vValue = (vValue << 8) | aBytes[i];
    return vValue;
}

// Undoes the lz4 filter: [8 B raw size][4 B block size], then per block [4 B compressed size]
// [data], all big-endian; a block stored with its raw size is not compressed.
bool lz4Decode(const std::vector<uint8_t>& aIn, std::vector<uint8_t>& aOut){
    if(aIn.size() < 12) return false;
    const uint64_t vTotal = readBigEndian(aIn.data(), 8), vBlock = readBigEndian(aIn.data() + 8, 4);
    if(!vBlock || vTotal > ((uint64_t)1 << 40)) return false;
    aOut.resize((size_t)vTotal);
    size_t vIn = 12, vOut = 0;
    while(vOut < vTotal){
        const size_t vWant = (size_t)std::min<uint64_t>(vBlock, vTotal - vOut);
        if(vIn + 4 > aIn.size()) return false;
        const size_t vCompressed = (size_t)readBigEndian(aIn.data() + vIn, 4);
        vIn += 4;
        if(vIn + vCompressed > aIn.size()) return false;
        if(vCompressed == vWant){
            memcpy(aOut.data() + vOut, aIn.data() + vIn, vWant);
        } else {
            const int vGot = LZ4_decompress_safe((const char*)aIn.data() + vIn, (char*)aOut.data() + vOut, (int)vCompressed, (int)vWant);
            if(vGot != (int)vWant) return false;
        }
        vIn += vCompressed;
        vOut += vWant;
    }
    return true;
}

// Undoes the HDF5 byte shuffle: byte b of element k was stored at b*n + k.
void unshuffle(const std::vector<uint8_t>& aIn, std::vector<uint8_t>& aOut, size_t aBytes){
    aOut.resize(aIn.size());
    const size_t vN = aIn.size()/aBytes;
    for(size_t b = 0; b < aBytes; ++b){
        const uint8_t* vSrc = aIn.data() + b*vN;
        uint8_t* vDst = aOut.data() + b;
        for(size_t k = 0; k < vN; ++k) vDst[k*aBytes] = vSrc[k];
    }
    if(vN*aBytes < aIn.size()) memcpy(aOut.data() + vN*aBytes, aIn.data() + vN*aBytes, aIn.size() - vN*aBytes);
}

// Decodes one stored chunk into aOut (aWork is scratch); false if it does not decode to aBytes.
bool decodeChunk(const Pipeline& aPipeline, unsigned aMask, std::vector<uint8_t>& aChunk, std::vector<uint8_t>& aWork,
                 size_t aBytes){
    for(int i = (int)aPipeline.filters.size() - 1; i >= 0; --i){
        if(aMask & (1u << i)) continue;   // bit i set: filter i was skipped for this chunk
        if(aPipeline.filters[i] == kFilterLZ4){
            if(!lz4Decode(aChunk, aWork)) return false;
        } else if(aPipeline.shuffleBytes > 1){
            unshuffle(aChunk, aWork, aPipeline.shuffleBytes);
        } else continue;
        aChunk.swap(aWork);
    }
    return aChunk.size() == aBytes;
}

// Copies the part of a decoded chunk (dims aChunk, X fastest, at aOrigin) inside [aStart, aEnd)
// to the output; with aData NULL it writes zeros there (a chunk that is not stored).
template<typename T>
void scatterChunk(const T* aData, const uint64_t aChunk[3], const uint64_t aOrigin[3], const uint64_t aStart[3],
                  const uint64_t aEnd[3], T* aOut, const int64_t aStride[3]){
    uint64_t vLo[3], vHi[3];
    for(int i = 0; i < 3; ++i){
        vLo[i] = std::max(aOrigin[i], aStart[i]);
        vHi[i] = std::min(aOrigin[i] + aChunk[i], aEnd[i]);
        if(vLo[i] >= vHi[i]) return;
    }
    const uint64_t vN = vHi[0] - vLo[0];
    for(uint64_t z = vLo[2]; z < vHi[2]; ++z){
        for(uint64_t y = vLo[1]; y < vHi[1]; ++y){
            T* vDst = aOut + (int64_t)(vLo[0] - aStart[0])*aStride[0] + (int64_t)(y - aStart[1])*aStride[1] +
                      (int64_t)(z - aStart[2])*aStride[2];
            const T* vSrc = aData ? aData + (vLo[0] - aOrigin[0]) + (y - aOrigin[1])*aChunk[0] +
                                    (z - aOrigin[2])*aChunk[0]*aChunk[1] : NULL;
            if(aStride[0] == 1){
                if(vSrc) memcpy(vDst, vSrc, vN*sizeof(T));
                else memset(vDst, 0, vN*sizeof(T));
            } else if(vSrc){
                for(uint64_t x = 0; x < vN; ++x) vDst[(int64_t)x*aStride[0]] = vSrc[x];
            } else {
                for(uint64_t x = 0; x < vN; ++x) vDst[(int64_t)x*aStride[0]] = 0;
            }
        }
    }
}

void scatterChunk(size_t aBytes, const void* aData, const uint64_t aChunk[3], const uint64_t aOrigin[3],
                  const uint64_t aStart[3], const uint64_t aEnd[3], void* aOut, const int64_t aStride[3]){
    if(aBytes == 1) scatterChunk((const uint8_t*)aData, aChunk, aOrigin, aStart, aEnd, (uint8_t*)aOut, aStride);
    else if(aBytes == 2) scatterChunk((const uint16_t*)aData, aChunk, aOrigin, aStart, aEnd, (uint16_t*)aOut, aStride);
    else scatterChunk((const uint32_t*)aData, aChunk, aOrigin, aStart, aEnd, (uint32_t*)aOut, aStride);
}

// --- checksums ---

void putLittleEndian(uint8_t* aBytes, uint64_t aValue){
    for(int i = 0; i < 8; ++i) aBytes[i] = (uint8_t)(aValue >> (8*i));
}

uLong crcBytes(uLong aCrc, const void* aData, size_t aSize){
    const Bytef* vData = (const Bytef*)aData;
    while(aSize){
        const uInt vPart = (uInt)std::min<size_t>(aSize, 1u << 30);
        aCrc = crc32(aCrc, vData, vPart);
        vData += vPart;
        aSize -= vPart;
    }
    return aCrc;
}

struct StoredChunk { hsize_t offset[3]; haddr_t address; hsize_t size; uLong crc; };

int collectChunk(const hsize_t* aOffset, unsigned, haddr_t aAddress, hsize_t aSize, void* aUser){
    StoredChunk vChunk;
    vChunk.offset[0] = aOffset[0];
    vChunk.offset[1] = aOffset[1];
    vChunk.offset[2] = aOffset[2];
    vChunk.address = aAddress;
    vChunk.size = aSize;
    vChunk.crc = 0;
    ((std::vector<StoredChunk>*)aUser)->push_back(vChunk);
    return 0;
}

bool byAddress(const StoredChunk& aA, const StoredChunk& aB){ return aA.address < aB.address; }
bool byOffset(const StoredChunk& aA, const StoredChunk& aB){
    return std::lexicographical_compare(aA.offset, aA.offset + 3, aB.offset, aB.offset + 3);
}

// The CRC of a Data dataset (see ImsDataset::crc). The chunks are read in file order and their
// records' CRCs combined in offset order (crc32_combine), which gives the CRC of the records
// concatenated in offset order.
Status dataCrc(hid_t aDataset, const std::string& aPath, uint64_t& aChunks, uint32_t& aCrc){
    std::vector<StoredChunk> vChunks;
    if(H5Dchunk_iter(aDataset, H5P_DEFAULT, collectChunk, &vChunks) < 0) return Status(Code::ReadFailed, "could not list the chunks of " + aPath);
    aChunks = vChunks.size();
    std::sort(vChunks.begin(), vChunks.end(), byAddress);
    std::vector<uint8_t> vBytes;
    for(size_t i = 0; i < vChunks.size(); ++i){
        StoredChunk& vChunk = vChunks[i];
        uint8_t vHeader[32];
        for(int a = 0; a < 3; ++a) putLittleEndian(vHeader + 8*a, vChunk.offset[a]);
        putLittleEndian(vHeader + 24, vChunk.size);
        vBytes.resize((size_t)vChunk.size);
        uint32_t vMask = 0;
        if(vChunk.size && H5Dread_chunk(aDataset, H5P_DEFAULT, vChunk.offset, &vMask, vBytes.data()) < 0){
            return Status(Code::ReadFailed, "could not read a chunk of " + aPath);
        }
        vChunk.crc = crcBytes(crcBytes(crc32(0L, Z_NULL, 0), vHeader, sizeof(vHeader)), vBytes.data(), vBytes.size());
    }
    std::sort(vChunks.begin(), vChunks.end(), byOffset);
    uLong vCrc = crc32(0L, Z_NULL, 0);
    for(size_t i = 0; i < vChunks.size(); ++i) vCrc = crc32_combine(vCrc, vChunks[i].crc, (z_off_t)(32 + vChunks[i].size));
    aCrc = (uint32_t)vCrc;
    return Status();
}

// Adds the values of a small dataset (a histogram) as 8-byte little-endian integers.
bool crcHistogram(hid_t aGroup, const char* aName, uLong& aCrc){
    if(H5Lexists(aGroup, aName, H5P_DEFAULT) <= 0) return true;
    Hid vData(H5Dopen2(aGroup, aName, H5P_DEFAULT), H5Dclose);
    if(!vData.valid()) return false;
    Hid vSpace(H5Dget_space(vData), H5Sclose);
    const hssize_t vCount = H5Sget_simple_extent_npoints(vSpace);
    if(vCount < 0) return false;
    std::vector<unsigned long long> vValues((size_t)vCount);
    if(vCount && H5Dread(vData, H5T_NATIVE_ULLONG, H5S_ALL, H5S_ALL, H5P_DEFAULT, vValues.data()) < 0) return false;
    std::vector<uint8_t> vBytes(vValues.size()*8);
    for(size_t i = 0; i < vValues.size(); ++i) putLittleEndian(vBytes.data() + 8*i, vValues[i]);
    aCrc = crcBytes(aCrc, vBytes.data(), vBytes.size());
    return true;
}

Status histogramCrc(hid_t aFile, const std::string& aGroup, uint32_t& aCrc){
    Hid vGroup(H5Gopen2(aFile, aGroup.c_str(), H5P_DEFAULT), H5Gclose);
    uLong vCrc = crc32(0L, Z_NULL, 0);
    if(!vGroup.valid() || !crcHistogram(vGroup, "Histogram", vCrc) || !crcHistogram(vGroup, "Histogram1024", vCrc)){
        return Status(Code::ReadFailed, "could not read the histograms of " + aGroup);
    }
    const std::vector<std::string> vNames = attributeNames(vGroup);
    for(size_t i = 0; i < vNames.size(); ++i){
        std::string vText;
        attributeText(vGroup, vNames[i].c_str(), vText);
        vCrc = crcBytes(vCrc, vNames[i].c_str(), vNames[i].size() + 1);
        vCrc = crcBytes(vCrc, vText.c_str(), vText.size() + 1);
    }
    aCrc = (uint32_t)vCrc;
    return Status();
}

Status thumbnailCrc(hid_t aFile, uint32_t& aCrc){
    aCrc = 0;
    if(!pathExists(aFile, "/Thumbnail/Data")) return Status();
    Hid vData(H5Dopen2(aFile, "/Thumbnail/Data", H5P_DEFAULT), H5Dclose);
    Hid vType(H5Dget_type(vData), H5Tclose), vSpace(H5Dget_space(vData), H5Sclose);
    if(!vData.valid() || !vType.valid() || !vSpace.valid()) return Status(Code::ReadFailed, "could not read /Thumbnail/Data");
    Hid vMemType(H5Tget_native_type(vType, H5T_DIR_ASCEND), H5Tclose);
    const hssize_t vCount = H5Sget_simple_extent_npoints(vSpace);
    if(vCount < 0 || !vMemType.valid()) return Status(Code::ReadFailed, "could not read /Thumbnail/Data");
    std::vector<uint8_t> vBytes((size_t)vCount*H5Tget_size(vMemType));
    if(!vBytes.empty() && H5Dread(vData, vMemType, H5S_ALL, H5S_ALL, H5P_DEFAULT, vBytes.data()) < 0){
        return Status(Code::ReadFailed, "could not read /Thumbnail/Data");
    }
    aCrc = (uint32_t)crcBytes(crc32(0L, Z_NULL, 0), vBytes.data(), vBytes.size());
    return Status();
}

// Counts the groups "<aPrefix>0", "<aPrefix>1", ... under aParent.
unsigned countNumbered(hid_t aFile, const std::string& aParent, const char* aPrefix){
    unsigned vCount = 0;
    while(pathExists(aFile, aParent + "/" + aPrefix + std::to_string(vCount))) ++vCount;
    return vCount;
}

Status infoLocked(const std::string& aFile, ImsInfo& aInfo, bool aChecksums){
    Hid vFile;
    Status vStatus = openFile(aFile, vFile);
    if(!vStatus.ok()) return vStatus;
    ImsInfo vInfo;
    vInfo.levels = countNumbered(vFile, "/DataSet", "ResolutionLevel ");
    if(vInfo.levels) vInfo.timepoints = countNumbered(vFile, "/DataSet/ResolutionLevel 0", "TimePoint ");
    if(vInfo.timepoints) vInfo.channels = countNumbered(vFile, "/DataSet/ResolutionLevel 0/TimePoint 0", "Channel ");
    if(!vInfo.channels) return Status(Code::ReadFailed, aFile + " is not an .ims file (it has no " + datasetGroup(0, 0, 0) + ").");

    for(unsigned vL = 0; vL < vInfo.levels; ++vL){
        for(unsigned vT = 0; vT < vInfo.timepoints; ++vT){
            for(unsigned vC = 0; vC < vInfo.channels; ++vC){
                const std::string vGroup = datasetGroup(vL, vT, vC), vPath = vGroup + "/Data";
                if(!pathExists(vFile, vPath)) return Status(Code::ReadFailed, aFile + " has no " + vPath + ".");
                ImsDataset vSet;
                vSet.level = vL;
                vSet.timepoint = vT;
                vSet.channel = vC;
                vStatus = imageSize(vFile, vGroup, vSet.size);
                if(!vStatus.ok()) return vStatus;
                Hid vData(H5Dopen2(vFile, vPath.c_str(), H5P_DEFAULT), H5Dclose);
                Hid vPlist(H5Dget_create_plist(vData), H5Pclose);
                hsize_t vChunk[3] = {0, 0, 0};
                if(!vData.valid() || !vPlist.valid() || H5Pget_layout(vPlist) != H5D_CHUNKED || H5Pget_chunk(vPlist, 3, vChunk) != 3){
                    return Status(Code::Unsupported, vPath + " is not a chunked 3-D dataset.");
                }
                vSet.chunk[0] = vChunk[2];
                vSet.chunk[1] = vChunk[1];
                vSet.chunk[2] = vChunk[0];
                DataType vType;
                vStatus = dataTypeOf(vData, vPath, vType);
                if(!vStatus.ok()) return vStatus;
                if(!vL && !vT && !vC) vInfo.dataType = vType;
                if(aChecksums){
                    vStatus = dataCrc(vData, vPath, vSet.chunks, vSet.crc);
                    if(!vStatus.ok()) return vStatus;
                    vStatus = histogramCrc(vFile, vGroup, vSet.histogramCrc);
                    if(!vStatus.ok()) return vStatus;
                } else {
                    Hid vSpace(H5Dget_space(vData), H5Sclose);
                    hsize_t vCount = 0;
                    if(H5Dget_num_chunks(vData, vSpace, &vCount) < 0) return Status(Code::ReadFailed, "could not count the chunks of " + vPath);
                    vSet.chunks = vCount;
                }
                vInfo.datasets.push_back(vSet);
            }
        }
    }
    for(int i = 0; i < 3; ++i) vInfo.size[i] = vInfo.datasets[0].size[i];

    for(int i = 0; i < 3; ++i){
        vInfo.extentMin[i] = strtof(objectAttribute(vFile, "/DataSetInfo/Image", ("ExtMin" + std::to_string(i)).c_str()).c_str(), NULL);
        vInfo.extentMax[i] = strtof(objectAttribute(vFile, "/DataSetInfo/Image", ("ExtMax" + std::to_string(i)).c_str()).c_str(), NULL);
        vInfo.voxelSize[i] = (vInfo.extentMax[i] - vInfo.extentMin[i])/(float)vInfo.size[i];
    }
    for(unsigned vC = 0; vC < vInfo.channels; ++vC){
        const std::string vGroup = "/DataSetInfo/Channel " + std::to_string(vC);
        vInfo.channelNames.push_back(objectAttribute(vFile, vGroup, "Name"));
        vInfo.channelColors.push_back(objectAttribute(vFile, vGroup, "Color"));
    }
    for(unsigned vT = 0; vT < vInfo.timepoints; ++vT){
        vInfo.timePoints.push_back(objectAttribute(vFile, "/DataSetInfo/TimeInfo", ("TimePoint" + std::to_string(vT + 1)).c_str()));
    }
    if(aChecksums){
        vStatus = thumbnailCrc(vFile, vInfo.thumbnailCrc);
        if(!vStatus.ok()) return vStatus;
    }
    collectAttributes(vFile, "/", vInfo.attributes);
    aInfo = vInfo;
    return Status();
}

Status readLocked(const std::string& aFile, unsigned aLevel, unsigned aTimepoint, unsigned aChannel,
                  const uint64_t aStart[3], const uint64_t aEnd[3], DataType aType, void* aOut,
                  const int64_t aStride[3], int aThreads){
    Hid vFile;
    Status vStatus = openFile(aFile, vFile);
    if(!vStatus.ok()) return vStatus;
    const std::string vGroup = datasetGroup(aLevel, aTimepoint, aChannel), vPath = vGroup + "/Data";
    if(!pathExists(vFile, vPath)) return Status(Code::InvalidArgument, aFile + " has no " + vPath + ".");
    uint64_t vSize[3];
    vStatus = imageSize(vFile, vGroup, vSize);
    if(!vStatus.ok()) return vStatus;
    for(int i = 0; i < 3; ++i){
        if(aEnd[i] > vSize[i]){
            return Status(Code::InvalidArgument, "The region ends at " + std::to_string(aEnd[0]) + "x" + std::to_string(aEnd[1]) +
                          "x" + std::to_string(aEnd[2]) + ", outside " + vPath + " (" + std::to_string(vSize[0]) + "x" +
                          std::to_string(vSize[1]) + "x" + std::to_string(vSize[2]) + ").");
        }
    }
    Hid vData(H5Dopen2(vFile, vPath.c_str(), H5P_DEFAULT), H5Dclose);
    if(!vData.valid()) return Status(Code::ReadFailed, "could not open " + vPath);
    DataType vType;
    vStatus = dataTypeOf(vData, vPath, vType);
    if(!vStatus.ok()) return vStatus;
    if(vType != aType){
        return Status(Code::InvalidArgument, vPath + " holds " + dataTypeName(vType) + " data, not " + dataTypeName(aType) + ".");
    }
    Pipeline vPipeline;
    vStatus = pipelineOf(vData, vPath, vPipeline);
    if(!vStatus.ok()) return vStatus;
    Hid vPlist(H5Dget_create_plist(vData), H5Pclose);
    hsize_t vDims[3] = {0, 0, 0};
    if(H5Pget_chunk(vPlist, 3, vDims) != 3) return Status(Code::Unsupported, vPath + " is not a chunked 3-D dataset.");
    const uint64_t vChunk[3] = {vDims[2], vDims[1], vDims[0]};   // X, Y, Z
    const size_t vBytes = dataTypeBytes(vType);
    const size_t vChunkBytes = (size_t)(vChunk[0]*vChunk[1]*vChunk[2])*vBytes;
    const int vThreads = aThreads > 0 ? aThreads : omp_get_max_threads();

    // Chunks in (z, y, x) order, read in batches of about 256 MiB of stored bytes; each batch is
    // decoded and copied out in parallel.
    struct Raw { uint64_t origin[3]; unsigned mask; bool stored; std::vector<uint8_t> bytes; };
    std::vector<Raw> vBatch;
    size_t vBatchBytes = 0;
    const size_t kBatchBytes = (size_t)256 << 20;
    const uint64_t vFirst[3] = {aStart[0]/vChunk[0], aStart[1]/vChunk[1], aStart[2]/vChunk[2]};
    const uint64_t vLast[3] = {(aEnd[0] - 1)/vChunk[0], (aEnd[1] - 1)/vChunk[1], (aEnd[2] - 1)/vChunk[2]};
    std::string vError;
    auto flush = [&](){
        const int vN = (int)vBatch.size();
        const int vUse = std::max(1, std::min(vThreads, vN));
        #pragma omp parallel num_threads(vUse)
        {
            std::vector<uint8_t> vWork;
            #pragma omp for schedule(dynamic)
            for(int i = 0; i < vN; ++i){
                Raw& vRaw = vBatch[i];
                if(vRaw.stored && !decodeChunk(vPipeline, vRaw.mask, vRaw.bytes, vWork, vChunkBytes)){
                    #pragma omp critical(piwReadError)
                    { if(vError.empty()) vError = "a chunk of " + vPath + " at (x, y, z) " + std::to_string(vRaw.origin[0]) + "," +
                                                  std::to_string(vRaw.origin[1]) + "," + std::to_string(vRaw.origin[2]) + " does not decode"; }
                    continue;
                }
                scatterChunk(vBytes, vRaw.stored ? vRaw.bytes.data() : NULL, vChunk, vRaw.origin, aStart, aEnd, aOut, aStride);
                std::vector<uint8_t>().swap(vRaw.bytes);
            }
        }
        vBatch.clear();
        vBatchBytes = 0;
    };
    for(uint64_t vZ = vFirst[2]; vZ <= vLast[2] && vError.empty(); ++vZ){
        for(uint64_t vY = vFirst[1]; vY <= vLast[1] && vError.empty(); ++vY){
            for(uint64_t vX = vFirst[0]; vX <= vLast[0]; ++vX){
                hsize_t vOffset[3] = {vZ*vChunk[2], vY*vChunk[1], vX*vChunk[0]};
                unsigned vMask = 0;
                haddr_t vAddress = HADDR_UNDEF;
                hsize_t vStored = 0;
                vBatch.push_back(Raw());
                Raw& vRaw = vBatch.back();
                vRaw.origin[0] = vOffset[2];
                vRaw.origin[1] = vOffset[1];
                vRaw.origin[2] = vOffset[0];
                vRaw.mask = 0;
                vRaw.stored = H5Dget_chunk_info_by_coord(vData, vOffset, &vMask, &vAddress, &vStored) >= 0 &&
                              vAddress != HADDR_UNDEF && vStored > 0;
                if(vRaw.stored){
                    vRaw.bytes.resize((size_t)vStored);
                    uint32_t vReadMask = 0;
                    if(H5Dread_chunk(vData, H5P_DEFAULT, vOffset, &vReadMask, vRaw.bytes.data()) < 0){
                        return Status(Code::ReadFailed, "could not read a chunk of " + vPath);
                    }
                    vRaw.mask = vReadMask;
                    vBatchBytes += (size_t)vStored;
                }
                if(vBatchBytes >= kBatchBytes || vBatch.size() >= 4096) flush();
            }
        }
    }
    if(vError.empty() && !vBatch.empty()) flush();
    if(!vError.empty()) return Status(Code::ReadFailed, vError);
    return Status();
}

}  // namespace

Status imsInfo(const std::string& file, ImsInfo& info, bool checksums) noexcept{
    try{
        std::lock_guard<std::mutex> vLock(detail::libraryMutex());
        QuietErrors vQuiet;
        return infoLocked(file, info, checksums);
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status readIms(const std::string& file, unsigned level, unsigned timepoint, unsigned channel,
               const uint64_t start[3], const uint64_t end[3], DataType dataType,
               void* out, const int64_t stride[3], int numThreads) noexcept{
    try{
        if(!start || !end || !stride || !out) return Status(Code::InvalidArgument, "readIms needs a region, an output and its strides.");
        if(dataType != DataType::UInt8 && dataType != DataType::UInt16 && dataType != DataType::UInt32 && dataType != DataType::Float32){
            return Status(Code::InvalidArgument, "Unknown data type " + std::to_string((int)dataType) + ".");
        }
        for(int i = 0; i < 3; ++i){
            if(start[i] >= end[i]) return Status(Code::InvalidArgument, "The region must end after it starts along every axis.");
            if(stride[i] <= 0) return Status(Code::InvalidArgument, "Output strides must be positive.");
        }
        if(numThreads < 0) return Status(Code::InvalidArgument, "The number of threads cannot be negative.");
        std::lock_guard<std::mutex> vLock(detail::libraryMutex());
        QuietErrors vQuiet;
        return readLocked(file, level, timepoint, channel, start, end, dataType, out, stride, numThreads);
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

}  // namespace piw
