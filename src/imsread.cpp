// Reading back .ims files: piw::imsInfo, piw::imsDataset, piw::readIms and piw::verifyIms
// (src/parallelimariswriter.h).
//
// An Imaris 5.5 .ims file is HDF5: /DataSet/ResolutionLevel l/TimePoint t/Channel c holds a
// chunked 3-D Data dataset (dims z, y, x) next to its Histogram datasets, and /DataSetInfo holds
// the metadata as string attributes. ImarisWriter compresses every chunk itself (byte shuffle for
// 2- and 4-byte integers, then LZ4 in the HDF5 lz4 filter format, id 32004) and writes it with
// H5Dwrite_chunk, so the chunks are read raw here (H5Dread_chunk) and decoded here: no HDF5
// filter needs to be registered. Raw reads are serial; decoding runs in an OpenMP loop.
// Like the rest of the core: no printing (HDF5's error stack is silenced for each call), no exit,
// errors come back as a Status, and every call holds the library mutex (HDF5 here is not
// thread-safe). verifyIms opens the file through a read-only driver that prefetches the nodes of
// the chunk indexes (Prefetch, below).
#include "parallelimariswriter.h"
#include "imscore.h"     // GetOptimalImagePyramid: the levels ImarisWriter makes
#include "hdf5.h"
#include "H5FDdevelop.h"
#include <lz4.h>
#include <zlib.h>
#include <omp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif
#if defined(__linux__)
#include <sys/vfs.h>
#endif
#include <algorithm>
#include <climits>
#include <condition_variable>
#include <deque>
#include <exception>
#include <map>
#include <mutex>
#include <thread>
#include <string>
#include <vector>

namespace piw {
namespace detail {
std::mutex& libraryMutex();   // src/converter.cpp
Status verifyImsLocked(const std::string& aFile, ImsVerifyResult& aResult);
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

// Prefetch: a read-only HDF5 file driver (POSIX pread) that reads the nodes of chunk indexes ahead,
// in parallel. Counting the chunks of a dataset (H5Dget_num_chunks) walks its v1 B-tree one node
// at a time, and on a network file system every node read waits for a round trip (a 43 GB
// uint16 file has about 2900 nodes: 1.4 s, of which 0.06 s is CPU time; with this driver 0.2 s,
// 16 reads at a time). When HDF5 reads a
// B-tree node of an inner level of a chunk index, this driver queues its children, and helper
// threads read them (and the children of inner ones) while HDF5 is still working through earlier
// nodes; HDF5's later reads of those nodes are then served from memory. What is read ahead are
// plain file bytes at the addresses HDF5 will ask for, so the decoding below only decides what to
// read ahead: a node it misreads costs one wasted read, never wrong data. At most kPrefetchBytes
// are held at once (beyond that, nodes are left to HDF5's own reads). Not on Windows (there the
// default driver is used).
// On an NFS mount (Linux) the file is read with O_DIRECT. A file this node has just written is
// still in its page cache, and the NFS client drops all of those pages before the first buffered
// read after the writer closed the file (3.5 s for 43 GB, whatever is read); direct reads leave
// them to be dropped later, by the kernel or by the next reader, as without the check.
#ifndef _WIN32
const size_t kPrefetchThreads = 16;
const size_t kPrefetchBytes = (size_t)128 << 20;

// The read-ahead state of one open file.
struct Prefetcher {
    enum State { Queued, Reading, Ready };
    struct Entry { State state; std::vector<char> bytes; };
    int fd = -1;
    haddr_t eoa = 0, eof = 0;
    dev_t dev = 0;
    ino_t ino = 0;
    std::mutex mutex;
    std::condition_variable changed;
    std::map<haddr_t, Entry> entries;                    // by address; the queue holds the Queued ones
    std::deque<std::pair<haddr_t, size_t> > queue;
    size_t held = 0;                                     // bytes of Reading and Ready entries
    bool stop = false;
    std::vector<std::thread> threads;
};
struct PrefetchFile {
    H5FD_t pub;                                          // first: HDF5 casts between the two
    Prefetcher* p;
};
Prefetcher& stateOf(const H5FD_t* aFile){ return *((const PrefetchFile*)aFile)->p; }

bool preadAll(int aFd, char* aBuffer, size_t aSize, haddr_t aAddress){
    size_t vGot = 0;
    while(vGot < aSize){
        const ssize_t k = pread(aFd, aBuffer + vGot, aSize - vGot, (off_t)(aAddress + vGot));
        if(k < 0 && errno == EINTR) continue;
        if(k < 0) return false;
        if(k == 0) break;
        vGot += (size_t)k;
    }
    if(vGot < aSize) memset(aBuffer + vGot, 0, aSize - vGot);   // past the end reads as zeros, like sec2
    return true;
}

void prefetchWorker(Prefetcher* aState);

// A node of a chunk index ("TREE", node type 1) of an inner level: queues its children, which are
// nodes of the same size. The node: signature, type, level, entries used (2 bytes), two sibling
// addresses, then key, child, key, ..., key; with 8-byte addresses a key of a 3-D dataset's index
// is 40 bytes (chunk size, filter mask, 4 offsets), so a node of 2K entries is 96 K + 64 bytes (no
// other rank gives such a size). Called with the mutex held.
void queueChildren(Prefetcher& aState, const char* aNode, size_t aSize){
    if(aSize < 64 || (aSize - 64) % 96 || memcmp(aNode, "TREE", 4) || aNode[4] != 1 || aNode[5] == 0) return;
    const size_t vCapacity = 2*((aSize - 64)/96);
    const size_t vUsed = (unsigned char)aNode[6] | (size_t)(unsigned char)aNode[7] << 8;
    if(!vUsed || vUsed > vCapacity) return;
    bool vQueued = false;
    for(size_t i = 0; i < vUsed; ++i){
        const unsigned char* vChild = (const unsigned char*)aNode + 24 + 40*(i + 1) + 8*i;
        haddr_t vAddress = 0;
        for(int b = 7; b >= 0; --b) vAddress = vAddress << 8 | vChild[b];
        if(!vAddress || vAddress >= aState.eof || aSize > aState.eof - vAddress) return;   // not what it seemed
        if(aState.entries.count(vAddress)) continue;
        aState.entries[vAddress].state = Prefetcher::Queued;
        aState.queue.push_back(std::make_pair(vAddress, aSize));
        vQueued = true;
    }
    if(!vQueued) return;
    try{
        while(aState.threads.size() < kPrefetchThreads) aState.threads.push_back(std::thread(prefetchWorker, &aState));
    }
    catch(...){}   // fewer helpers (with none, HDF5 reads every node itself)
    aState.changed.notify_all();
}

void prefetchWorker(Prefetcher* aState){
    Prefetcher& s = *aState;
    std::unique_lock<std::mutex> vLock(s.mutex);
    for(;;){
        s.changed.wait(vLock, [&s]{ return s.stop || !s.queue.empty(); });
        if(s.stop) return;
        const haddr_t vAddress = s.queue.front().first;
        const size_t vSize = s.queue.front().second;
        s.queue.pop_front();
        std::map<haddr_t, Prefetcher::Entry>::iterator it = s.entries.find(vAddress);
        if(it == s.entries.end() || it->second.state != Prefetcher::Queued) continue;   // HDF5 read it itself
        if(s.held + vSize > kPrefetchBytes){ s.entries.erase(it); continue; }
        try{
            std::vector<char> vBytes(vSize);
            it->second.state = Prefetcher::Reading;
            s.held += vSize;
            vLock.unlock();
            const bool vRead = preadAll(s.fd, vBytes.data(), vSize, vAddress);
            vLock.lock();
            it = s.entries.find(vAddress);               // still there: only this thread removes a Reading entry
            if(vRead){
                it->second.bytes.swap(vBytes);
                it->second.state = Prefetcher::Ready;
                queueChildren(s, it->second.bytes.data(), vSize);
            }
            else{
                s.held -= vSize;
                s.entries.erase(it);
            }
        }
        catch(...){                                      // out of memory: HDF5 reads that node itself
            if(!vLock.owns_lock()) vLock.lock();
            it = s.entries.find(vAddress);
            if(it != s.entries.end() && it->second.state != Prefetcher::Queued){
                s.held -= it->second.state == Prefetcher::Ready ? it->second.bytes.size() : vSize;
                s.entries.erase(it);
            }
        }
        s.changed.notify_all();
    }
}

H5FD_t* prefetchOpen(const char* aName, unsigned aFlags, hid_t, haddr_t){
    if(aFlags & (H5F_ACC_RDWR | H5F_ACC_TRUNC | H5F_ACC_CREAT | H5F_ACC_EXCL)) return NULL;   // read-only
    const int vFd = open(aName, O_RDONLY);
    if(vFd < 0) return NULL;
    #if defined(__linux__)
    struct statfs vFs;
    const long kNfs = 0x6969;   // NFS_SUPER_MAGIC
    if(fstatfs(vFd, &vFs) == 0 && (long)vFs.f_type == kNfs){
        const int vFlags = fcntl(vFd, F_GETFL);
        if(vFlags != -1) fcntl(vFd, F_SETFL, vFlags | O_DIRECT);   // if refused: buffered reads
    }
    #endif
    struct stat vInfo;
    PrefetchFile* vFile = NULL;
    try{
        if(fstat(vFd, &vInfo) < 0) throw 0;
        vFile = (PrefetchFile*)calloc(1, sizeof(PrefetchFile));
        if(!vFile) throw 0;
        vFile->p = new Prefetcher();
    }
    catch(...){ free(vFile); close(vFd); return NULL; }
    Prefetcher& s = *vFile->p;
    s.fd = vFd;
    s.eof = (haddr_t)vInfo.st_size;
    s.dev = vInfo.st_dev;
    s.ino = vInfo.st_ino;
    return &vFile->pub;
}

herr_t prefetchClose(H5FD_t* aFile){
    PrefetchFile* vFile = (PrefetchFile*)aFile;
    Prefetcher* vState = vFile->p;
    {
        std::lock_guard<std::mutex> vLock(vState->mutex);
        vState->stop = true;
    }
    vState->changed.notify_all();
    for(size_t i = 0; i < vState->threads.size(); ++i) vState->threads[i].join();
    const int vClosed = close(vState->fd);
    delete vState;
    free(vFile);
    return vClosed == 0 ? 0 : -1;
}

int prefetchCmp(const H5FD_t* aA, const H5FD_t* aB){
    const Prefetcher& a = stateOf(aA);
    const Prefetcher& b = stateOf(aB);
    if(a.dev != b.dev) return a.dev < b.dev ? -1 : 1;
    if(a.ino != b.ino) return a.ino < b.ino ? -1 : 1;
    return 0;
}

herr_t prefetchQuery(const H5FD_t*, unsigned long* aFlags){
    if(aFlags) *aFlags = H5FD_FEAT_AGGREGATE_METADATA | H5FD_FEAT_DATA_SIEVE | H5FD_FEAT_DEFAULT_VFD_COMPATIBLE;
    return 0;
}

haddr_t prefetchGetEoa(const H5FD_t* aFile, H5FD_mem_t){ return stateOf(aFile).eoa; }
herr_t prefetchSetEoa(H5FD_t* aFile, H5FD_mem_t, haddr_t aAddress){ stateOf(aFile).eoa = aAddress; return 0; }
haddr_t prefetchGetEof(const H5FD_t* aFile, H5FD_mem_t){ return stateOf(aFile).eof; }

herr_t prefetchRead(H5FD_t* aFile, H5FD_mem_t aType, hid_t, haddr_t aAddress, size_t aSize, void* aBuffer){
    Prefetcher& s = stateOf(aFile);
    try{
        std::unique_lock<std::mutex> vLock(s.mutex);
        std::map<haddr_t, Prefetcher::Entry>::iterator it = s.entries.find(aAddress);
        if(it != s.entries.end() && it->second.state == Prefetcher::Reading){
            s.changed.wait(vLock, [&]{
                it = s.entries.find(aAddress);
                return it == s.entries.end() || it->second.state != Prefetcher::Reading; });
        }
        if(it != s.entries.end() && it->second.state == Prefetcher::Ready && it->second.bytes.size() == aSize){
            memcpy(aBuffer, it->second.bytes.data(), aSize);
            s.held -= aSize;
            s.entries.erase(it);
            return 0;
        }
        if(it != s.entries.end()){                       // queued (or read with another size): read here instead
            if(it->second.state == Prefetcher::Ready) s.held -= it->second.bytes.size();
            s.entries.erase(it);
        }
        vLock.unlock();
        if(!preadAll(s.fd, (char*)aBuffer, aSize, aAddress)) return -1;
        if(aType == H5FD_MEM_BTREE){
            vLock.lock();
            queueChildren(s, (const char*)aBuffer, aSize);
        }
        return 0;
    }
    catch(...){ return -1; }
}

herr_t prefetchWrite(H5FD_t*, H5FD_mem_t, hid_t, haddr_t, size_t, const void*){ return -1; }   // read-only
herr_t prefetchLock(H5FD_t*, hbool_t){ return 0; }
herr_t prefetchUnlock(H5FD_t*){ return 0; }

hid_t prefetchDriver(){
    static hid_t vId = H5I_INVALID_HID;
    if(vId >= 0 && H5Iis_valid(vId) > 0) return vId;
    static H5FD_class_t vClass;
    memset(&vClass, 0, sizeof vClass);
    vClass.version = H5FD_CLASS_VERSION;
    vClass.value = (H5FD_class_value_t)302;   // 256-511: drivers that are not registered with The HDF Group
    vClass.name = "piw_prefetch";
    vClass.maxaddr = (haddr_t)INT64_MAX;
    vClass.fc_degree = H5F_CLOSE_WEAK;
    vClass.open = prefetchOpen;
    vClass.close = prefetchClose;
    vClass.cmp = prefetchCmp;
    vClass.query = prefetchQuery;
    vClass.get_eoa = prefetchGetEoa;
    vClass.set_eoa = prefetchSetEoa;
    vClass.get_eof = prefetchGetEof;
    vClass.read = prefetchRead;
    vClass.write = prefetchWrite;
    vClass.lock = prefetchLock;
    vClass.unlock = prefetchUnlock;
    const H5FD_mem_t vMap[H5FD_MEM_NTYPES] = H5FD_FLMAP_DICHOTOMY;
    memcpy(vClass.fl_map, vMap, sizeof vMap);
    vId = H5FDregister(&vClass);
    return vId;
}
#endif

// aPrefetch: through the Prefetch driver (POSIX), else HDF5's default one. External links (a link
// file's parts) are opened with the same driver.
Status openFile(const std::string& aFile, Hid& aId, bool aPrefetch = false){
    struct stat vInfo;
    if(stat(aFile.c_str(), &vInfo) != 0) return Status(Code::NotFound, aFile + " does not exist.");
    Hid vFapl;
    #ifndef _WIN32
    if(aPrefetch){
        const hid_t vDriver = prefetchDriver();
        vFapl.reset(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
        if(vDriver < 0 || !vFapl.valid() || H5Pset_driver(vFapl, vDriver, NULL) < 0) vFapl.reset(-1, NULL);
    }
    #else
    (void)aPrefetch;
    #endif
    hid_t vId = H5Fopen(aFile.c_str(), H5F_ACC_RDONLY, vFapl.valid() ? (hid_t)vFapl : H5P_DEFAULT);
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

// A whole number written as digits only (an attribute's text).
bool wholeNumber(const std::string& aText, uint64_t& aValue){
    if(aText.empty() || aText.size() > 19 || aText.find_first_not_of("0123456789") != std::string::npos) return false;
    aValue = strtoull(aText.c_str(), NULL, 10);
    return true;
}

std::string sizeText(const uint64_t aSize[3]){
    return std::to_string(aSize[0]) + "x" + std::to_string(aSize[1]) + "x" + std::to_string(aSize[2]);
}

// verifyIms with the library mutex held. Every message names the file and the first object that
// is wrong.
Status verifyLocked(const std::string& aFile, ImsVerifyResult& aResult){
    Hid vFile;
    Status vStatus = openFile(aFile, vFile, true);
    if(!vStatus.ok()) return vStatus;
    const std::string vPrefix = aFile + ": ";
    auto bad = [&](const std::string& aWhy){ return Status(Code::ReadFailed, vPrefix + aWhy); };
    ImsVerifyResult vResult;

    // What the metadata says (written when the file is finished).
    static const char* const kAxes[3] = {"X", "Y", "Z"};
    for(int i = 0; i < 3; ++i){
        const std::string vText = objectAttribute(vFile, "/DataSetInfo/Image", kAxes[i]);
        if(!wholeNumber(vText, vResult.size[i]) || !vResult.size[i] || vResult.size[i] > UINT_MAX){
            return bad(std::string("no usable /DataSetInfo/Image ") + kAxes[i] + " (the file was not finished)");
        }
    }
    std::string vTimes = objectAttribute(vFile, "/DataSetInfo/TimeInfo", "FileTimePoints");
    if(vTimes.empty()) vTimes = objectAttribute(vFile, "/DataSetInfo/TimeInfo", "DatasetTimePoints");
    uint64_t vTimepoints = 0;
    if(!wholeNumber(vTimes, vTimepoints) || !vTimepoints || vTimepoints > UINT_MAX){
        return bad("no usable /DataSetInfo/TimeInfo FileTimePoints (the file was not finished)");
    }
    vResult.timepoints = (unsigned)vTimepoints;
    vResult.channels = countNumbered(vFile, "/DataSetInfo", "Channel ");
    if(!vResult.channels) return bad("no /DataSetInfo/Channel 0 (the file was not finished)");

    // Level 0's sample type, and the pyramid ImarisWriter makes of that size and type.
    const std::string vFirst = datasetGroup(0, 0, 0) + "/Data";
    if(!pathExists(vFile, vFirst)) return bad("no " + vFirst);
    {
        Hid vData(H5Dopen2(vFile, vFirst.c_str(), H5P_DEFAULT), H5Dclose);
        if(!vData.valid()) return bad("cannot open " + vFirst);
        vStatus = dataTypeOf(vData, vFirst, vResult.dataType);
        if(!vStatus.ok()) return Status(vStatus.code, vPrefix + vStatus.message);
    }
    const std::vector<imscore::Vec3> vLevels = imscore::GetOptimalImagePyramid(
        (1024*1024)/dataTypeBytes(vResult.dataType), {vResult.size[0], vResult.size[1], vResult.size[2]}, true);
    vResult.levels = (unsigned)vLevels.size();
    const std::string vImage = sizeText(vResult.size) + " " + dataTypeName(vResult.dataType) + " image";

    // Exactly those levels, time points and channels.
    if(pathExists(vFile, "/DataSet/ResolutionLevel " + std::to_string(vResult.levels))){
        return bad("more resolution levels than the " + std::to_string(vResult.levels) + " of a " + vImage);
    }
    for(unsigned vL = 0; vL < vResult.levels; ++vL){
        const std::string vLevel = "/DataSet/ResolutionLevel " + std::to_string(vL);
        if(!pathExists(vFile, vLevel)) return bad("no " + vLevel + " (a " + vImage + " has " + std::to_string(vResult.levels) + " levels)");
        if(pathExists(vFile, vLevel + "/TimePoint " + std::to_string(vResult.timepoints))){
            return bad(vLevel + " has more time points than the " + std::to_string(vResult.timepoints) + " of /DataSetInfo/TimeInfo");
        }
        for(unsigned vT = 0; vT < vResult.timepoints; ++vT){
            const std::string vPoint = vLevel + "/TimePoint " + std::to_string(vT);
            if(pathExists(vFile, vPoint + "/Channel " + std::to_string(vResult.channels))){
                return bad(vPoint + " has more channels than the " + std::to_string(vResult.channels) + " of /DataSetInfo");
            }
        }
    }

    // Every Data dataset: its size, sample type and every chunk.
    for(unsigned vL = 0; vL < vResult.levels; ++vL){
        for(unsigned vT = 0; vT < vResult.timepoints; ++vT){
            for(unsigned vC = 0; vC < vResult.channels; ++vC){
                const std::string vGroup = datasetGroup(vL, vT, vC), vPath = vGroup + "/Data";
                if(!pathExists(vFile, vPath)) return bad("no " + vPath);
                uint64_t vSize[3] = {0, 0, 0};
                vStatus = imageSize(vFile, vGroup, vSize);
                if(!vStatus.ok()) return Status(vStatus.code, vPrefix + vStatus.message);
                if(vSize[0] != vLevels[vL][0] || vSize[1] != vLevels[vL][1] || vSize[2] != vLevels[vL][2]){
                    const uint64_t vWant[3] = {vLevels[vL][0], vLevels[vL][1], vLevels[vL][2]};
                    return bad(vGroup + " is " + sizeText(vSize) + "; level " + std::to_string(vL) + " of a " + vImage + " is " + sizeText(vWant));
                }
                Hid vData(H5Dopen2(vFile, vPath.c_str(), H5P_DEFAULT), H5Dclose);
                if(!vData.valid()) return bad("cannot open " + vPath);
                Hid vPlist(H5Dget_create_plist(vData), H5Pclose);
                hsize_t vChunk[3] = {0, 0, 0};
                if(!vPlist.valid() || H5Pget_layout(vPlist) != H5D_CHUNKED || H5Pget_chunk(vPlist, 3, vChunk) != 3 ||
                   !vChunk[0] || !vChunk[1] || !vChunk[2]){
                    return bad(vPath + " is not a chunked 3-D dataset");
                }
                DataType vType;
                vStatus = dataTypeOf(vData, vPath, vType);
                if(!vStatus.ok()) return Status(vStatus.code, vPrefix + vStatus.message);
                if(vType != vResult.dataType){
                    return bad(vPath + " holds " + dataTypeName(vType) + " data, " + vFirst + " " + dataTypeName(vResult.dataType));
                }
                // chunk dims are (z, y, x)
                const uint64_t vWant = ((vSize[0] + vChunk[2] - 1)/vChunk[2])*((vSize[1] + vChunk[1] - 1)/vChunk[1])*
                                       ((vSize[2] + vChunk[0] - 1)/vChunk[0]);
                Hid vSpace(H5Dget_space(vData), H5Sclose);
                hsize_t vCount = 0;
                if(!vSpace.valid() || H5Dget_num_chunks(vData, vSpace, &vCount) < 0) return bad("cannot count the chunks of " + vPath);
                if(vCount != vWant){
                    return bad(vPath + " holds " + std::to_string((unsigned long long)vCount) + " chunks; its size " + sizeText(vSize) +
                               " in chunks of " + std::to_string((unsigned long long)vChunk[2]) + "x" + std::to_string((unsigned long long)vChunk[1]) +
                               "x" + std::to_string((unsigned long long)vChunk[0]) + " needs " + std::to_string((unsigned long long)vWant));
                }
                vResult.chunks += vCount;
            }
        }
    }
    aResult = vResult;
    return Status();
}

Status datasetLocked(const std::string& aFile, unsigned aLevel, unsigned aTimepoint, unsigned aChannel,
                     uint64_t aSize[3], DataType& aType){
    Hid vFile;
    Status vStatus = openFile(aFile, vFile);
    if(!vStatus.ok()) return vStatus;
    if(!pathExists(vFile, datasetGroup(0, 0, 0))){
        return Status(Code::ReadFailed, aFile + " is not an .ims file (it has no " + datasetGroup(0, 0, 0) + ").");
    }
    const std::string vGroup = datasetGroup(aLevel, aTimepoint, aChannel), vPath = vGroup + "/Data";
    if(!pathExists(vFile, vPath)) return Status(Code::InvalidArgument, aFile + " has no " + vPath + ".");
    vStatus = imageSize(vFile, vGroup, aSize);
    if(!vStatus.ok()) return vStatus;
    Hid vData(H5Dopen2(vFile, vPath.c_str(), H5P_DEFAULT), H5Dclose);
    if(!vData.valid()) return Status(Code::ReadFailed, "could not open " + vPath);
    return dataTypeOf(vData, vPath, aType);
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

namespace detail {
// verifyIms for the converter, which holds the library mutex already.
Status verifyImsLocked(const std::string& aFile, ImsVerifyResult& aResult){
    QuietErrors vQuiet;
    return verifyLocked(aFile, aResult);
}
}

Status verifyIms(const std::string& file, ImsVerifyResult* result) noexcept{
    try{
        std::lock_guard<std::mutex> vLock(detail::libraryMutex());
        ImsVerifyResult vResult;
        const Status vStatus = detail::verifyImsLocked(file, vResult);
        if(vStatus.ok() && result) *result = vResult;
        return vStatus;
    }
    catch(const std::exception& vError){ return internalError(vError.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status imsDataset(const std::string& file, unsigned level, unsigned timepoint, unsigned channel,
                  uint64_t size[3], DataType& dataType) noexcept{
    try{
        if(!size) return Status(Code::InvalidArgument, "imsDataset needs an output for the size.");
        std::lock_guard<std::mutex> vLock(detail::libraryMutex());
        QuietErrors vQuiet;
        return datasetLocked(file, level, timepoint, channel, size, dataType);
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
