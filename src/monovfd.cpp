// Placement VFD for the single-file cluster output: see monovfd.h for the file layout and the driver rules.
#include "monovfd.h"
#include "H5FDdevelop.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

unsigned mono_dtype_bytes(unsigned t){ return t==MONO_U8?1:(t==MONO_U16?2:((t==MONO_U32||t==MONO_F32)?4:0)); }

MonoVfd::MonoVfd(uint64_t r):rawEnd(r),armState(0),armAddr(0),armSize(0),inUse(false){ st.rawEnd=r; }

namespace {
enum { ARM_IDLE=0, ARM_ARMED=1, ARM_ALLOCATED=2, ARM_CONSUMED=3 };
bool armed(const MonoVfd& s){ return s.armState==ARM_ARMED||s.armState==ARM_ALLOCATED; }
std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }
void fail(MonoVfd& s,const std::string& msg){ s.st.failures++; if(s.err.empty()) s.err=msg; }
template<class T> T withErr(std::string* err,const std::string& msg,T rv){ if(err) *err=msg; return rv; }

// LZ4 filter (id 32004) as the .ims files declare it. The finisher never runs it (H5Dwrite_chunk bypasses
// the pipeline), so a no-op is registered unless a real one (ImarisWriter's, or a test's decoder) already is.
const int LZ4_FILTER=32004;
size_t lz4Noop(unsigned,size_t,const unsigned int*,size_t,size_t*,void**){ return 0; }
const H5Z_class2_t NOOP_LZ4={H5Z_CLASS_T_VERS,(H5Z_filter_t)LZ4_FILTER,1,1,
    "HDF5 lz4 filter; see http://www.hdfgroup.org/services/contributions.html",NULL,NULL,(H5Z_func_t)lz4Noop};
bool ensureLZ4(){ return H5Zfilter_avail(LZ4_FILTER)>0||H5Zregister(&NOOP_LZ4)>=0; }

hid_t fileType(unsigned dtype){
    switch(dtype){ case MONO_U8: return H5T_STD_U8LE; case MONO_U16: return H5T_STD_U16LE;
                   case MONO_U32: return H5T_STD_U32LE; case MONO_F32: return H5T_IEEE_F32LE; }
    return -1;
}
bool lessZYX(const MonoChunk& a,const MonoChunk& b){
    if(a.cz!=b.cz) return a.cz<b.cz;
    if(a.cy!=b.cy) return a.cy<b.cy;
    return a.cx<b.cx;
}

#ifndef _WIN32
// ---------------- the driver ----------------
const uint64_t FAPL_MAGIC=0x31646676706d7770ull;   // "pwmpvfd1"
struct FaplInfo { uint64_t magic; MonoVfd* s; };    // driver info carried by the FAPL (copied by HDF5)
struct PFile { H5FD_t pub; int fd; haddr_t eoa, eof; bool first; dev_t dev; ino_t ino; MonoVfd* s; };

bool pwriteAll(int fd,const void* b,size_t n,uint64_t off){
    const char* p=(const char*)b; size_t put=0;
    while(put<n){ ssize_t k=pwrite(fd,p+put,n-put,(off_t)(off+put)); if(k<0&&errno==EINTR) continue; if(k<=0) return false; put+=(size_t)k; }
    return true;
}

H5FD_t* pvOpen(const char* name,unsigned flags,hid_t fapl,haddr_t){
    const FaplInfo* fi=(const FaplInfo*)H5Pget_driver_info(fapl);
    if(!fi||fi->magic!=FAPL_MAGIC||!fi->s) return NULL;
    MonoVfd& s=*fi->s;
    // Create only. HDF5 first tries a "tentative" open without H5F_ACC_TRUNC/CREAT and quietly retries with
    // the caller's flags when it fails; an H5Fopen through this driver fails, as it must (a fresh superblock
    // would be written over the file).
    if(!(flags&H5F_ACC_RDWR)||!(flags&H5F_ACC_TRUNC)) return NULL;
    if(s.inUse){ fail(s,"the placement session is already bound to an open file"); return NULL; }
    if(s.rawEnd<MONO_RAW_BEGIN){ fail(s,"raw region end "+u64s(s.rawEnd)+" is below its start"); return NULL; }
    int fd=open(name,O_RDWR|O_CREAT,0666);     // never O_TRUNC: the raw region is already there
    if(fd<0){ fail(s,std::string("cannot open ")+name+": "+strerror(errno)); return NULL; }
    struct stat sb;
    if(fstat(fd,&sb)<0){ fail(s,std::string("cannot stat ")+name+": "+strerror(errno)); close(fd); return NULL; }
    // A new HDF5 structure over the placed chunks: zero the header region and drop the tail of an earlier
    // finish, so nothing stale is left in the file.
    std::vector<char> zeros(MONO_RAW_BEGIN,0);
    if(!pwriteAll(fd,zeros.data(),zeros.size(),0)||((uint64_t)sb.st_size>s.rawEnd&&ftruncate(fd,(off_t)s.rawEnd)<0)){
        fail(s,std::string("cannot reset the header/tail of ")+name+": "+strerror(errno)); close(fd); return NULL; }
    PFile* f=(PFile*)calloc(1,sizeof(PFile));
    if(!f){ fail(s,"out of memory"); close(fd); return NULL; }
    f->fd=fd; f->eoa=0; f->eof=0; f->first=false; f->dev=sb.st_dev; f->ino=sb.st_ino; f->s=&s;
    // eof stays 0: HDF5 treats the file as new (max(eof,eoa)==0) and writes a superblock.
    s.inUse=true; s.armState=ARM_IDLE;
    return (H5FD_t*)f;
}
herr_t pvClose(H5FD_t* _f){
    PFile* f=(PFile*)_f; MonoVfd& s=*f->s; herr_t rc=0;
    if(fsync(f->fd)<0){ fail(s,std::string("fsync failed: ")+strerror(errno)); rc=-1; }
    if(close(f->fd)<0){ fail(s,std::string("close failed: ")+strerror(errno)); rc=-1; }
    s.st.eoa=f->eoa; s.inUse=false; free(f); return rc;
}
int pvCmp(const H5FD_t* a,const H5FD_t* b){
    const PFile* x=(const PFile*)a; const PFile* y=(const PFile*)b;
    if(x->dev!=y->dev) return x->dev<y->dev?-1:1;
    if(x->ino!=y->ino) return x->ino<y->ino?-1:1;
    return 0;
}
herr_t pvQuery(const H5FD_t*,unsigned long* flags){
    // NOT H5FD_FEAT_AGGREGATE_SMALLDATA (raw allocations must reach pvAlloc one by one) and NOT DATA_SIEVE.
    if(flags) *flags=H5FD_FEAT_AGGREGATE_METADATA|H5FD_FEAT_ACCUMULATE_METADATA|H5FD_FEAT_DEFAULT_VFD_COMPATIBLE;
    return 0;
}
haddr_t pvAlloc(H5FD_t* _f,H5FD_mem_t type,hid_t,hsize_t size){
    PFile* f=(PFile*)_f; MonoVfd& s=*f->s;
    if(type==H5FD_MEM_DRAW&&armed(s)){                      // the armed chunk: it is already at armAddr
        if(s.armState==ARM_ALLOCATED){ fail(s,"second raw allocation while a chunk is armed"); return HADDR_UNDEF; }
        if(size!=s.armSize){ fail(s,"chunk armed at "+u64s(s.armAddr)+": HDF5 allocates "+u64s(size)+" bytes, armed "+u64s(s.armSize)); return HADDR_UNDEF; }
        if(!f->first||s.armAddr>f->eoa||size>f->eoa-s.armAddr){ fail(s,"chunk armed at "+u64s(s.armAddr)+" lies beyond the end of allocation"); return HADDR_UNDEF; }
        s.armState=ARM_ALLOCATED; s.st.placed++; return s.armAddr;
    }
    s.st.allocs++;
    haddr_t a=f->eoa;
    if(!f->first){                                         // superblock block at the start; then skip the raw region
        f->first=true;
        if(a+size>MONO_RAW_BEGIN){ fail(s,"first metadata block ("+u64s(size)+" bytes at "+u64s(a)+") does not fit the header region"); return HADDR_UNDEF; }
        f->eoa=std::max<haddr_t>(a+size,s.rawEnd); return a;
    }
    f->eoa=a+size; return a;                                // everything else: the tail
}
haddr_t pvGetEoa(const H5FD_t* f,H5FD_mem_t){ return ((const PFile*)f)->eoa; }
herr_t pvSetEoa(H5FD_t* _f,H5FD_mem_t,haddr_t a){
    PFile* f=(PFile*)_f;
    if(f->first&&a<f->s->rawEnd){ fail(*f->s,"HDF5 moved the end of allocation to "+u64s(a)+", inside the raw region"); return -1; }
    f->eoa=a; return 0;
}
haddr_t pvGetEof(const H5FD_t* _f,H5FD_mem_t){ const PFile* f=(const PFile*)_f; return std::max(f->eof,f->eoa); }
herr_t pvRead(H5FD_t* _f,H5FD_mem_t,hid_t,haddr_t addr,size_t size,void* buf){
    PFile* f=(PFile*)_f; MonoVfd& s=*f->s; char* p=(char*)buf; size_t got=0;
    while(got<size){
        ssize_t k=pread(f->fd,p+got,size-got,(off_t)(addr+got));
        if(k<0&&errno==EINTR) continue;
        if(k<0){ fail(s,"read of "+u64s(size)+" bytes at "+u64s(addr)+" failed: "+strerror(errno)); return -1; }
        if(k==0) break;
        got+=(size_t)k;
    }
    if(got<size) memset(p+got,0,size-got);                 // past the end reads as zeros, like sec2
    s.st.reads++; s.st.readBytes+=size; return 0;
}
herr_t pvWrite(H5FD_t* _f,H5FD_mem_t type,hid_t,haddr_t addr,size_t size,const void* buf){
    PFile* f=(PFile*)_f; MonoVfd& s=*f->s;
    if(type==H5FD_MEM_DRAW&&armed(s)){                      // the armed chunk's payload: already on disk
        if(s.armState!=ARM_ALLOCATED){ fail(s,"armed chunk written at "+u64s(addr)+" without being allocated at its address"); return -1; }
        if(addr!=s.armAddr||size!=s.armSize){ fail(s,"armed chunk written as "+u64s(size)+" bytes at "+u64s(addr)+", armed "+u64s(s.armSize)+" at "+u64s(s.armAddr)); return -1; }
        s.armState=ARM_CONSUMED; s.st.discarded++;
        if(addr+size>f->eof) f->eof=addr+size;
        return 0;
    }
    if(size==0) return 0;
    if(addr<s.rawEnd&&addr+size>MONO_RAW_BEGIN){ fail(s,"HDF5 write of "+u64s(size)+" bytes at "+u64s(addr)+" overlaps the raw region"); return -1; }
    if(!pwriteAll(f->fd,buf,size,addr)){ fail(s,"write of "+u64s(size)+" bytes at "+u64s(addr)+" failed: "+strerror(errno)); return -1; }
    s.st.writes++; s.st.writeBytes+=size;
    if(type==H5FD_MEM_DRAW){ s.st.rawWrites++; s.st.rawWriteBytes+=size; }   // small contiguous datasets
    if(addr+size>f->eof) f->eof=addr+size;
    return 0;
}
herr_t pvTruncate(H5FD_t* _f,hid_t,hbool_t){
    PFile* f=(PFile*)_f; MonoVfd& s=*f->s;
    if(f->eoa<s.rawEnd){ fail(s,"truncate to "+u64s(f->eoa)+" would cut the raw region (ends at "+u64s(s.rawEnd)+")"); return -1; }
    if(ftruncate(f->fd,(off_t)f->eoa)<0){ fail(s,std::string("ftruncate failed: ")+strerror(errno)); return -1; }
    f->eof=f->eoa; return 0;
}
herr_t pvLock(H5FD_t*,hbool_t){ return 0; }
herr_t pvUnlock(H5FD_t*){ return 0; }

hid_t driverId(){
    static hid_t id=H5I_INVALID_HID;
    if(id>=0&&H5Iis_valid(id)>0) return id;
    static H5FD_class_t c;
    memset(&c,0,sizeof c);
    c.version=H5FD_CLASS_VERSION; c.value=(H5FD_class_value_t)301;   // 256-511: values for unregistered drivers
    c.name="piw_place"; c.maxaddr=(haddr_t)INT64_MAX; c.fc_degree=H5F_CLOSE_WEAK;
    c.fapl_size=sizeof(FaplInfo);                                    // copied with memcpy
    c.open=pvOpen; c.close=pvClose; c.cmp=pvCmp; c.query=pvQuery; c.alloc=pvAlloc;
    c.get_eoa=pvGetEoa; c.set_eoa=pvSetEoa; c.get_eof=pvGetEof; c.read=pvRead; c.write=pvWrite;
    c.truncate=pvTruncate; c.lock=pvLock; c.unlock=pvUnlock;
    H5FD_mem_t fl[H5FD_MEM_NTYPES]=H5FD_FLMAP_DICHOTOMY; memcpy(c.fl_map,fl,sizeof fl);
    id=H5FDregister(&c);
    return id;
}
#endif
}  // namespace

hid_t mono_vfd_fapl(MonoVfd& s,std::string* err){
#ifdef _WIN32
    (void)s; return withErr(err,"the single-file cluster output is not supported on this platform",(hid_t)-1);
#else
    hid_t drv=driverId();
    if(drv<0) return withErr(err,"cannot register the placement driver",(hid_t)-1);
    FaplInfo fi; fi.magic=FAPL_MAGIC; fi.s=&s;
    hid_t fapl=H5Pcreate(H5P_FILE_ACCESS);
    if(fapl<0||H5Pset_libver_bounds(fapl,H5F_LIBVER_V18,H5F_LIBVER_V18)<0||H5Pset_file_locking(fapl,false,false)<0
       ||H5Pset_driver(fapl,drv,&fi)<0){
        if(fapl>=0) H5Pclose(fapl);
        return withErr(err,"cannot set up the placement file-access property list",(hid_t)-1); }
    return fapl;
#endif
}
void mono_vfd_arm(MonoVfd& s,uint64_t addr,uint64_t size){
    if(armed(s)) fail(s,"chunk armed at "+u64s(s.armAddr)+" was never written");
    s.armState=ARM_ARMED; s.armAddr=addr; s.armSize=size; s.st.armed++;
}
bool mono_vfd_consumed(const MonoVfd& s){ return s.armState==ARM_CONSUMED; }
void mono_vfd_disarm(MonoVfd& s){ s.armState=ARM_IDLE; }
const MonoVfdStats& mono_vfd_stats(const MonoVfd& s){ return s.st; }
bool mono_vfd_ok(const MonoVfd& s,std::string* err){ if(s.st.failures&&err) *err=s.err; return s.st.failures==0; }

hid_t mono_create_dataset(hid_t loc,const char* name,const MonoGeom& g,std::string* err){
    unsigned eb=mono_dtype_bytes(g.dtype);
    if(!eb) return withErr(err,std::string(name)+": unknown sample type "+u64s(g.dtype),(hid_t)-1);
    for(int a=0;a<3;a++) if(!g.chunk[a]||!g.grid[a]||g.size[a]>(uint64_t)g.chunk[a]*g.grid[a])
        return withErr(err,std::string(name)+": inconsistent geometry (size/chunk/grid)",(hid_t)-1);
    if(!ensureLZ4()) return withErr(err,"cannot register the LZ4 filter",(hid_t)-1);
    hsize_t dims[3]={(hsize_t)g.chunk[2]*g.grid[2],(hsize_t)g.chunk[1]*g.grid[1],(hsize_t)g.chunk[0]*g.grid[0]};
    hsize_t chk[3]={g.chunk[2],g.chunk[1],g.chunk[0]};
    unsigned bs=1u<<30;
    hid_t dcpl=H5Pcreate(H5P_DATASET_CREATE), lcpl=H5Pcreate(H5P_LINK_CREATE), sp=H5Screate_simple(3,dims,NULL), d=-1;
    if(dcpl>=0&&lcpl>=0&&sp>=0&&H5Pset_chunk(dcpl,3,chk)>=0&&(eb==1||H5Pset_shuffle(dcpl)>=0)
       &&H5Pset_filter(dcpl,LZ4_FILTER,H5Z_FLAG_MANDATORY,1,&bs)>=0&&H5Pset_create_intermediate_group(lcpl,1)>=0)
        d=H5Dcreate2(loc,name,fileType(g.dtype),sp,lcpl,dcpl,H5P_DEFAULT);
    if(sp>=0){ H5Sclose(sp); }
    if(lcpl>=0){ H5Pclose(lcpl); }
    if(dcpl>=0){ H5Pclose(dcpl); }
    if(d<0&&err) *err=std::string(name)+": cannot create the dataset";
    return d;
}

bool mono_index_dataset(MonoVfd& s,hid_t loc,const char* name,const MonoGeom& g,size_t n,const MonoChunkAt& at,std::string* err){
    std::string e;
    if(!mono_vfd_ok(s,&e)) return withErr(err,std::string(name)+": the placement session already failed: "+e,false);
    hid_t d=mono_create_dataset(loc,name,g,err);
    if(d<0) return false;
    MonoChunk c, prev=MonoChunk();
    for(size_t k=0;k<n&&e.empty();k++){
        c=MonoChunk(); at(k,c);
        if(c.cx>=g.grid[0]||c.cy>=g.grid[1]||c.cz>=g.grid[2]){ e="chunk ("+u64s(c.cx)+","+u64s(c.cy)+","+u64s(c.cz)+") is outside the chunk grid"; break; }
        if(k>0&&!lessZYX(prev,c)){ e="chunks are not sorted by (z,y,x) or a chunk is listed twice"; break; }
        if(c.size==0||c.offset<MONO_RAW_BEGIN||c.offset>s.rawEnd||c.size>s.rawEnd-c.offset){
            e="chunk ("+u64s(c.cx)+","+u64s(c.cy)+","+u64s(c.cz)+"): "+u64s(c.size)+" bytes at "+u64s(c.offset)+" are not inside the raw region"; break; }
        if(s.zeros.size()<c.size) s.zeros.assign(c.size,0);   // H5Dwrite_chunk wants a buffer; the driver drops it
        hsize_t off[3]={(hsize_t)c.cz*g.chunk[2],(hsize_t)c.cy*g.chunk[1],(hsize_t)c.cx*g.chunk[0]};
        mono_vfd_arm(s,c.offset,c.size);
        herr_t rc=H5Dwrite_chunk(d,H5P_DEFAULT,c.mask,off,c.size,s.zeros.data());
        bool used=mono_vfd_consumed(s);
        mono_vfd_disarm(s);
        if(rc<0||!used){
            if(rc>=0) fail(s,"a chunk write of "+std::string(name)+" did not reach the placement driver");
            std::string why; mono_vfd_ok(s,&why);
            e="indexing chunk ("+u64s(c.cx)+","+u64s(c.cy)+","+u64s(c.cz)+") failed"+(why.empty()?std::string():": "+why); break; }
        prev=c;
    }
    if(H5Dclose(d)<0&&e.empty()) e="cannot close the dataset";
    if(!e.empty()) return withErr(err,std::string(name)+": "+e,false);
    return true;
}
bool mono_index_dataset(MonoVfd& s,hid_t loc,const char* name,const MonoGeom& g,std::vector<MonoChunk>& chunks,std::string* err){
    std::sort(chunks.begin(),chunks.end(),lessZYX);
    const std::vector<MonoChunk>& v=chunks;
    return mono_index_dataset(s,loc,name,g,v.size(),[&v](size_t k,MonoChunk& c){ c=v[k]; },err);
}

namespace {
struct VState { const MonoGeom* g; size_t n; const MonoChunkAt* at; size_t next, found; uint64_t bad; std::string first; };
int verifyCb(const hsize_t* off,unsigned mask,haddr_t addr,hsize_t size,void* op){
    VState& v=*(VState*)op;
    MonoChunk key; memset(&key,0,sizeof key);
    key.cz=(uint32_t)(off[0]/v.g->chunk[2]); key.cy=(uint32_t)(off[1]/v.g->chunk[1]); key.cx=(uint32_t)(off[2]/v.g->chunk[0]);
    MonoChunk e=MonoChunk(); bool hit=false;
    if(v.next<v.n){ (*v.at)(v.next,e); hit=!lessZYX(e,key)&&!lessZYX(key,e); if(hit) v.next++; }
    if(!hit){                                       // not in iteration order: binary search
        size_t lo=0,hi=v.n;
        while(lo<hi){ size_t mid=lo+(hi-lo)/2; (*v.at)(mid,e); if(lessZYX(e,key)) lo=mid+1; else hi=mid; }
        if(lo<v.n){ (*v.at)(lo,e); hit=!lessZYX(e,key)&&!lessZYX(key,e); }
    }
    if(hit) v.found++;
    if(!hit||e.offset!=addr||e.size!=size||e.mask!=mask){
        v.bad++;
        if(v.first.empty()) v.first="chunk ("+u64s(key.cx)+","+u64s(key.cy)+","+u64s(key.cz)+") at "+u64s(addr)+", "+u64s(size)
            +" bytes"+(hit?", expected "+u64s(e.size)+" bytes at "+u64s(e.offset):std::string(", not expected"));
    }
    return 0;
}
}  // namespace

bool mono_verify_dataset(hid_t dset,const MonoGeom& g,size_t n,const MonoChunkAt& at,uint64_t* mismatches,std::string* err){
    if(mismatches) *mismatches=0;
    hsize_t dims[3]={0,0,0}, chk[3]={0,0,0};
    hid_t sp=H5Dget_space(dset), pl=H5Dget_create_plist(dset), ty=H5Dget_type(dset);
    bool shapeOk=sp>=0&&pl>=0&&ty>=0&&H5Sget_simple_extent_ndims(sp)==3&&H5Sget_simple_extent_dims(sp,dims,NULL)==3
        &&H5Pget_layout(pl)==H5D_CHUNKED&&H5Pget_chunk(pl,3,chk)==3&&H5Tget_size(ty)==mono_dtype_bytes(g.dtype);
    for(int a=0;a<3&&shapeOk;a++) shapeOk=dims[2-a]==(hsize_t)g.chunk[a]*g.grid[a]&&chk[2-a]==g.chunk[a];
    if(ty>=0){ H5Tclose(ty); }
    if(pl>=0){ H5Pclose(pl); }
    if(sp>=0){ H5Sclose(sp); }
    if(!shapeOk) return withErr(err,"dataset shape, chunking or type differs from the sidecar geometry",false);
    VState v; v.g=&g; v.n=n; v.at=&at; v.next=0; v.found=0; v.bad=0;
    if(H5Dchunk_iter(dset,H5P_DEFAULT,verifyCb,&v)<0) return withErr(err,"H5Dchunk_iter failed",false);
    uint64_t bad=v.bad+(uint64_t)(n-v.found);       // wrong or unexpected + missing
    if(mismatches) *mismatches=bad;
    if(bad&&err) *err=u64s(bad)+" chunk mismatches"+(v.first.empty()?std::string(" (missing chunks)"):", first: "+v.first);
    return bad==0;
}
bool mono_verify_dataset(hid_t dset,const MonoGeom& g,std::vector<MonoChunk>& chunks,uint64_t* mismatches,std::string* err){
    std::sort(chunks.begin(),chunks.end(),lessZYX);
    const std::vector<MonoChunk>& v=chunks;
    return mono_verify_dataset(dset,g,v.size(),[&v](size_t k,MonoChunk& c){ c=v[k]; },mismatches,err);
}
