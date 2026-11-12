// Single-file cluster output from per-(timepoint, channel) parts: monoinit, monoplace, monofinish and
// monostatus (see mono.h). HDF5 is called only under the library mutex; reading parts, writing chunks and the
// sidecars run outside it.
#include "mono.h"
#include "imscore.h"
#include "monoclaim.h"
#include "monoidx.h"
#include "monovfd.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace piw { namespace detail { std::mutex& libraryMutex(); } }   // src/converter.cpp

namespace {
using piw::Code;
using piw::Status;

const char* const NOSUP="the single-file cluster output is not supported on this platform";
const char* const UINT16_ONLY="the single-file cluster output takes uint16 data only for now";
Status internalError(const char* what){ return Status(Code::Internal,std::string("unexpected error: ")+what); }

#ifndef _WIN32
std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }
std::string ownerOf(int t,int c){ char b[48]; snprintf(b,sizeof b,"P_C%d_T%d",c,t); return b; }
std::string trimSlash(std::string w){ while(w.size()>1&&w[w.size()-1]=='/') w.erase(w.size()-1); return w; }
std::string monoDir(const std::string& wd){ return wd+"/mono"; }
std::string partialPath(const std::string& wd){ return wd+"/output.ims.partial"; }
std::string idxPath(const std::string& wd,int t,int c){ return monoDir(wd)+"/"+ownerOf(t,c)+".idx"; }
std::string histPath(const std::string& wd,int r,int t,int c){ char b[64]; snprintf(b,sizeof b,"/h_L%d_C%d_T%d.hist",r,c,t); return monoDir(wd)+b; }
std::string finishedPath(const std::string& wd){ return monoDir(wd)+"/finished"; }
double seconds(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
int threadsOf(int n){ return n>0?n:std::max(1,piw::defaultThreads()); }

// ---------------- manifest (timepoints, channels, voxel size, optional output, metadata) ----------------
struct MonoManifest { int nT=0, nC=0; float vox[3]={0,0,0}; std::string output; imsmeta::Metadata meta; };
// The metadata of nC channels x nT timepoints for these options (imsmeta::resolveMetadata). Without input files
// it resolves placeholders: names, colors and wavelengths as given or the defaults, time stamps 1 s apart (or
// timeInterval) from startTime, else from the current time.
std::string resolveMonoMetadata(piw::ConvertOptions o,int nT,int nC,imsmeta::Metadata& md){
    if(o.files.empty()) o.files.assign((size_t)nC,std::vector<std::string>((size_t)nT));
    bool fits=o.files.size()==(size_t)nC; for(size_t c=0;fits&&c<o.files.size();c++) fits=o.files[c].size()==(size_t)nT;
    if(!fits) return "the input files are not "+std::to_string(nC)+" channels x "+std::to_string(nT)+" timepoints";
    return imsmeta::resolveMetadata(o,md);
}
Status readMonoManifest(const std::string& wd,MonoManifest& m){
    std::string p=wd+"/manifest.json"; std::vector<uint8_t> b;
    if(!imscore::readFileBytes(p,b)) return Status(Code::NotFound,"cannot read "+p+" (run monoinit or setup first)");
    bool hasMeta=false;
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        m.nT=j.value("timepoints",0); m.nC=j.value("channels",0);
        if(!j.contains("voxel")||j["voxel"].size()!=3) return Status(Code::InvalidArgument,p+" has no voxel size");
        for(int a=0;a<3;a++) m.vox[a]=j["voxel"][a].get<float>();
        if(j.contains("output")) m.output=j["output"].get<std::string>();
        if((hasMeta=j.contains("metadata"))){ imscore::Status ms=imscore::readMetadataJson(j["metadata"],m.nC,m.nT,m.meta);
            if(!ms.ok()) return Status(Code::InvalidArgument,p+": "+ms.message); }
    }catch(const std::exception& e){ return Status(Code::InvalidArgument,"invalid "+p+": "+e.what()); }
    if(m.nT<1||m.nC<1) return Status(Code::InvalidArgument,p+" needs timepoints >= 1 and channels >= 1");
    for(int a=0;a<3;a++) if(!(m.vox[a]>0)||!std::isfinite(m.vox[a])) return Status(Code::InvalidArgument,p+" has a voxel size that is not > 0");
    if(!hasMeta){ std::string e=resolveMonoMetadata(piw::ConvertOptions(),m.nT,m.nC,m.meta); if(!e.empty()) return Status(Code::InvalidArgument,p+": "+e); }
    return Status();
}
// Whether two resolved metadata are the same; clockStart: b's start time came from the clock (only the spacing
// of the time stamps counts).
bool sameMetadata(const imsmeta::Metadata& a,const imsmeta::Metadata& b,bool clockStart){
    if(a.names!=b.names||a.emission!=b.emission||a.excitation!=b.excitation||a.colors.size()!=b.colors.size()||a.times.size()!=b.times.size()) return false;
    for(size_t c=0;c<a.colors.size();c++)
        if(a.colors[c].red!=b.colors[c].red||a.colors[c].green!=b.colors[c].green||a.colors[c].blue!=b.colors[c].blue) return false;
    for(size_t t=0;t<a.times.size();t++)
        if(clockStart?a.times[t]-a.times[0]!=b.times[t]-b.times[0]:a.times[t]!=b.times[t]) return false;
    return true;
}

// ---------------- small POSIX helpers ----------------
int preadAll(int fd,void* buf,size_t n,uint64_t off){   // 0, or an errno (EIO for a short file)
    char* p=(char*)buf; size_t got=0;
    while(got<n){ ssize_t k=pread(fd,p+got,n-got,(off_t)(off+got)); if(k<0&&errno==EINTR) continue; if(k<0) return errno; if(k==0) return EIO; got+=(size_t)k; }
    return 0;
}
bool exists(const std::string& p){ struct stat st; return stat(p.c_str(),&st)==0; }
struct FdGuard { int fd; explicit FdGuard(int f):fd(f){} ~FdGuard(){ if(fd>=0) close(fd); } FdGuard(const FdGuard&)=delete; FdGuard& operator=(const FdGuard&)=delete; };
// Runs fn(worker) on n threads (the caller's thread is worker 0); exceptions become a message.
template<class Fn> std::string runThreads(int n,Fn fn){
    std::mutex mu; std::string err;
    auto body=[&](int w){ try{ fn(w); }catch(const std::exception& e){ std::lock_guard<std::mutex> g(mu); if(err.empty()) err=e.what(); } };
    std::vector<std::thread> th;
    for(int w=1;w<n;w++) th.push_back(std::thread(body,w));
    body(0);
    for(size_t i=0;i<th.size();i++) th[i].join();
    return err;
}
// First error of a parallel loop, kept by the first thread that reports one.
struct FirstError {
    std::mutex mu; std::string msg; std::atomic<bool> set;
    FirstError():set(false){}
    void put(const std::string& m){ std::lock_guard<std::mutex> g(mu); if(msg.empty()){ msg=m; set=true; } }
};

// ---------------- geometry: mono/layout.json ----------------
std::string geomText(const MonoIdxLevel& l){
    char b[256]; snprintf(b,sizeof b,"size %llux%llux%llu, chunks %ux%ux%u",(unsigned long long)l.size[0],(unsigned long long)l.size[1],
                          (unsigned long long)l.size[2],l.chunk[0],l.chunk[1],l.chunk[2]); return b;
}
bool sameGeom(const MonoIdxLevel& a,const MonoIdxLevel& b){
    if(a.level!=b.level||a.dtype!=b.dtype) return false;
    for(int i=0;i<3;i++) if(a.size[i]!=b.size[i]||a.chunk[i]!=b.chunk[i]||a.grid[i]!=b.grid[i]) return false;
    return true;
}
std::string layoutText(const std::vector<MonoIdxLevel>& lv){
    nlohmann::ordered_json j; j["dtype"]="uint16"; nlohmann::ordered_json a=nlohmann::ordered_json::array();
    for(size_t r=0;r<lv.size();r++){ const MonoIdxLevel& l=lv[r]; nlohmann::ordered_json e;
        e["size"]={(unsigned long long)l.size[0],(unsigned long long)l.size[1],(unsigned long long)l.size[2]};
        e["chunk"]={l.chunk[0],l.chunk[1],l.chunk[2]}; e["grid"]={l.grid[0],l.grid[1],l.grid[2]}; a.push_back(e); }
    j["levels"]=a; return j.dump(2)+"\n";
}
bool parseLayout(const std::vector<uint8_t>& b,std::vector<MonoIdxLevel>& lv,std::string* err){
    lv.clear();
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        if(j.value("dtype",std::string())!="uint16"){ *err="unknown dtype"; return false; }
        for(auto& e:j["levels"]){ MonoIdxLevel l; l.level=(uint32_t)lv.size(); l.dtype=MONO_U16;
            for(int a=0;a<3;a++){ l.size[a]=e["size"][a].get<uint64_t>(); l.chunk[a]=e["chunk"][a].get<uint32_t>(); l.grid[a]=e["grid"][a].get<uint32_t>(); }
            l.nChunks=(uint64_t)l.grid[0]*l.grid[1]*l.grid[2];
            lv.push_back(l); }
    }catch(const std::exception& e){ *err=e.what(); return false; }
    if(lv.empty()){ *err="no levels"; return false; }
    return true;
}
Status readLayout(const std::string& wd,std::vector<MonoIdxLevel>& lv){
    std::string p=monoDir(wd)+"/layout.json", e; std::vector<uint8_t> b;
    if(!exists(p)) return Status(Code::NotFound,"no "+p+": no part was placed yet (run monoplace first)");
    if(!mono_read_file(p,b,&e)) return Status(Code::ReadFailed,e);
    if(!parseLayout(b,lv,&e)) return Status(Code::ReadFailed,"invalid "+p+": "+e);
    return Status();
}
Status compareLayout(const std::vector<MonoIdxLevel>& have,const std::vector<MonoIdxLevel>& lv,const std::string& p,const std::string& what){
    const char* hint=" (every part must come from the same converter settings, crop and data type)";
    if(have.size()!=lv.size()) return Status(Code::InvalidArgument,what+" has "+u64s(lv.size())+" resolution levels, "+p+" has "+u64s(have.size())+hint);
    for(size_t r=0;r<lv.size();r++) if(!sameGeom(have[r],lv[r]))
        return Status(Code::InvalidArgument,what+" level "+u64s(r)+": "+geomText(lv[r])+"; "+p+": "+geomText(have[r])+hint);
    return Status();
}
// The first placer publishes its geometry (temporary file + link(): created exactly once, whole, also over
// NFS); every later one compares.
Status checkLayout(const std::string& wd,const std::vector<MonoIdxLevel>& lv,const std::string& part){
    std::string p=monoDir(wd)+"/layout.json", e;
    for(int attempt=0;attempt<3;attempt++){
        if(exists(p)){
            std::vector<MonoIdxLevel> have;
            Status s=readLayout(wd,have); if(!s.ok()) return s;
            return compareLayout(have,lv,p,"part "+part);
        }
        std::string text=layoutText(lv), tmp=monoDir(wd)+"/tmp/layout."+mono_process_tag();
        int fd=open(tmp.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0666);
        if(fd<0) return Status(Code::WriteFailed,"cannot create "+tmp+": "+strerror(errno));
        bool ok=write(fd,text.data(),text.size())==(ssize_t)text.size()&&fsync(fd)==0; int we=errno;
        if(close(fd)!=0&&ok){ ok=false; we=errno; }
        if(!ok){ unlink(tmp.c_str()); return Status(Code::WriteFailed,"cannot write "+tmp+": "+strerror(we)); }
        int lr=link(tmp.c_str(),p.c_str()), le=errno; struct stat st;
        bool mine=lr==0||(stat(tmp.c_str(),&st)==0&&st.st_nlink>=2);
        unlink(tmp.c_str());
        if(mine) return Status();
        if(le!=EEXIST) return Status(Code::WriteFailed,"cannot create "+p+": "+strerror(le));
    }
    return Status(Code::Internal,"cannot create or read "+p);
}

// ---------------- part scan (HDF5: call with the library mutex held) ----------------
struct PartChunk { uint64_t addr; uint32_t size, cx, cy, cz; };
struct PartLevel { MonoIdxLevel g; std::vector<PartChunk> chunks; };
bool lessZYX(const PartChunk& a,const PartChunk& b){
    if(a.cz!=b.cz) return a.cz<b.cz;
    if(a.cy!=b.cy) return a.cy<b.cy;
    return a.cx<b.cx;
}
struct IterState { const MonoIdxLevel* g; std::vector<PartChunk>* out; std::string err; };
int partChunkCb(const hsize_t* off,unsigned mask,haddr_t addr,hsize_t size,void* op){
    IterState& s=*(IterState*)op; const MonoIdxLevel& g=*s.g;
    if(off[0]%g.chunk[2]||off[1]%g.chunk[1]||off[2]%g.chunk[0]){ s.err="a chunk at a non-chunk offset"; return -1; }
    PartChunk c; c.cz=(uint32_t)(off[0]/g.chunk[2]); c.cy=(uint32_t)(off[1]/g.chunk[1]); c.cx=(uint32_t)(off[2]/g.chunk[0]);
    if(c.cx>=g.grid[0]||c.cy>=g.grid[1]||c.cz>=g.grid[2]){ s.err="a chunk outside the image"; return -1; }
    if(mask!=0){ s.err="a chunk stored with filter mask "+u64s(mask)+" (a filter was skipped)"; return -1; }
    if(size==0||size>0xffffffffull||addr==HADDR_UNDEF){ s.err="a chunk with no data or of "+u64s(size)+" bytes"; return -1; }
    c.addr=(uint64_t)addr; c.size=(uint32_t)size; s.out->push_back(c);
    return 0;
}
bool parseSize(const std::string& s,uint64_t& v){
    if(s.empty()) return false;
    char* end=NULL; errno=0; unsigned long long x=strtoull(s.c_str(),&end,10);
    if(*end||errno||x==0) return false;
    v=x; return true;
}
// One level's Data: uint16, chunked, padded to whole chunks, shuffle + LZ4 like the converter writes, every
// chunk of the grid stored (a silently failed conversion is caught here).
Status scanData(hid_t d,const std::string& where,PartLevel& L){
    hid_t ty=H5Dget_type(d), sp=H5Dget_space(d), pl=H5Dget_create_plist(d);
    Status s;
    bool u16=ty>=0&&H5Tget_class(ty)==H5T_INTEGER&&H5Tget_size(ty)==2&&H5Tget_sign(ty)==H5T_SGN_NONE&&H5Tget_order(ty)==H5T_ORDER_LE;
    hsize_t dims[3]={0,0,0}, chk[3]={0,0,0};
    if(!u16) s=Status(Code::Unsupported,std::string(UINT16_ONLY)+"; "+where+" holds another sample type");
    else if(sp<0||H5Sget_simple_extent_ndims(sp)!=3||H5Sget_simple_extent_dims(sp,dims,NULL)!=3) s=Status(Code::ReadFailed,where+" is not 3-D");
    else if(pl<0||H5Pget_layout(pl)!=H5D_CHUNKED||H5Pget_chunk(pl,3,chk)!=3) s=Status(Code::ReadFailed,where+" is not chunked");
    if(s.ok()){
        // the pipeline mono_create_dataset gives the finished file: shuffle, then LZ4 (32004) with block size 1 GiB
        bool pipe=H5Pget_nfilters(pl)==2;
        for(unsigned i=0;pipe&&i<2;i++){
            unsigned flags=0,cd[8]={0}; size_t ncd=8; char name[64]; unsigned cfg=0;
            H5Z_filter_t id=H5Pget_filter2(pl,i,&flags,&ncd,cd,sizeof name,name,&cfg);
            pipe=i==0?id==H5Z_FILTER_SHUFFLE:(id==(H5Z_filter_t)32004&&ncd>=1&&cd[0]==(1u<<30));
        }
        if(!pipe) s=Status(Code::Unsupported,where+": filters other than shuffle + LZ4 (not a converter .ims?)");
    }
    for(int a=0;s.ok()&&a<3;a++){
        hsize_t c=chk[2-a];
        if(c==0||c>0xffffffffull||L.g.size[a]>dims[2-a]) s=Status(Code::ReadFailed,where+": ImageSize is larger than the dataset");
        else { L.g.chunk[a]=(uint32_t)c; uint64_t grid=(L.g.size[a]+c-1)/c;
            if(grid>0xffffffffull||grid*c!=dims[2-a]) s=Status(Code::ReadFailed,where+": the dataset is not the image padded to whole chunks");
            else L.g.grid[a]=(uint32_t)grid; }
    }
    if(s.ok()){
        IterState it; it.g=&L.g; it.out=&L.chunks;
        if(H5Dchunk_iter(d,H5P_DEFAULT,partChunkCb,&it)<0) s=Status(Code::ReadFailed,where+": "+(it.err.empty()?std::string("cannot list its chunks"):it.err));
    }
    uint64_t want=(uint64_t)L.g.grid[0]*L.g.grid[1]*L.g.grid[2];
    if(s.ok()&&L.chunks.size()!=want)
        s=Status(Code::ReadFailed,where+" has "+u64s(L.chunks.size())+" of its "+u64s(want)+" chunks (an unfinished or failed conversion?)");
    if(s.ok()){ std::sort(L.chunks.begin(),L.chunks.end(),lessZYX); L.g.nChunks=want; }
    if(ty>=0){ H5Tclose(ty); }
    if(sp>=0){ H5Sclose(sp); }
    if(pl>=0){ H5Pclose(pl); }
    return s;
}
Status scanOpenPart(hid_t f,const std::string& path,uint64_t fileBytes,std::vector<PartLevel>& lv){
    const std::string p0="/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0";
    if(!imscore::h5PathExists(f,p0+"/Data")) return Status(Code::ReadFailed,"part "+path+" has no "+p0+"/Data (not a converter .ims?)");
    if(imscore::h5PathExists(f,"/DataSet/ResolutionLevel 0/TimePoint 1")||imscore::h5PathExists(f,"/DataSet/ResolutionLevel 0/TimePoint 0/Channel 1"))
        return Status(Code::InvalidArgument,"part "+path+" holds more than one timepoint or channel (convert each (timepoint, channel) on its own)");
    for(int r=0;;r++){
        const std::string lp="/DataSet/ResolutionLevel "+std::to_string(r), gp=lp+"/TimePoint 0/Channel 0";
        if(!imscore::h5PathExists(f,lp)) break;
        if(!imscore::h5PathExists(f,gp+"/Data")) return Status(Code::ReadFailed,"part "+path+" has no "+gp+"/Data");
        PartLevel L; L.g.level=(uint32_t)r; L.g.dtype=MONO_U16;
        hid_t g=H5Gopen2(f,gp.c_str(),H5P_DEFAULT);
        if(g<0) return Status(Code::ReadFailed,"part "+path+": cannot open "+gp);
        const char* k[3]={"ImageSizeX","ImageSizeY","ImageSizeZ"}; bool sized=true;
        for(int a=0;a<3;a++) sized=sized&&parseSize(imscore::readStrAttr(g,k[a]),L.g.size[a]);
        hid_t d=sized?H5Dopen2(g,"Data",H5P_DEFAULT):-1;
        Status s=!sized?Status(Code::ReadFailed,"part "+path+": "+gp+" has no valid ImageSizeX/Y/Z")
                :d<0?Status(Code::ReadFailed,"part "+path+": cannot open "+gp+"/Data"):scanData(d,"part "+path+" "+gp+"/Data",L);
        if(d>=0){ H5Dclose(d); }
        H5Gclose(g);
        if(!s.ok()) return s;
        for(size_t i=0;i<L.chunks.size();i++) if(L.chunks[i].addr>fileBytes||L.chunks[i].size>fileBytes-L.chunks[i].addr)
            return Status(Code::ReadFailed,"part "+path+": level "+u64s(r)+" has a chunk beyond the end of the file (truncated?)");
        lv.push_back(L);
    }
    return Status();
}
Status scanPart(const std::string& path,std::vector<PartLevel>& lv){
    lv.clear();
    struct stat st;
    if(stat(path.c_str(),&st)!=0) return Status(Code::NotFound,"cannot open part "+path+": "+strerror(errno));
    imscore::H5Quiet quiet;   // every failure below has its own message
    hid_t fapl=H5Pcreate(H5P_FILE_ACCESS);
    if(fapl>=0) H5Pset_file_locking(fapl,false,true);
    hid_t f=H5Fopen(path.c_str(),H5F_ACC_RDONLY,fapl>=0?fapl:H5P_DEFAULT);
    if(fapl>=0) H5Pclose(fapl);
    if(f<0) return Status(Code::ReadFailed,"cannot open part "+path+" (not an HDF5 file, or truncated)");
    Status s=scanOpenPart(f,path,(uint64_t)st.st_size,lv);
    if(H5Fclose(f)<0&&s.ok()) s=Status(Code::ReadFailed,"cannot close part "+path);
    return s;
}

// ---------------- chunk decoding + exact histograms ----------------
struct LevelInfo { uint32_t chunk[3]; uint64_t size[3]; size_t raw; };
LevelInfo levelInfo(const MonoIdxLevel& g){
    LevelInfo li; for(int a=0;a<3;a++){ li.chunk[a]=g.chunk[a]; li.size[a]=g.size[a]; }
    li.raw=(size_t)g.chunk[0]*g.chunk[1]*g.chunk[2]*2; return li;
}
// Decodes one chunk (LZ4 + size checks) and counts its in-image voxels into H (65536 bins). sh: scratch of
// li.raw bytes. The shuffled bytes are read directly: byte 0 of every voxel first, then byte 1.
imscore::Status countChunk(const uint8_t* c,size_t cs,uint32_t cx,uint32_t cy,uint32_t cz,const LevelInfo& li,uint8_t* sh,uint64_t* H){
    imscore::Status s=imscore::decodeChunkShuffled(c,cs,sh,li.raw); if(!s.ok()) return s;
    const uint32_t bx=li.chunk[0],by=li.chunk[1],bz=li.chunk[2];
    const uint64_t xv=std::min<uint64_t>(bx,li.size[0]-(uint64_t)cx*bx), yv=std::min<uint64_t>(by,li.size[1]-(uint64_t)cy*by), zv=std::min<uint64_t>(bz,li.size[2]-(uint64_t)cz*bz);
    const uint8_t* lo=sh; const uint8_t* hi=sh+li.raw/2;
    for(uint64_t z=0;z<zv;z++) for(uint64_t y=0;y<yv;y++){
        const size_t i0=(size_t)bx*(y+(size_t)by*z);
        for(uint64_t x=0;x<xv;x++) H[lo[i0+x]|((unsigned)hi[i0+x]<<8)]++;
    }
    return s;
}
uint64_t voxelsOf(const MonoIdxLevel& g){ return g.size[0]*g.size[1]*g.size[2]; }

// The chunk dims of a single-node conversion of all nT timepoints. ImarisWriter's block layout takes the
// timepoint count into account (a block may span timepoints when that pads less), so small levels of an
// nT-timepoint file can have smaller chunks than the 1-timepoint parts. to[r] is level r's geometry in such a
// file; rechunk[r] is set where it differs from the parts'. Nothing changes if the parts' chunks are not the
// converter's 1-timepoint layout.
bool singleNodeLayout(const std::vector<MonoIdxLevel>& geo,int nT,std::vector<MonoIdxLevel>& to,std::vector<char>& rechunk){
    to=geo; rechunk.assign(geo.size(),0);
    std::vector<imscore::Vec3> sz;
    for(size_t r=0;r<geo.size();r++){ imscore::Vec3 v={{geo[r].size[0],geo[r].size[1],geo[r].size[2]}}; sz.push_back(v); }
    const imscore::bpSize budget=(1024*1024)/2;   // bpMultiresolutionImsImage::GetOptimalBlockSizes, uint16
    std::vector<imscore::Vec3> one=imscore::GetOptimalBlockSizes(budget,sz,1), all=imscore::GetOptimalBlockSizes(budget,sz,(imscore::bpSize)nT);
    for(size_t r=0;r<geo.size();r++) for(int a=0;a<3;a++) if(one[r][a]!=geo[r].chunk[a]) return false;
    for(size_t r=0;r<geo.size();r++){
        // re-chunking holds the level of one part in memory: such levels are small (a block spans timepoints only
        // where the bigger blocks would pad more than 2x), but never take more than 4 GiB for it
        if(all[r]==one[r]||voxelsOf(geo[r])>(1ull<<31)) continue;
        rechunk[r]=1; to[r].nChunks=1;
        for(int a=0;a<3;a++){ to[r].chunk[a]=(uint32_t)all[r][a]; to[r].grid[a]=(uint32_t)((geo[r].size[a]+all[r][a]-1)/all[r][a]); to[r].nChunks*=to[r].grid[a]; }
    }
    return true;
}
// Re-chunks level `from` of one part to `to` (call with the library mutex held): decodes the placed chunks
// (fd: the file being built), cuts the level into to's chunks and encodes them with the project codec (the
// bytes ImarisWriter writes), creates <g>/Data with to's geometry and writes them with H5Dwrite_chunk; HDF5
// puts them after the raw region (the placement driver passes un-armed writes through). out: the chunks
// written, with their file addresses.
Status rechunkLevel(int fd,hid_t g,const MonoIdxLevel& from,const MonoIdxLevel& to,const MonoChunk* chunks,size_t n,int T,
                    std::vector<MonoChunk>& out){
    out.clear();
    const uint64_t X=from.size[0],Y=from.size[1],Z=from.size[2];
    std::vector<uint16_t> vol((size_t)(X*Y*Z),0);
    const size_t rawFrom=(size_t)from.chunk[0]*from.chunk[1]*from.chunk[2]*2;
    std::atomic<size_t> next(0); FirstError fe;
    std::string x=runThreads(T,[&](int){
        std::vector<uint8_t> cb,vox;
        for(;;){ size_t i=next.fetch_add(1); if(i>=n||fe.set) break; const MonoChunk& ch=chunks[i];
            cb.resize(ch.size); int re=preadAll(fd,cb.data(),ch.size,ch.offset);
            imscore::Status ds=re?imscore::Status::fail(strerror(re)):imscore::decodeChunk(cb.data(),ch.size,vox,rawFrom);
            if(!ds.ok()){ fe.put("level "+u64s(from.level)+" chunk ("+u64s(ch.cx)+","+u64s(ch.cy)+","+u64s(ch.cz)+") at "+u64s(ch.offset)+": "+ds.message); break; }
            const uint64_t bx=from.chunk[0],by=from.chunk[1],bz=from.chunk[2],x0=ch.cx*bx,y0=ch.cy*by,z0=ch.cz*bz;
            const uint64_t xv=std::min(bx,X-x0),yv=std::min(by,Y-y0),zv=std::min(bz,Z-z0);
            const uint16_t* v=(const uint16_t*)vox.data();
            for(uint64_t z=0;z<zv;z++) for(uint64_t y=0;y<yv;y++) memcpy(&vol[(size_t)(x0+(y0+y)*X+(z0+z)*X*Y)],v+(size_t)(y+z*by)*bx,(size_t)xv*2);
        } });
    if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x);
    const uint64_t bx=to.chunk[0],by=to.chunk[1],bz=to.chunk[2],gx=to.grid[0],gy=to.grid[1];
    const size_t nn=(size_t)to.nChunks;
    std::vector<std::vector<uint8_t> > enc(nn);
    std::atomic<size_t> next2(0); FirstError fe2;
    x=runThreads(T,[&](int){
        std::vector<uint16_t> blk((size_t)(bx*by*bz));
        for(;;){ size_t k=next2.fetch_add(1); if(k>=nn||fe2.set) break;   // k = cx + gx*(cy + gy*cz): (z, y, x) order
            const uint64_t x0=(k%gx)*bx,y0=((k/gx)%gy)*by,z0=(k/(gx*gy))*bz;
            const uint64_t xv=std::min(bx,X-x0),yv=std::min(by,Y-y0),zv=std::min(bz,Z-z0);
            std::fill(blk.begin(),blk.end(),(uint16_t)0);
            for(uint64_t z=0;z<zv;z++) for(uint64_t y=0;y<yv;y++) memcpy(&blk[(size_t)((y+z*by)*bx)],&vol[(size_t)(x0+(y0+y)*X+(z0+z)*X*Y)],(size_t)xv*2);
            if(!imscore::encodeChunk((const uint8_t*)blk.data(),blk.size()*2,enc[k])){ fe2.put("LZ4 could not compress a chunk"); break; }
        } });
    if(!fe2.msg.empty()||!x.empty()) return Status(Code::Internal,!fe2.msg.empty()?fe2.msg:x);
    std::string e;
    hid_t d=mono_create_dataset(g,"Data",mono_idx_geom(to),&e);
    if(d<0) return Status(Code::WriteFailed,e);
    Status s;
    for(size_t k=0;s.ok()&&k<nn;k++){
        hsize_t off[3]={(hsize_t)((k/(gx*gy))*bz),(hsize_t)(((k/gx)%gy)*by),(hsize_t)((k%gx)*bx)};
        if(H5Dwrite_chunk(d,H5P_DEFAULT,0,off,enc[k].size(),enc[k].data())<0) s=Status(Code::WriteFailed,"cannot write a re-chunked chunk (HDF5 error above)");
    }
    std::vector<PartChunk> got; IterState it; it.g=&to; it.out=&got;
    if(s.ok()&&H5Dchunk_iter(d,H5P_DEFAULT,partChunkCb,&it)<0) s=Status(Code::WriteFailed,"cannot list the re-chunked chunks: "+it.err);
    if(s.ok()){ std::sort(got.begin(),got.end(),lessZYX);
        if(got.size()!=nn) s=Status(Code::Internal,"wrote "+u64s(nn)+" re-chunked chunks, the dataset holds "+u64s(got.size()));
        for(size_t k=0;s.ok()&&k<nn;k++){
            if(got[k].size!=enc[k].size()) s=Status(Code::Internal,"a re-chunked chunk has the wrong size in the dataset");
            MonoChunk mc; memset(&mc,0,sizeof mc);
            mc.offset=got[k].addr; mc.cx=got[k].cx; mc.cy=got[k].cy; mc.cz=got[k].cz; mc.size=got[k].size; mc.level=(uint16_t)to.level;
            out.push_back(mc); } }
    if(H5Dclose(d)<0&&s.ok()) s=Status(Code::WriteFailed,"cannot close a re-chunked dataset");
    return s;
}
Status readHist(const std::string& p,const MonoIdxLevel& g,std::vector<uint64_t>& bins){
    std::vector<uint8_t> b;
    if(!imscore::readFileBytes(p,b)) return Status(Code::NotFound,"missing histogram "+p);
    if(b.size()!=imscore::HIST_BYTES) return Status(Code::ReadFailed,p+": "+u64s(b.size())+" bytes, expected "+u64s(imscore::HIST_BYTES));
    bins.resize(65536); memcpy(bins.data(),b.data(),imscore::HIST_BYTES);
    uint64_t n=0; for(size_t i=0;i<bins.size();i++) n+=bins[i];
    if(n!=voxelsOf(g)) return Status(Code::ReadFailed,p+" counts "+u64s(n)+" voxels, the level has "+u64s(voxelsOf(g)));
    return Status();
}
#endif
}  // namespace

namespace piw {

Status monoInit(const MonoInitOptions& o) noexcept{
    try{
#ifdef _WIN32
        (void)o; return Status(Code::Unsupported,NOSUP);
#else
        if(o.workdir.empty()) return Status(Code::InvalidArgument,"monoinit needs a workdir");
        if(o.timepoints<1||o.channels<1) return Status(Code::InvalidArgument,"monoinit needs timepoints >= 1 and channels >= 1");
        for(int a=0;a<3;a++) if(!(o.voxelSize[a]>0)||!std::isfinite(o.voxelSize[a])) return Status(Code::InvalidArgument,"monoinit needs voxel sizes > 0");
        // the metadata, resolved once here, as the converter resolves it (a bad option fails before anything is written)
        piw::ConvertOptions mo; mo.files=o.files; mo.channelNames=o.channelNames; mo.channelColors=o.channelColors;
        mo.emissionWavelengths=o.emissionWavelengths; mo.excitationWavelengths=o.excitationWavelengths; mo.timeInterval=o.timeInterval; mo.startTime=o.startTime;
        imsmeta::Metadata md;
        { std::string me=resolveMonoMetadata(mo,o.timepoints,o.channels,md); if(!me.empty()) return Status(Code::InvalidArgument,me); }
        const bool metaGiven=!o.files.empty()||!o.channelNames.empty()||!o.channelColors.empty()||!o.emissionWavelengths.empty()||
                             !o.excitationWavelengths.empty()||o.timeInterval!=0||!o.startTime.empty();
        const std::string wd=trimSlash(o.workdir), p=wd+"/manifest.json";
        imscore::Status ms=imscore::makeDirs(wd); if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
        if(exists(p)){                             // keep a manifest that agrees (e.g. setup's, a superset)
            MonoManifest m; Status s=readMonoManifest(wd,m);
            bool same=s.ok()&&m.nT==o.timepoints&&m.nC==o.channels&&(o.outputFile.empty()||m.output==o.outputFile);
            for(int a=0;same&&a<3;a++) same=imscore::voxClean(m.vox[a])==imscore::voxClean(o.voxelSize[a]);
            if(same&&metaGiven) same=sameMetadata(m.meta,md,o.files.empty()&&o.startTime.empty());
            if(same) return Status();
            return Status(Code::InvalidArgument,p+" already exists with other values"+(s.ok()?std::string():" ("+s.message+")")+"; use a new workdir");
        }
        nlohmann::ordered_json j;
        j["timepoints"]=o.timepoints; j["channels"]=o.channels;
        j["voxel"]={imscore::voxClean(o.voxelSize[0]),imscore::voxClean(o.voxelSize[1]),imscore::voxClean(o.voxelSize[2])};
        if(!o.outputFile.empty()) j["output"]=o.outputFile;
        j["metadata"]=imscore::metadataJson(md);
        std::string s=j.dump(2)+"\n", e;      // a unique temporary name: tasks may all run monoinit at once
        return mono_write_atomic(p,s.data(),s.size(),&e)?Status():Status(Code::WriteFailed,e);
#endif
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status monoPlace(const MonoPlaceOptions& o,MonoPlaceResult* result) noexcept{
    try{
#ifdef _WIN32
        (void)o; (void)result; return Status(Code::Unsupported,NOSUP);
#else
        const double t0=seconds();
        MonoPlaceResult res;
        const std::string wd=trimSlash(o.workdir);
        MonoManifest m; Status s=readMonoManifest(wd,m); if(!s.ok()) return s;
        if(o.timepoint<0||o.timepoint>=m.nT||o.channel<0||o.channel>=m.nC)
            return Status(Code::InvalidArgument,"(t="+std::to_string(o.timepoint)+", c="+std::to_string(o.channel)+") is outside the manifest's "
                          +std::to_string(m.nT)+" timepoints x "+std::to_string(m.nC)+" channels");
        const int T=threadsOf(o.numThreads), t=o.timepoint, c=o.channel;
        res.owner=ownerOf(t,c); res.threads=T;
        std::string e;
        MonoClaims claims;
        if(!claims.open(monoDir(wd),"",&e)) return Status(Code::WriteFailed,e);
        { uint64_t end=0; bool sealed=false;
          if(!claims.end(&end,&sealed,&e)) return Status(Code::ReadFailed,e);
          if(sealed) return Status(Code::InvalidArgument,"the single file in "+wd+" was already finished (claims journal sealed): no more parts can be placed"); }

        // 1. scan the part: geometry, every chunk's address and size
        std::vector<PartLevel> lv;
        { std::lock_guard<std::mutex> lock(detail::libraryMutex()); s=scanPart(o.partFile,lv); }
        if(!s.ok()) return s;
        std::vector<MonoIdxLevel> geo; for(size_t r=0;r<lv.size();r++) geo.push_back(lv[r].g);
        s=checkLayout(wd,geo,o.partFile); if(!s.ok()) return s;
        res.levels=(int)lv.size();
        res.scanSeconds=seconds()-t0;

        // 2. the blob: every chunk in (level, z, y, x) order, back to back
        struct Item { uint32_t level; size_t idx; };
        std::vector<Item> order; std::vector<uint64_t> blobOff; uint64_t total=0;
        for(size_t r=0;r<lv.size();r++) for(size_t i=0;i<lv[r].chunks.size();i++){
            Item it; it.level=(uint32_t)r; it.idx=i; order.push_back(it); blobOff.push_back(total); total+=lv[r].chunks[i].size; }
        res.chunks=order.size(); res.bytes=total;
        std::vector<LevelInfo> li; size_t maxRaw=0;
        for(size_t r=0;r<lv.size();r++){ li.push_back(levelInfo(lv[r].g)); maxRaw=std::max(maxRaw,li.back().raw); }
        // batches of about 128 MiB: one is read and decoded while the previous one is handed to the writer
        struct Batch { size_t first, end; uint64_t bytes; };
        std::vector<Batch> batches; size_t maxBatch=0;
        for(size_t k=0;k<order.size();){
            Batch b; b.first=k; b.bytes=0;
            while(k<order.size()&&(b.bytes==0||b.bytes+lv[order[k].level].chunks[order[k].idx].size<=(128u<<20))){ b.bytes+=lv[order[k].level].chunks[order[k].idx].size; k++; }
            b.end=k; batches.push_back(b); maxBatch=std::max(maxBatch,(size_t)b.bytes);
        }

        // 3. claim the range, then read + decode + count (worker threads) while the blob writer writes
        const double tp0=seconds();
        MonoClaim rec;
        if(!claims.claim(res.owner,total,rec,&e)) return Status(Code::WriteFailed,e);
        res.claimOffset=rec.offset; res.claimSize=rec.size;
        int fd=open(o.partFile.c_str(),O_RDONLY);
        if(fd<0) return Status(Code::ReadFailed,"cannot open part "+o.partFile+": "+strerror(errno));
        std::vector<std::vector<uint64_t> > hist((size_t)T,std::vector<uint64_t>(lv.size()*65536,0));
        std::vector<std::vector<uint8_t> > scratch((size_t)T);
        std::vector<uint8_t> buf[2]; buf[0].resize(maxBatch); buf[1].resize(batches.size()>1?maxBatch:0);
        auto decodeBatch=[&](const Batch& B,uint8_t* dst)->std::string{
            std::atomic<size_t> next(B.first); FirstError fe;
            std::string x=runThreads(T,[&](int w){
                std::vector<uint8_t>& sh=scratch[(size_t)w]; if(sh.size()<maxRaw) sh.resize(maxRaw);
                for(;;){
                    size_t k=next.fetch_add(1); if(k>=B.end||fe.set) break;
                    const Item& it=order[k]; const PartChunk& pc=lv[it.level].chunks[it.idx];
                    uint8_t* p=dst+(blobOff[k]-blobOff[B.first]);
                    int re=preadAll(fd,p,pc.size,pc.addr); std::string why;
                    if(re) why=std::string("cannot read it from the part: ")+strerror(re);
                    else { imscore::Status cs=countChunk(p,pc.size,pc.cx,pc.cy,pc.cz,li[it.level],sh.data(),hist[(size_t)w].data()+(size_t)it.level*65536); if(!cs.ok()) why=cs.message; }
                    if(!why.empty()){ fe.put("part "+o.partFile+", level "+u64s(it.level)+" chunk ("+u64s(pc.cx)+","+u64s(pc.cy)+","+u64s(pc.cz)+"): "+why); break; }
                }
            });
            return !fe.msg.empty()?fe.msg:x;
        };
        MonoBlobWriter writer; std::string err;
        if(!writer.open(partialPath(wd),rec.offset,rec.size,T,&e)) err=e;
        double tDecode=0;
        if(err.empty()&&!batches.empty()){ double a=seconds(); err=decodeBatch(batches[0],buf[0].data()); tDecode+=seconds()-a; }
        for(size_t b=1;err.empty()&&b<=batches.size();b++){
            bool appended=true; std::string appendErr; const uint8_t* src=buf[(b-1)%2].data(); size_t n=(size_t)batches[b-1].bytes;
            std::thread app([&writer,&appended,&appendErr,src,n](){
                try{ appended=writer.append(src,n,&appendErr); }catch(const std::exception& x){ appended=false; appendErr=x.what(); } });
            if(b<batches.size()){ double a=seconds(); err=decodeBatch(batches[b],buf[b%2].data()); tDecode+=seconds()-a; }
            app.join();
            if(!appended&&err.empty()) err=appendErr;
        }
        close(fd);
        if(err.empty()&&!writer.finish(&e)) err=e;
        if(!err.empty()) return Status(Code::WriteFailed,err+" (nothing was committed; run monoplace again)");
        res.decodeSeconds=tDecode; res.direct=writer.direct();
        res.placeSeconds=seconds()-tp0;

        // 4. histograms, then the sidecar (the commit point)
        MonoIdx x; x.owner=res.owner; x.timepoint=t; x.channel=c; x.claimOffset=rec.offset; x.claimSize=rec.size; x.blobBytes=total;
        x.levels=geo; x.chunks.reserve(order.size());
        for(size_t r=0;r<lv.size();r++){
            std::vector<uint64_t> sum(65536,0);
            for(int w=0;w<T;w++){ const uint64_t* h=hist[(size_t)w].data()+r*65536; for(size_t i=0;i<65536;i++) sum[i]+=h[i]; }
            uint64_t n=0; for(size_t i=0;i<65536;i++) n+=sum[i];
            if(n!=voxelsOf(geo[r])) return Status(Code::Internal,"level "+u64s(r)+": counted "+u64s(n)+" voxels, the level has "+u64s(voxelsOf(geo[r])));
            if(!mono_write_atomic(histPath(wd,(int)r,t,c),sum.data(),imscore::HIST_BYTES,&e)) return Status(Code::WriteFailed,e);
        }
        for(size_t k=0;k<order.size();k++){ const PartChunk& pc=lv[order[k].level].chunks[order[k].idx];
            MonoChunk mc; memset(&mc,0,sizeof mc);
            mc.offset=rec.offset+blobOff[k]; mc.cx=pc.cx; mc.cy=pc.cy; mc.cz=pc.cz; mc.size=pc.size; mc.mask=0; mc.level=(uint16_t)order[k].level;
            x.chunks.push_back(mc); }
        if(!mono_idx_write(idxPath(wd,t,c),x,&e)) return Status(Code::WriteFailed,e);
        res.totalSeconds=seconds()-t0;
        if(result) *result=res;
        return Status();
#endif
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status monoStatus(const std::string& workdir,MonoProgress& p) noexcept{
    try{
        p=MonoProgress();
#ifdef _WIN32
        (void)workdir; return Status(Code::Unsupported,NOSUP);
#else
        const std::string wd=trimSlash(workdir);
        MonoManifest m; Status s=readMonoManifest(wd,m); if(!s.ok()) return s;
        p.timepoints=m.nT; p.channels=m.nC;
        for(int t=0;t<m.nT;t++) for(int c=0;c<m.nC;c++) (exists(idxPath(wd,t,c))?p.placed:p.missing).push_back(ownerOf(t,c));
        std::vector<uint8_t> b; std::string e;
        if(exists(finishedPath(wd))&&mono_read_file(finishedPath(wd),b,&e)){ p.finished=true; p.finishedFile.assign(b.begin(),b.end()); }
        if(exists(monoDir(wd)+"/claims.mode")){            // open() would create the journal: only when there is one
            MonoClaims cl; uint64_t end=0; bool sealed=false;
            if(!cl.open(monoDir(wd),"",&e)||!cl.end(&end,&sealed,&e)) return Status(Code::ReadFailed,e);
            p.sealed=sealed; p.claimedBytes=end;
        }
        return Status();
#endif
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

#ifndef _WIN32
namespace {
// /Thumbnail: not written yet (B-8 builds it from every channel). Called where ImarisWriter writes it, after
// the metadata, so the finisher's order stays fixed when it is added.
Status writeThumbnail(hid_t){ return Status(); }

// The sidecar's chunks of each level (sorted by level, then z, y, x): [first[r], first[r+1]).
std::vector<size_t> levelRanges(MonoIdx& x){
    std::vector<MonoChunk>& v=x.chunks;
    auto less=[](const MonoChunk& a,const MonoChunk& b){
        if(a.level!=b.level) return a.level<b.level;
        if(a.cz!=b.cz) return a.cz<b.cz;
        if(a.cy!=b.cy) return a.cy<b.cy;
        return a.cx<b.cx; };
    if(!std::is_sorted(v.begin(),v.end(),less)) std::sort(v.begin(),v.end(),less);
    std::vector<size_t> first(x.levels.size()+1,v.size());
    for(size_t r=0,k=0;r<x.levels.size();r++){ while(k<v.size()&&v[k].level<r) k++; first[r]=k; }
    return first;
}
}  // namespace
#endif

Status monoFinish(const MonoFinishOptions& o,MonoFinishResult* result) noexcept{
    try{
#ifdef _WIN32
        (void)o; (void)result; return Status(Code::Unsupported,NOSUP);
#else
        const double t0=seconds();
        MonoFinishResult res;
        const std::string wd=trimSlash(o.workdir), partial=partialPath(wd);
        MonoManifest m; Status s=readMonoManifest(wd,m); if(!s.ok()) return s;
        const int T=threadsOf(o.numThreads), nT=m.nT, nC=m.nC;
        res.timepoints=nT; res.channels=nC;
        res.outputFile=!o.outputFile.empty()?o.outputFile:(!m.output.empty()?m.output:wd+"/output.ims");
        std::string e;
        if(!exists(partial)){
            std::vector<uint8_t> b;
            if(exists(finishedPath(wd))&&mono_read_file(finishedPath(wd),b,&e)){
                res.alreadyFinished=true; res.outputFile.assign(b.begin(),b.end());
                if(result) *result=res;
                return Status();
            }
            return Status(Code::NotFound,"no "+partial+": no part was placed yet (run monoplace first)");
        }
        std::vector<MonoIdxLevel> geo; s=readLayout(wd,geo); if(!s.ok()) return s;
        res.levels=(int)geo.size();

        // 1. every sidecar: present, complete, consistent with the layout (read in parallel: many small files)
        const int nP=nT*nC;
        std::vector<MonoIdx> idx((size_t)nP); std::vector<char> have((size_t)nP,0);
        { std::atomic<int> next(0); FirstError fe; std::string x=runThreads(std::min(T,16),[&](int){
            for(;;){ int k=next.fetch_add(1); if(k>=nP||fe.set) break;
                int t=k/nC, c=k%nC; std::string p=idxPath(wd,t,c), why;
                if(!exists(p)) continue;
                MonoIdx& ix=idx[(size_t)k];
                if(!mono_idx_read(p,ix,&why)||!mono_idx_validate(ix,true,&why)){ fe.put(why+" (run monoplace for t="+std::to_string(t)+", c="+std::to_string(c)+" again)"); break; }
                if(ix.owner!=ownerOf(t,c)||ix.timepoint!=t||ix.channel!=c){ fe.put(p+" belongs to another part ("+ix.owner+")"); break; }
                Status ls=compareLayout(geo,ix.levels,monoDir(wd)+"/layout.json",p); if(!ls.ok()){ fe.put(ls.message); break; }
                for(int r=0;r<(int)geo.size()&&why.empty();r++){ struct stat st; std::string h=histPath(wd,r,t,c);
                    if(stat(h.c_str(),&st)!=0||(uint64_t)st.st_size!=imscore::HIST_BYTES) why="missing or damaged histogram "+h; }
                if(!why.empty()){ fe.put(why+" (run monoplace for t="+std::to_string(t)+", c="+std::to_string(c)+" again)"); break; }
                have[(size_t)k]=1;
            } });
          if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x); }
        for(int k=0;k<nP;k++) if(!have[(size_t)k]) res.missing.push_back(ownerOf(k/nC,k%nC));
        if(!res.missing.empty()){
            std::string list; for(size_t i=0;i<res.missing.size()&&i<20;i++) list+=(i?", ":"")+res.missing[i];
            if(res.missing.size()>20) list+=", ...";
            if(result) *result=res;
            return Status(Code::NotFound,u64s(res.missing.size())+" of "+u64s(nP)+" parts are not placed yet: "+list+" (run monoplace for them)");
        }

        // 2. each sidecar's range must be a claim of its owner in the journal; then seal: the raw region ends there
        MonoClaims claims;
        if(!claims.open(monoDir(wd),"",&e)) return Status(Code::ReadFailed,e);
        std::vector<MonoClaim> recs; bool sealed=false;
        if(!claims.list(recs,&sealed,&e)) return Status(Code::ReadFailed,e);
        { std::multimap<std::string,const MonoClaim*> byOwner; for(size_t i=0;i<recs.size();i++) if(!recs[i].seal) byOwner.insert(std::make_pair(recs[i].owner,&recs[i]));
          for(int k=0;k<nP;k++){ const MonoIdx& ix=idx[(size_t)k]; bool found=false;
            auto range=byOwner.equal_range(ix.owner);
            for(auto it=range.first;!found&&it!=range.second;++it) found=it->second->offset==ix.claimOffset&&it->second->size==ix.claimSize;
            if(!found) return Status(Code::ReadFailed,"the sidecar of "+ix.owner+" names bytes ["+u64s(ix.claimOffset)+", "+u64s(ix.claimOffset+ix.claimSize)
                                     +") that the claims journal does not give it (a sidecar of another run? place that part again)"); } }
        MonoClaim seal;
        if(!claims.seal(seal,&e)) return Status(Code::WriteFailed,e);
        const uint64_t rawEnd=seal.offset; res.rawEnd=rawEnd;
        for(int k=0;k<nP;k++) res.chunks+=idx[(size_t)k].chunks.size();
        std::vector<std::vector<size_t> > first((size_t)nP);
        for(int k=0;k<nP;k++) first[(size_t)k]=levelRanges(idx[(size_t)k]);
        // cheap check that every blob was written: the LZ4 headers of each part's first and last chunk
        { int fd=open(partial.c_str(),O_RDONLY);
          if(fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
          std::string bad;
          for(int k=0;k<nP&&bad.empty();k++){ const MonoIdx& ix=idx[(size_t)k];
            for(int end=0;end<2&&bad.empty();end++){ const MonoChunk& ch=ix.chunks[end?ix.chunks.size()-1:0]; uint8_t h[16];
              const MonoIdxLevel& g=geo[ch.level]; uint64_t raw=(uint64_t)g.chunk[0]*g.chunk[1]*g.chunk[2]*2;
              if(preadAll(fd,h,16,ch.offset)!=0||imscore::rbe(h,8)!=raw||imscore::rbe(h+12,4)+16!=ch.size)
                  bad="the chunks of "+ix.owner+" are not in "+partial+" at "+u64s(ch.offset)+" (its blob was not written: place that part again)"; } }
          close(fd);
          if(!bad.empty()) return Status(Code::ReadFailed,bad); }
        res.readSeconds=seconds()-t0;

        // 3. the HDF5 structure over the placed chunks, in ImarisWriter's order (imscore.h): root attributes and
        // /DataSet, every (t, c) and its levels, /DataSetInfo, then the thumbnail
        auto histOf=[&](int r,int c,int t,std::vector<uint64_t>& bins)->imscore::Status{
            Status hs=readHist(histPath(wd,r,t,c),geo[(size_t)r],bins); return hs.ok()?imscore::Status():imscore::Status::fail(hs.message); };
        // levels whose chunks a single-node conversion of all nT timepoints lays out differently: re-chunked
        std::vector<MonoIdxLevel> target; std::vector<char> rechunk;
        if(!singleNodeLayout(geo,nT,target,rechunk)) res.note="the parts' chunks are not the converter's 1-timepoint layout: no level was re-chunked";
        uint64_t armedWant=0;
        for(size_t r=0;r<geo.size();r++){ if(rechunk[r]) res.rechunkedLevels.push_back((int)r); else armedWant+=(uint64_t)nP*geo[r].nChunks; }
        std::vector<std::vector<std::vector<MonoChunk> > > newChunks((size_t)nP,std::vector<std::vector<MonoChunk> >(geo.size()));
        // the chunks the file's index holds for part k, level r
        auto chunksOf=[&](int k,size_t r,const MonoChunk*& p,size_t& n){
            if(rechunk[r]){ p=newChunks[(size_t)k][r].data(); n=newChunks[(size_t)k][r].size(); }
            else { p=idx[(size_t)k].chunks.data()+first[(size_t)k][r]; n=first[(size_t)k][r+1]-first[(size_t)k][r]; } };
        FdGuard rfd(res.rechunkedLevels.empty()?-1:open(partial.c_str(),O_RDONLY));
        if(!res.rechunkedLevels.empty()&&rfd.fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
        MonoVfd vfd(rawEnd);
        { std::lock_guard<std::mutex> lock(detail::libraryMutex());
          const double ti=seconds();
          hid_t fapl=mono_vfd_fapl(vfd,&e);
          if(fapl<0) return Status(Code::WriteFailed,e);
          hid_t f=H5Fcreate(partial.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl);
          std::string why; mono_vfd_ok(vfd,&why);
          if(f<0) return Status(Code::WriteFailed,"cannot build the HDF5 structure in "+partial+(why.empty()?std::string(" (HDF5 error above)"):": "+why));
          hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
          imscore::Manifest im; im.nC=nC; im.nT=nT; im.X=geo[0].size[0]; im.Y=geo[0].size[1]; im.Z=geo[0].size[2]; im.elem=2;
          for(int a=0;a<3;a++) im.vox[a]=m.vox[a];
          im.meta=m.meta;
          std::vector<int> chs,tps; for(int c=0;c<nC;c++) chs.push_back(c); for(int t=0;t<nT;t++) tps.push_back(t);
          imscore::Status ws=imscore::writeImsRoot(f,lcpl);
          Status bs=ws.ok()?Status():Status(Code::WriteFailed,ws.message);
          std::vector<uint64_t> bins;
          for(int t=0;bs.ok()&&t<nT;t++) for(int c=0;bs.ok()&&c<nC;c++){ MonoIdx& ix=idx[(size_t)(t*nC+c)]; const std::vector<size_t>& fr=first[(size_t)(t*nC+c)];
            for(size_t r=0;bs.ok()&&r<geo.size();r++){
                const std::string gp="DataSet/ResolutionLevel "+u64s(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
                hid_t g=H5Gcreate2(f,gp.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
                if(g<0){ bs=Status(Code::WriteFailed,"cannot create "+gp+" (HDF5 error above)"); break; }
                bool ok=imscore::setStrAttr(g,"ImageSizeX",u64s(geo[r].size[0]))&&imscore::setStrAttr(g,"ImageSizeY",u64s(geo[r].size[1]))&&imscore::setStrAttr(g,"ImageSizeZ",u64s(geo[r].size[2]));
                const MonoChunk* base=ix.chunks.data()+fr[r]; std::string ie;
                if(ok&&rechunk[r]){ Status rs=rechunkLevel(rfd.fd,g,geo[r],target[r],base,fr[r+1]-fr[r],T,newChunks[(size_t)(t*nC+c)][r]);
                    if(!rs.ok()) bs=Status(rs.code,gp+": "+rs.message); else res.rechunkedChunks+=newChunks[(size_t)(t*nC+c)][r].size(); }
                else if(ok&&!mono_index_dataset(vfd,g,"Data",mono_idx_geom(geo[r]),fr[r+1]-fr[r],[base](size_t k,MonoChunk& ch){ ch=base[k]; },&ie))
                    bs=Status(Code::WriteFailed,gp+": "+ie);
                if(bs.ok()&&ok){ Status hs=readHist(histPath(wd,(int)r,t,c),geo[r],bins);
                    if(!hs.ok()) bs=hs; else ok=imscore::writeHistogramFromBins(g,bins); }
                if(bs.ok()&&!ok) bs=Status(Code::WriteFailed,"cannot write "+gp+" (HDF5 error above)");
                H5Gclose(g);
            } }
          if(bs.ok()){ ws=imscore::writeImsDataSetInfo(f,im,lcpl,chs,tps,histOf); if(!ws.ok()) bs=Status(Code::WriteFailed,ws.message); }
          if(bs.ok()) bs=writeThumbnail(f);
          H5Pclose(lcpl);
          if(H5Fclose(f)<0&&bs.ok()) bs=Status(Code::WriteFailed,"cannot close "+partial+" (HDF5 error above)");
          if(bs.ok()&&!mono_vfd_ok(vfd,&why)) bs=Status(Code::WriteFailed,"the placement driver refused a write: "+why);
          const MonoVfdStats& st=mono_vfd_stats(vfd);
          if(bs.ok()&&(st.armed!=armedWant||st.placed!=armedWant||st.discarded!=armedWant))
              bs=Status(Code::Internal,"indexed "+u64s(st.placed)+" of "+u64s(armedWant)+" placed chunks");
          if(!bs.ok()) return Status(bs.code,bs.message+" (the placed chunks are untouched: monofinish can run again)");
          res.indexSeconds=seconds()-ti; }

        // 4. reopen with the stock driver: every chunk index must equal its sidecar
        { std::lock_guard<std::mutex> lock(detail::libraryMutex());
          const double tc=seconds();
          hid_t fapl=H5Pcreate(H5P_FILE_ACCESS); H5Pset_file_locking(fapl,false,true);
          hid_t f=H5Fopen(partial.c_str(),H5F_ACC_RDONLY,fapl); H5Pclose(fapl);
          if(f<0) return Status(Code::ReadFailed,"the finished "+partial+" does not open with the stock HDF5 driver (HDF5 error above)");
          Status cs;
          for(int t=0;cs.ok()&&t<nT;t++) for(int c=0;cs.ok()&&c<nC;c++){
            for(size_t r=0;cs.ok()&&r<geo.size();r++){
                const std::string dp="DataSet/ResolutionLevel "+u64s(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c)+"/Data";
                hid_t d=H5Dopen2(f,dp.c_str(),H5P_DEFAULT); uint64_t bad=0; std::string ve; const MonoChunk* base=NULL; size_t n=0;
                chunksOf(t*nC+c,r,base,n);
                if(d<0) cs=Status(Code::ReadFailed,"cannot open "+dp+" in the finished file");
                else if(!mono_verify_dataset(d,mono_idx_geom(target[r]),n,[base](size_t k,MonoChunk& ch){ ch=base[k]; },&bad,&ve))
                    cs=Status(Code::ReadFailed,dp+" in the finished file: "+ve);
                if(d>=0){ H5Dclose(d); }
            } }
          if(H5Fclose(f)<0&&cs.ok()) cs=Status(Code::ReadFailed,"cannot close "+partial);
          if(!cs.ok()) return cs;
          struct stat st;
          if(stat(partial.c_str(),&st)!=0||(uint64_t)st.st_size!=mono_vfd_stats(vfd).eoa)
              return Status(Code::Internal,partial+" does not end where HDF5's allocation ends");
          res.fileBytes=(uint64_t)st.st_size;
          res.checkSeconds=seconds()-tc; }

        // 5. --verify: decode every chunk from the file and compare each level's histogram with the part's
        if(o.verify){
            const double tv=seconds();
            int fd=open(partial.c_str(),O_RDONLY);
            if(fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
            std::vector<LevelInfo> li; size_t maxRaw=0; for(size_t r=0;r<geo.size();r++){ li.push_back(levelInfo(target[r])); maxRaw=std::max(maxRaw,li.back().raw); }
            std::vector<std::vector<uint64_t> > hist((size_t)T); std::vector<std::vector<uint8_t> > cbuf((size_t)T), sh((size_t)T);
            std::string bad;
            for(int k=0;bad.empty()&&k<nP;k++){ const MonoIdx& ix=idx[(size_t)k];
                for(int w=0;w<T;w++) hist[(size_t)w].assign(geo.size()*65536,0);
                std::vector<const MonoChunk*> items;   // what the file's index holds for this part
                for(size_t r=0;r<geo.size();r++){ const MonoChunk* p=NULL; size_t n=0; chunksOf(k,r,p,n); for(size_t i=0;i<n;i++) items.push_back(p+i); }
                std::atomic<size_t> next(0); FirstError fe;
                std::string x=runThreads(T,[&](int w){
                    if(sh[(size_t)w].size()<maxRaw) sh[(size_t)w].resize(maxRaw);
                    for(;;){ size_t i=next.fetch_add(1); if(i>=items.size()||fe.set) break; const MonoChunk& ch=*items[i];
                        std::vector<uint8_t>& cb=cbuf[(size_t)w]; cb.resize(ch.size); std::string why;
                        int re=preadAll(fd,cb.data(),ch.size,ch.offset);
                        if(re) why=strerror(re);
                        else { imscore::Status ds=countChunk(cb.data(),ch.size,ch.cx,ch.cy,ch.cz,li[ch.level],sh[(size_t)w].data(),hist[(size_t)w].data()+(size_t)ch.level*65536); if(!ds.ok()) why=ds.message; }
                        if(!why.empty()){ fe.put(ix.owner+" level "+u64s(ch.level)+" chunk ("+u64s(ch.cx)+","+u64s(ch.cy)+","+u64s(ch.cz)+") at "+u64s(ch.offset)+": "+why); break; } } });
                if(!fe.msg.empty()||!x.empty()){ bad=!fe.msg.empty()?fe.msg:x; break; }
                for(size_t r=0;bad.empty()&&r<geo.size();r++){
                    std::vector<uint64_t> want; Status hs=readHist(histPath(wd,(int)r,ix.timepoint,ix.channel),geo[r],want);
                    if(!hs.ok()){ bad=hs.message; break; }
                    for(size_t i=0;i<65536&&bad.empty();i++){ uint64_t n=0; for(int w=0;w<T;w++) n+=hist[(size_t)w][r*65536+i];
                        if(n!=want[i]) bad=ix.owner+" level "+u64s(r)+": the voxels in the file differ from the part's (value "+u64s(i)+")"; }
                }
            }
            close(fd);
            if(!bad.empty()) return Status(Code::ReadFailed,"--verify: "+bad);
            res.verifySeconds=seconds()-tv;
        }

        // 6. done: record the output, then rename the file to it
        const std::string& out=res.outputFile;
        if(!mono_write_atomic(finishedPath(wd),out.data(),out.size(),&e)) return Status(Code::WriteFailed,e);
        if(rename(partial.c_str(),out.c_str())!=0){ int re=errno; unlink(finishedPath(wd).c_str());
            return Status(Code::WriteFailed,"cannot rename "+partial+" to "+out+": "+strerror(re)
                          +(re==EXDEV?" (the output must be on the workdir's filesystem)":"")+"; the finished file stays at "+partial); }
        res.totalSeconds=seconds()-t0;
        if(result) *result=res;
        return Status();
#endif
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

}  // namespace piw

// ---------------- command line ----------------
namespace {
int fail(const piw::Status& s){ fprintf(stderr,"ERROR: %s\n",s.message.c_str()); return 1; }
int usage(const char* cmd){
    fprintf(stderr,"usage: parallelimariswriter %s (see src/mono.h)\n"
        "  monoinit   <workdir> <nT> <nC> <vx,vy,vz> [out.ims] [--option value ...]\n"
        "             (the converter's metadata options: --channel-names, --colors, --emission, --excitation,\n"
        "              --time-interval, --start-time)\n"
        "  monoplace  <workdir> <t> <c> <part.ims> [--threads N]\n"
        "  monofinish <workdir> [out.ims] [--verify] [--threads N]\n"
        "  monostatus <workdir>\n",cmd);
    return 2;
}
bool parseInt(const char* s,int& v){ char* e=NULL; errno=0; long r=strtol(s,&e,10); if(e==s||*e||errno||r<0||r>2147483647L) return false; v=(int)r; return true; }
// Splits off --threads N, --verify and, with meta (monoinit), the metadata options (--name value or --name=value;
// a bad value: metaError); the rest are positional.
bool options(int argc,char** argv,std::vector<std::string>& pos,int& threads,bool& verify,bool allowVerify,
             piw::ConvertOptions* meta,std::string& metaError){
    for(int i=2;i<argc;i++){ std::string a=argv[i];
        if(a=="--threads"){ if(i+1>=argc||!parseInt(argv[i+1],threads)||threads<1) return false; i++; }
        else if(a=="--verify"&&allowVerify) verify=true;
        else if(a.size()>2&&a.compare(0,2,"--")==0){
            size_t eq=a.find('='); std::string name=a.substr(2,eq==std::string::npos?std::string::npos:eq-2), value;
            if(!meta||!imsmeta::isMetadataOption(name)) return false;
            if(eq!=std::string::npos) value=a.substr(eq+1); else if(i+1<argc) value=argv[++i]; else return false;
            if(metaError.empty()) metaError=imsmeta::parseMetadataOption(name,value,*meta); }
        else pos.push_back(a); }
    return true;
}
double gbps(uint64_t b,double s){ return s>0?(double)b/1e9/s:0.0; }
}  // namespace

bool isMonoCommand(const char* c){ return c&&(!strcmp(c,"monoinit")||!strcmp(c,"monoplace")||!strcmp(c,"monofinish")||!strcmp(c,"monostatus")); }

int runMono(int argc,char** argv){
    const std::string cmd=argc>=2?argv[1]:"";
    std::vector<std::string> pos; int threads=0; bool verify=false; piw::ConvertOptions meta; std::string metaError;
    if(!options(argc,argv,pos,threads,verify,cmd=="monofinish",cmd=="monoinit"?&meta:NULL,metaError)) return usage(cmd.c_str());
    if(!metaError.empty()){ fprintf(stderr,"ERROR: %s\n",metaError.c_str()); return 1; }
    if(cmd=="monoinit"){
        piw::MonoInitOptions o;
        o.channelNames=meta.channelNames; o.channelColors=meta.channelColors; o.emissionWavelengths=meta.emissionWavelengths;
        o.excitationWavelengths=meta.excitationWavelengths; o.timeInterval=meta.timeInterval; o.startTime=meta.startTime;
        if(pos.size()<4||pos.size()>5||!parseInt(pos[1].c_str(),o.timepoints)||!parseInt(pos[2].c_str(),o.channels)) return usage("monoinit");
        char tail=0;
        if(sscanf(pos[3].c_str(),"%g,%g,%g%c",&o.voxelSize[0],&o.voxelSize[1],&o.voxelSize[2],&tail)!=3){
            fprintf(stderr,"ERROR: bad voxel size '%s' (expected vx,vy,vz > 0)\n",pos[3].c_str()); return 1; }
        o.workdir=pos[0]; if(pos.size()>4) o.outputFile=pos[4];
        piw::Status s=piw::monoInit(o); if(!s.ok()) return fail(s);
        printf("monoinit: %s: %d timepoints x %d channels, voxel %g,%g,%g%s%s\n",o.workdir.c_str(),o.timepoints,o.channels,
               o.voxelSize[0],o.voxelSize[1],o.voxelSize[2],o.outputFile.empty()?"":", output ",o.outputFile.c_str());
        return 0;
    }
    if(cmd=="monoplace"){
        piw::MonoPlaceOptions o;
        if(pos.size()!=4||!parseInt(pos[1].c_str(),o.timepoint)||!parseInt(pos[2].c_str(),o.channel)) return usage("monoplace");
        o.workdir=pos[0]; o.partFile=pos[3]; o.numThreads=threads;
        piw::MonoPlaceResult r; piw::Status s=piw::monoPlace(o,&r); if(!s.ok()) return fail(s);
        printf("monoplace %s: %llu chunks, %d levels, %.2f GB at %llu: scan %.1f s, decode %.1f s, place %.1f s (%.2f GB/s, %s, %d threads), total %.1f s\n",
               r.owner.c_str(),(unsigned long long)r.chunks,r.levels,(double)r.bytes/1e9,(unsigned long long)r.claimOffset,r.scanSeconds,r.decodeSeconds,
               r.placeSeconds,gbps(r.bytes,r.placeSeconds),r.direct?"direct I/O":"buffered + sync",r.threads,r.totalSeconds);
        return 0;
    }
    if(cmd=="monofinish"){
        piw::MonoFinishOptions o;
        if(pos.empty()||pos.size()>2) return usage("monofinish");
        o.workdir=pos[0]; if(pos.size()>1) o.outputFile=pos[1]; o.verify=verify; o.numThreads=threads;
        piw::MonoFinishResult r; piw::Status s=piw::monoFinish(o,&r);
        if(!s.ok()){ fail(s); return r.missing.empty()?1:3; }
        if(r.alreadyFinished){ printf("monofinish: already finished: %s\n",r.outputFile.c_str()); return 0; }
        printf("monofinish: %d timepoints x %d channels x %d levels, %llu chunks, %.2f GB -> %s\n",r.timepoints,r.channels,r.levels,
               (unsigned long long)r.chunks,(double)r.fileBytes/1e9,r.outputFile.c_str());
        printf("  sidecars + seal %.1f s, HDF5 structure %.1f s (%.1f MB after the raw region), stock-driver check %.1f s%s, total %.1f s\n",
               r.readSeconds,r.indexSeconds,(double)(r.fileBytes-r.rawEnd)/1e6,r.checkSeconds,
               o.verify?(", --verify "+std::to_string((long long)(r.verifySeconds+0.5))+" s").c_str():"",r.totalSeconds);
        if(!r.rechunkedLevels.empty()){ std::string l; for(size_t i=0;i<r.rechunkedLevels.size();i++) l+=(i?", ":"")+std::to_string(r.rechunkedLevels[i]);
            printf("  levels %s re-chunked to the %d-timepoint layout (%llu chunks)\n",l.c_str(),r.timepoints,(unsigned long long)r.rechunkedChunks); }
        if(!r.note.empty()) printf("  note: %s\n",r.note.c_str());
        return 0;
    }
    if(cmd=="monostatus"){
        if(pos.size()!=1) return usage("monostatus");
        piw::MonoProgress p; piw::Status s=piw::monoStatus(pos[0],p); if(!s.ok()) return fail(s);
        printf("monostatus: %s: %llu of %d parts placed (%d timepoints x %d channels), %.2f GB claimed, %s\n",pos[0].c_str(),
               (unsigned long long)p.placed.size(),p.timepoints*p.channels,p.timepoints,p.channels,
               p.claimedBytes>MONO_RAW_BEGIN?(double)(p.claimedBytes-MONO_RAW_BEGIN)/1e9:0.0,
               p.finished?("finished: "+p.finishedFile).c_str():p.sealed?"finishing (sealed)":"open for placements");
        for(size_t i=0;i<p.missing.size()&&i<50;i++) printf("  missing %s\n",p.missing[i].c_str());
        if(p.missing.size()>50) printf("  ... and %llu more\n",(unsigned long long)(p.missing.size()-50));
        return p.missing.empty()?0:3;
    }
    return usage(cmd.c_str());
}
