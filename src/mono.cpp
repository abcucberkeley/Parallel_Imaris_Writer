// Single-file cluster output from per-(timepoint, channel) parts: monoinit, monoplace, monofinish and
// monostatus (see mono.h). HDF5 is called only under the library mutex; reading parts, writing chunks and the
// sidecars run outside it.
#include "mono.h"
#include "imscore.h"
#include "imsthumb.h"
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
const char* const TYPES="the single-file cluster output takes uint8, uint16 and float32 data (tile workers: uint8 and uint16)";
Status internalError(const char* what){ return Status(Code::Internal,std::string("unexpected error: ")+what); }

#ifndef _WIN32
std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }
std::string ownerOf(int t,int c){ char b[48]; snprintf(b,sizeof b,"P_C%d_T%d",c,t); return b; }
std::string trimSlash(std::string w){ while(w.size()>1&&w[w.size()-1]=='/') w.erase(w.size()-1); return w; }
std::string monoDir(const std::string& wd){ return imscore::monoClaimsDir(wd); }
std::string partialPath(const std::string& wd){ return imscore::monoFilePath(wd); }
std::string idxPath(const std::string& wd,int t,int c){ return monoDir(wd)+"/"+ownerOf(t,c)+".idx"; }
std::string histPath(const std::string& wd,int r,int t,int c){ char b[64]; snprintf(b,sizeof b,"/h_L%d_C%d_T%d.hist",r,c,t); return monoDir(wd)+b; }
std::string floatHistPath(const std::string& wd,int t,int c){ return monoDir(wd)+"/"+ownerOf(t,c)+".fhist"; }
std::string finishedPath(const std::string& wd){ return monoDir(wd)+"/finished"; }
double seconds(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
int threadsOf(int n){ return n>0?n:std::max(1,piw::defaultThreads()); }

// ---------------- manifest (timepoints, channels, voxel size, optional output, metadata; setup's store; a plan's kind) ----------------
struct MonoManifest { int nT=0, nC=0; float vox[3]={0,0,0}; std::string output, store, kind; imsmeta::Metadata meta; };
// The metadata of o's fields for nC channels x nT timepoints (imsmeta::resolveMetadataFields; without input files
// the start time is startTime, else the current time)
std::string resolveMonoMetadata(const piw::MonoInitOptions& o,int nT,int nC,imsmeta::Metadata& md){
    bool fits=o.files.empty()||o.files.size()==(size_t)nC; for(size_t c=0;fits&&c<o.files.size();c++) fits=o.files[c].size()==(size_t)nT;
    if(!fits) return "the input files are not "+std::to_string(nC)+" channels x "+std::to_string(nT)+" timepoints";
    return imsmeta::resolveMetadataFields(o,(size_t)nC,(size_t)nT,o.files.empty()?NULL:&o.files,md);
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
        m.store=j.value("store",std::string()); m.kind=j.value("kind",std::string());
        if((hasMeta=j.contains("metadata"))){ imscore::Status ms=imscore::readMetadataJson(j["metadata"],m.nC,m.nT,m.meta);
            if(!ms.ok()) return Status(Code::InvalidArgument,p+": "+ms.message); }
    }catch(const std::exception& e){ return Status(Code::InvalidArgument,"invalid "+p+": "+e.what()); }
    if(m.nT<1||m.nC<1) return Status(Code::InvalidArgument,p+" needs timepoints >= 1 and channels >= 1");
    for(int a=0;a<3;a++) if(!(m.vox[a]>0)||!std::isfinite(m.vox[a])) return Status(Code::InvalidArgument,p+" has a voxel size that is not > 0");
    if(!hasMeta){ std::string e=resolveMonoMetadata(piw::MonoInitOptions(),m.nT,m.nC,m.meta); if(!e.empty()) return Status(Code::InvalidArgument,p+": "+e); }
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
const char* dtypeName(unsigned d);
std::string geomText(const MonoIdxLevel& l){
    char b[256]; snprintf(b,sizeof b,"%s, size %llux%llux%llu, chunks %ux%ux%u",dtypeName(l.dtype),(unsigned long long)l.size[0],(unsigned long long)l.size[1],
                          (unsigned long long)l.size[2],l.chunk[0],l.chunk[1],l.chunk[2]); return b;
}
bool sameGeom(const MonoIdxLevel& a,const MonoIdxLevel& b){
    if(a.level!=b.level||a.dtype!=b.dtype) return false;
    for(int i=0;i<3;i++) if(a.size[i]!=b.size[i]||a.chunk[i]!=b.chunk[i]||a.grid[i]!=b.grid[i]) return false;
    return true;
}
const char* dtypeName(unsigned d){ return d==MONO_U8?"uint8":d==MONO_U16?"uint16":d==MONO_U32?"uint32":d==MONO_F32?"float32":"unknown"; }
unsigned dtypeCode(const std::string& n){ return n=="uint8"?MONO_U8:n=="uint16"?MONO_U16:n=="uint32"?MONO_U32:n=="float32"?MONO_F32:0; }
std::string layoutText(const std::vector<MonoIdxLevel>& lv){
    nlohmann::ordered_json j; j["dtype"]=dtypeName(lv[0].dtype); nlohmann::ordered_json a=nlohmann::ordered_json::array();
    for(size_t r=0;r<lv.size();r++){ const MonoIdxLevel& l=lv[r]; nlohmann::ordered_json e;
        e["size"]={(unsigned long long)l.size[0],(unsigned long long)l.size[1],(unsigned long long)l.size[2]};
        e["chunk"]={l.chunk[0],l.chunk[1],l.chunk[2]}; e["grid"]={l.grid[0],l.grid[1],l.grid[2]}; a.push_back(e); }
    j["levels"]=a; return j.dump(2)+"\n";
}
bool parseLayout(const std::vector<uint8_t>& b,std::vector<MonoIdxLevel>& lv,std::string* err){
    lv.clear();
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        const unsigned dt=dtypeCode(j.value("dtype",std::string()));
        if(dt!=MONO_U8&&dt!=MONO_U16&&dt!=MONO_F32){ *err="unknown dtype"; return false; }
        for(auto& e:j["levels"]){ MonoIdxLevel l; l.level=(uint32_t)lv.size(); l.dtype=dt;
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
    if(!have.empty()&&!lv.empty()&&have[0].dtype!=lv[0].dtype)
        return Status(Code::InvalidArgument,what+" holds "+dtypeName(lv[0].dtype)+" data, "+p+" says "+dtypeName(have[0].dtype)+hint);
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
// A level's stored histograms as ImarisWriter wrote them (float32 parts: copied, see monoPlace): the attribute texts
// and the bins of Histogram (256) and, when written, Histogram1024 (the full histogram).
struct PartHist { bool present=false, has1024=false; std::string min, max, min1024, max1024; std::vector<uint64_t> bins, bins1024; };
struct PartLevel { MonoIdxLevel g; std::vector<PartChunk> chunks; PartHist hist; };
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
// The sample type of a Data dataset: MONO_U8, MONO_U16 or MONO_F32 (little-endian), else 0.
unsigned sampleType(hid_t ty){
    if(ty<0) return 0;
    const H5T_class_t k=H5Tget_class(ty); const size_t n=H5Tget_size(ty);
    if(k==H5T_INTEGER&&H5Tget_sign(ty)==H5T_SGN_NONE&&(n==1||(n==2&&H5Tget_order(ty)==H5T_ORDER_LE))) return n==1?MONO_U8:MONO_U16;
    if(k==H5T_FLOAT&&n==4&&H5Tget_order(ty)==H5T_ORDER_LE&&H5Tequal(ty,H5T_IEEE_F32LE)>0) return MONO_F32;
    return 0;
}
// One level's Data: uint8, uint16 or float32, chunked, padded to whole chunks, (shuffle +) LZ4 like the converter
// writes, every chunk of the grid stored (a silently failed conversion is caught here).
Status scanData(hid_t d,const std::string& where,PartLevel& L){
    hid_t ty=H5Dget_type(d), sp=H5Dget_space(d), pl=H5Dget_create_plist(d);
    Status s;
    L.g.dtype=sampleType(ty);
    hsize_t dims[3]={0,0,0}, chk[3]={0,0,0};
    if(!L.g.dtype) s=Status(Code::Unsupported,std::string(TYPES)+"; "+where+" holds another sample type");
    else if(sp<0||H5Sget_simple_extent_ndims(sp)!=3||H5Sget_simple_extent_dims(sp,dims,NULL)!=3) s=Status(Code::ReadFailed,where+" is not 3-D");
    else if(pl<0||H5Pget_layout(pl)!=H5D_CHUNKED||H5Pget_chunk(pl,3,chk)!=3) s=Status(Code::ReadFailed,where+" is not chunked");
    if(s.ok()){
        // the pipeline mono_create_dataset gives the finished file: shuffle (uint16 only), then LZ4 (32004)
        // with block size 1 GiB
        const bool shuf=mono_dtype_shuffle(L.g.dtype)>1; const unsigned nf=shuf?2:1;
        bool pipe=H5Pget_nfilters(pl)==(int)nf;
        for(unsigned i=0;pipe&&i<nf;i++){
            unsigned flags=0,cd[8]={0}; size_t ncd=8; char name[64]; unsigned cfg=0;
            H5Z_filter_t id=H5Pget_filter2(pl,i,&flags,&ncd,cd,sizeof name,name,&cfg);
            pipe=(shuf&&i==0)?id==H5Z_FILTER_SHUFFLE:(id==(H5Z_filter_t)32004&&ncd>=1&&cd[0]==(1u<<30));
        }
        if(!pipe) s=Status(Code::Unsupported,where+(shuf?": filters other than shuffle + LZ4":": filters other than LZ4")+" (not a converter .ims?)");
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
// A 1-D uint64 dataset of a group (a Histogram)
bool readU64s(hid_t g,const char* name,std::vector<uint64_t>& v){
    hid_t d=H5Dopen2(g,name,H5P_DEFAULT); if(d<0) return false;
    hid_t sp=H5Dget_space(d); hsize_t n=0;
    bool ok=sp>=0&&H5Sget_simple_extent_ndims(sp)==1&&H5Sget_simple_extent_dims(sp,&n,NULL)==1&&n>0&&n<=(1u<<20);
    if(ok){ v.resize((size_t)n); ok=H5Dread(d,H5T_NATIVE_UINT64,H5S_ALL,H5S_ALL,H5P_DEFAULT,v.data())>=0; }
    if(sp>=0){ H5Sclose(sp); }
    H5Dclose(d); return ok;
}
// A level group's stored histograms (absent: ImarisWriter writes none for an empty level)
Status readPartHist(hid_t g,const std::string& where,PartHist& h){
    h=PartHist();
    if(H5Lexists(g,"Histogram",H5P_DEFAULT)<=0) return Status();
    h.present=true; h.min=imscore::readStrAttr(g,"HistogramMin"); h.max=imscore::readStrAttr(g,"HistogramMax");
    if(!readU64s(g,"Histogram",h.bins)||h.min.empty()||h.max.empty()) return Status(Code::ReadFailed,where+": cannot read its Histogram");
    if(H5Lexists(g,"Histogram1024",H5P_DEFAULT)>0){ h.has1024=true; h.min1024=imscore::readStrAttr(g,"HistogramMin1024"); h.max1024=imscore::readStrAttr(g,"HistogramMax1024");
        if(!readU64s(g,"Histogram1024",h.bins1024)||h.min1024.empty()||h.max1024.empty()) return Status(Code::ReadFailed,where+": cannot read its Histogram1024"); }
    return Status();
}
Status scanOpenPart(hid_t f,const std::string& path,uint64_t fileBytes,std::vector<PartLevel>& lv,std::string& colorRange){
    const std::string p0="/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0";
    if(!imscore::h5PathExists(f,p0+"/Data")) return Status(Code::ReadFailed,"part "+path+" has no "+p0+"/Data (not a converter .ims?)");
    if(imscore::h5PathExists(f,"/DataSet/ResolutionLevel 0/TimePoint 1")||imscore::h5PathExists(f,"/DataSet/ResolutionLevel 0/TimePoint 0/Channel 1"))
        return Status(Code::InvalidArgument,"part "+path+" holds more than one timepoint or channel (convert each (timepoint, channel) on its own)");
    for(int r=0;;r++){
        const std::string lp="/DataSet/ResolutionLevel "+std::to_string(r), gp=lp+"/TimePoint 0/Channel 0";
        if(!imscore::h5PathExists(f,lp)) break;
        if(!imscore::h5PathExists(f,gp+"/Data")) return Status(Code::ReadFailed,"part "+path+" has no "+gp+"/Data");
        PartLevel L; L.g.level=(uint32_t)r;
        hid_t g=H5Gopen2(f,gp.c_str(),H5P_DEFAULT);
        if(g<0) return Status(Code::ReadFailed,"part "+path+": cannot open "+gp);
        const char* k[3]={"ImageSizeX","ImageSizeY","ImageSizeZ"}; bool sized=true;
        for(int a=0;a<3;a++) sized=sized&&parseSize(imscore::readStrAttr(g,k[a]),L.g.size[a]);
        hid_t d=sized?H5Dopen2(g,"Data",H5P_DEFAULT):-1;
        Status s=!sized?Status(Code::ReadFailed,"part "+path+": "+gp+" has no valid ImageSizeX/Y/Z")
                :d<0?Status(Code::ReadFailed,"part "+path+": cannot open "+gp+"/Data"):scanData(d,"part "+path+" "+gp+"/Data",L);
        if(s.ok()&&!lv.empty()&&L.g.dtype!=lv[0].g.dtype) s=Status(Code::ReadFailed,"part "+path+": its levels have different sample types");
        if(s.ok()&&L.g.dtype==MONO_F32) s=readPartHist(g,"part "+path+" "+gp,L.hist);
        if(d>=0){ H5Dclose(d); }
        H5Gclose(g);
        if(!s.ok()) return s;
        for(size_t i=0;i<L.chunks.size();i++) if(L.chunks[i].addr>fileBytes||L.chunks[i].size>fileBytes-L.chunks[i].addr)
            return Status(Code::ReadFailed,"part "+path+": level "+u64s(r)+" has a chunk beyond the end of the file (truncated?)");
        lv.push_back(L);
    }
    if(!lv.empty()&&lv[0].g.dtype==MONO_F32){   // the part's ColorRange (float32: what the finisher writes for one timepoint)
        if(imscore::h5PathExists(f,"/DataSetInfo/Channel 0")){ hid_t g=H5Gopen2(f,"/DataSetInfo/Channel 0",H5P_DEFAULT);
            if(g>=0){ colorRange=imscore::readStrAttr(g,"ColorRange"); H5Gclose(g); } }
        float a=0,b=0; char tail=0;
        if(sscanf(colorRange.c_str(),"%f %f%c",&a,&b,&tail)!=2) return Status(Code::ReadFailed,"part "+path+" has no valid /DataSetInfo/Channel 0 ColorRange");
    }
    return Status();
}
Status scanPart(const std::string& path,std::vector<PartLevel>& lv,std::string& colorRange){
    lv.clear(); colorRange.clear();
    struct stat st;
    if(stat(path.c_str(),&st)!=0) return Status(Code::NotFound,"cannot open part "+path+": "+strerror(errno));
    imscore::H5Quiet quiet;   // every failure below has its own message
    hid_t fapl=H5Pcreate(H5P_FILE_ACCESS);
    if(fapl>=0) H5Pset_file_locking(fapl,false,true);
    hid_t f=H5Fopen(path.c_str(),H5F_ACC_RDONLY,fapl>=0?fapl:H5P_DEFAULT);
    if(fapl>=0) H5Pclose(fapl);
    if(f<0) return Status(Code::ReadFailed,"cannot open part "+path+" (not an HDF5 file, or truncated)");
    Status s=scanOpenPart(f,path,(uint64_t)st.st_size,lv,colorRange);
    if(H5Fclose(f)<0&&s.ok()) s=Status(Code::ReadFailed,"cannot close part "+path);
    return s;
}

// ---------------- chunk decoding + exact histograms ----------------
struct LevelInfo { uint32_t chunk[3]; uint64_t size[3]; size_t raw; unsigned dtype; };
LevelInfo levelInfo(const MonoIdxLevel& g){
    LevelInfo li; for(int a=0;a<3;a++){ li.chunk[a]=g.chunk[a]; li.size[a]=g.size[a]; }
    li.dtype=g.dtype; li.raw=(size_t)g.chunk[0]*g.chunk[1]*g.chunk[2]*mono_dtype_bytes(g.dtype); return li;
}
// Whether a sample type has exact histograms (65536 bins, bin i = value i): uint8 and uint16. float32 parts carry
// the histograms ImarisWriter stored.
bool exactHist(unsigned dtype){ return dtype==MONO_U8||dtype==MONO_U16; }
// Decodes one chunk (LZ4 + size checks) and counts its in-image voxels into H (65536 bins; uint8: 0..255). sh:
// scratch of li.raw bytes. The shuffled bytes are read directly: byte 0 of every voxel first, then byte 1 (uint8:
// not shuffled). float32: decoded only (H is not touched).
imscore::Status countChunk(const uint8_t* c,size_t cs,uint32_t cx,uint32_t cy,uint32_t cz,const LevelInfo& li,uint8_t* sh,uint64_t* H){
    imscore::Status s=imscore::decodeChunkShuffled(c,cs,sh,li.raw); if(!s.ok()) return s;
    if(!exactHist(li.dtype)) return s;
    const uint32_t bx=li.chunk[0],by=li.chunk[1],bz=li.chunk[2];
    const uint64_t xv=std::min<uint64_t>(bx,li.size[0]-(uint64_t)cx*bx), yv=std::min<uint64_t>(by,li.size[1]-(uint64_t)cy*by), zv=std::min<uint64_t>(bz,li.size[2]-(uint64_t)cz*bz);
    if(li.dtype==MONO_U8){
        for(uint64_t z=0;z<zv;z++) for(uint64_t y=0;y<yv;y++){ const uint8_t* row=sh+(size_t)bx*(y+(size_t)by*z); for(uint64_t x=0;x<xv;x++) H[row[x]]++; }
        return s;
    }
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
    const imscore::bpSize budget=(1024*1024)/mono_dtype_bytes(geo[0].dtype);   // bpMultiresolutionImsImage::GetOptimalBlockSizes
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
// Decodes the n placed chunks at(0..n-1) of one level of one dataset (fd: the file being built) into its dense
// X*Y*Z volume (the level's samples, as bytes).
Status decodeLevel(int fd,const MonoIdxLevel& from,size_t n,const MonoChunkAt& at,int T,std::vector<uint8_t>& vol){
    const imscore::bpSize size[3]={from.size[0],from.size[1],from.size[2]}, b[3]={from.chunk[0],from.chunk[1],from.chunk[2]};
    const size_t eb=mono_dtype_bytes(from.dtype), sb=mono_dtype_shuffle(from.dtype);
    vol.assign((size_t)(size[0]*size[1]*size[2])*eb,0);
    const size_t rawFrom=(size_t)b[0]*b[1]*b[2]*eb;
    std::atomic<size_t> next(0); FirstError fe;
    std::string x=runThreads(T,[&](int){
        std::vector<uint8_t> cb,vox;
        for(;;){ size_t i=next.fetch_add(1); if(i>=n||fe.set) break; MonoChunk ch; at(i,ch);
            cb.resize(ch.size); int re=preadAll(fd,cb.data(),ch.size,ch.offset);
            imscore::Status ds=re?imscore::Status::fail(strerror(re)):imscore::decodeChunk(cb.data(),ch.size,vox,rawFrom,sb);
            if(!ds.ok()){ fe.put("level "+u64s(from.level)+" chunk ("+u64s(ch.cx)+","+u64s(ch.cy)+","+u64s(ch.cz)+") at "+u64s(ch.offset)+": "+ds.message); break; }
            imscore::chunkToVolumeBytes(vox.data(),b,ch.cx,ch.cy,ch.cz,size,eb,vol.data());
        } });
    if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x);
    return Status();
}
// Re-chunks level `from` of one part to `to` (call with the library mutex held): decodes the placed chunks
// (fd: the file being built), cuts the level into to's chunks and encodes them with the project codec (the
// bytes ImarisWriter writes), creates <g>/Data with to's geometry and writes them with H5Dwrite_chunk; HDF5
// puts them after the raw region (the placement driver passes un-armed writes through). out: the chunks
// written, with their file addresses.
Status rechunkLevel(int fd,hid_t g,const MonoIdxLevel& from,const MonoIdxLevel& to,const MonoChunk* chunks,size_t n,int T,
                    std::vector<MonoChunk>& out){
    out.clear();
    const uint64_t X=from.size[0],Y=from.size[1],Z=from.size[2]; const size_t eb=mono_dtype_bytes(from.dtype), sb=mono_dtype_shuffle(from.dtype);
    std::vector<uint8_t> vol;
    Status ls=decodeLevel(fd,from,n,[chunks](size_t k,MonoChunk& ch){ ch=chunks[k]; },T,vol); if(!ls.ok()) return ls;
    std::string x;
    const uint64_t bx=to.chunk[0],by=to.chunk[1],bz=to.chunk[2],gx=to.grid[0],gy=to.grid[1];
    const size_t nn=(size_t)to.nChunks;
    std::vector<std::vector<uint8_t> > enc(nn);
    std::atomic<size_t> next2(0); FirstError fe2;
    x=runThreads(T,[&](int){
        std::vector<uint8_t> blk((size_t)(bx*by*bz)*eb);
        for(;;){ size_t k=next2.fetch_add(1); if(k>=nn||fe2.set) break;   // k = cx + gx*(cy + gy*cz): (z, y, x) order
            const uint64_t x0=(k%gx)*bx,y0=((k/gx)%gy)*by,z0=(k/(gx*gy))*bz;
            const uint64_t xv=std::min(bx,X-x0),yv=std::min(by,Y-y0),zv=std::min(bz,Z-z0);
            std::fill(blk.begin(),blk.end(),(uint8_t)0);
            for(uint64_t z=0;z<zv;z++) for(uint64_t y=0;y<yv;y++) memcpy(&blk[(size_t)((y+z*by)*bx)*eb],&vol[(size_t)(x0+(y0+y)*X+(z0+z)*X*Y)*eb],(size_t)xv*eb);
            if(!imscore::encodeChunk(blk.data(),blk.size(),enc[k],sb)){ fe2.put("LZ4 could not compress a chunk"); break; }
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
// ---------------- float32 parts: the histograms ImarisWriter stored, copied ----------------
// ImarisWriter's float32 histograms are adaptive (bpHistogramBuilderAdaptive: 1000 bins whose range grows with the
// values added, so the result depends on the order the values come in), not exact counts that could be summed or
// rebuilt. A float32 part's sidecar mono/P_C<c>_T<t>.fhist therefore carries what the part's conversion stored: every
// level's Histogram / Histogram1024 with their HistogramMin/Max texts (written into the file unchanged) and the
// part's ColorRange (written unchanged when the file has one timepoint).
std::string floatHistText(const std::vector<PartLevel>& lv,const std::string& colorRange){
    nlohmann::ordered_json j; j["colorRange"]=colorRange; nlohmann::ordered_json a=nlohmann::ordered_json::array();
    for(const PartLevel& L:lv){ const PartHist& h=L.hist; nlohmann::ordered_json e;
        e["min"]=h.min; e["max"]=h.max; e["bins"]=h.bins;
        if(h.has1024){ e["min1024"]=h.min1024; e["max1024"]=h.max1024; e["bins1024"]=h.bins1024; }
        a.push_back(e); }
    j["levels"]=a; return j.dump()+"\n";
}
// A level's histograms must count every voxel of the level once.
bool countsLevel(const std::vector<uint64_t>& b,const MonoIdxLevel& g){ uint64_t n=0; for(uint64_t v:b) n+=v; return !b.empty()&&n==voxelsOf(g); }
Status readFloatHist(const std::string& p,const std::vector<MonoIdxLevel>& geo,std::vector<PartHist>& h,std::string& colorRange){
    std::vector<uint8_t> b; std::string e; h.clear();
    if(!exists(p)) return Status(Code::NotFound,"missing histograms "+p);
    if(!mono_read_file(p,b,&e)) return Status(Code::ReadFailed,e);
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        colorRange=j.at("colorRange").get<std::string>();
        for(auto& x:j.at("levels")){ PartHist v; v.present=true;
            v.min=x.at("min").get<std::string>(); v.max=x.at("max").get<std::string>(); v.bins=x.at("bins").get<std::vector<uint64_t> >();
            if((v.has1024=x.contains("bins1024"))){ v.min1024=x.at("min1024").get<std::string>(); v.max1024=x.at("max1024").get<std::string>(); v.bins1024=x.at("bins1024").get<std::vector<uint64_t> >(); }
            h.push_back(v); }
    }catch(const std::exception& x){ return Status(Code::ReadFailed,"invalid "+p+": "+x.what()); }
    if(h.size()!=geo.size()) return Status(Code::ReadFailed,p+" has "+u64s(h.size())+" levels, the layout "+u64s(geo.size()));
    for(size_t r=0;r<h.size();r++) if(!countsLevel(h[r].bins,geo[r])||(h[r].has1024&&!countsLevel(h[r].bins1024,geo[r])))
        return Status(Code::ReadFailed,p+": the histograms of level "+u64s(r)+" do not count its "+u64s(voxelsOf(geo[r]))+" voxels");
    return Status();
}
// Writes a copied histogram as bpWriterHDF5::WriteHistogram does: Min, Max, Histogram, then the 1024 ones.
bool writeCopiedHist(hid_t g,const PartHist& h){
    bool ok=imscore::setStrAttr(g,"HistogramMin",h.min); ok&=imscore::setStrAttr(g,"HistogramMax",h.max); ok&=imscore::writeU64(g,"Histogram",h.bins);
    if(h.has1024){ ok&=imscore::setStrAttr(g,"HistogramMin1024",h.min1024); ok&=imscore::setStrAttr(g,"HistogramMax1024",h.max1024); ok&=imscore::writeU64(g,"Histogram1024",h.bins1024); }
    return ok;
}
// The full histogram of a copied level (GetHistogram: Histogram1024 when written, else the 256-bin one), as the
// ColorRange's input: its range from the texts, rounded to 3 decimals (see finishParts).
imscore::ColorHist fullHist(const PartHist& h){
    imscore::ColorHist c; c.bins=h.has1024?h.bins1024:h.bins;
    c.hmin=strtof((h.has1024?h.min1024:h.min).c_str(),NULL); c.hmax=strtof((h.has1024?h.max1024:h.max).c_str(),NULL);
    return c;
}
// Whether every histogram sidecar of part (t, c) is there and whole (levels: its sidecar's; float32: the .fhist).
bool partHistsThere(const std::string& wd,int t,int c,const std::vector<MonoIdxLevel>& levels,std::string* why){
    if(levels.empty()) return false;
    if(levels[0].dtype==MONO_F32){ std::vector<PartHist> h; std::string cr; Status s=readFloatHist(floatHistPath(wd,t,c),levels,h,cr);
        if(!s.ok()&&why) *why=s.message;
        return s.ok(); }
    for(size_t r=0;r<levels.size();r++){ struct stat st; const std::string h=histPath(wd,(int)r,t,c);
        if(stat(h.c_str(),&st)!=0||(uint64_t)st.st_size!=imscore::HIST_BYTES){ if(why) *why="missing or damaged histogram "+h; return false; } }
    return true;
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
        imsmeta::Metadata md;
        { std::string me=resolveMonoMetadata(o,o.timepoints,o.channels,md); if(!me.empty()) return Status(Code::InvalidArgument,me); }
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
        if(exists(monoDir(wd)+"/tiles"))
            return Status(Code::InvalidArgument,"the tile workers of "+wd+" write its single file: it cannot also take parts (use a new workdir)");

        // 1. scan the part: geometry, every chunk's address and size
        std::vector<PartLevel> lv; std::string colorRange;
        { std::lock_guard<std::mutex> lock(detail::libraryMutex()); s=scanPart(o.partFile,lv,colorRange); }
        if(!s.ok()) return s;
        const unsigned dtype=lv[0].g.dtype;
        if(dtype==MONO_F32) for(size_t r=0;r<lv.size();r++)
            if(!lv[r].hist.present||!countsLevel(lv[r].hist.bins,lv[r].g)||(lv[r].hist.has1024&&!countsLevel(lv[r].hist.bins1024,lv[r].g)))
                return Status(Code::ReadFailed,"part "+o.partFile+": level "+u64s(r)+" has no histogram of all its voxels (an unfinished conversion? convert it again)");
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
        if(dtype==MONO_F32){ const std::string ft=floatHistText(lv,colorRange);
            if(!mono_write_atomic(floatHistPath(wd,t,c),ft.data(),ft.size(),&e)) return Status(Code::WriteFailed,e); }
        else for(size_t r=0;r<lv.size();r++){
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

#ifndef _WIN32
namespace {
// ---------------- tile workdirs (setup + worker, mono store: parallel.h) ----------------
// A workdir whose single file the tile workers write: a tile plan's (src/cluster.cpp: the manifest's kind decides),
// else setup's mono store once a tile worker has marked it (mono/tiles). A setup workdir without the mark takes parts
// (monoplace) until a worker marks it.
bool tilesWorkdir(const std::string& wd,const MonoManifest& m){
    if(!m.kind.empty()) return m.kind=="tiles";
    return m.store=="mono"&&exists(monoDir(wd)+"/tiles"); }
// Nothing decides yet whether a setup workdir (mono store) takes parts or tiles: nothing placed, no mark.
bool undecidedWorkdir(const std::string& wd,const MonoManifest& m){
    return m.kind.empty()&&m.store=="mono"&&!exists(monoDir(wd)+"/tiles")&&!exists(monoDir(wd)+"/layout.json"); }
struct TileId { int r, t, c; long tile; };
// Every tile of the manifest, in (level, timepoint, channel, tile) order.
std::vector<TileId> allTiles(const imscore::Manifest& im){
    std::vector<TileId> v;
    for(int r=0;r<(int)im.lev.size();r++) for(int t=0;t<im.nT;t++) for(int c=0;c<im.nC;c++) for(long i=0;i<imscore::tilesPerCT(im.lev[(size_t)r]);i++){ TileId x={r,t,c,i}; v.push_back(x); }
    return v;
}
Status readTileManifest(const std::string& wd,imscore::Manifest& im){
    imscore::Status ms=imscore::readManifest(wd,im); if(!ms.ok()) return Status(Code::InvalidArgument,ms.message);
    if(im.dtype!="uint8"&&im.dtype!="uint16") return Status(Code::Unsupported,std::string(TYPES)+"; "+wd+" holds "+im.dtype+" data");
    return Status();
}
}  // namespace
#endif

Status monoStatus(const std::string& workdir,MonoProgress& p) noexcept{
    try{
        p=MonoProgress();
#ifdef _WIN32
        (void)workdir; return Status(Code::Unsupported,NOSUP);
#else
        const std::string wd=trimSlash(workdir);
        MonoManifest m; Status s=readMonoManifest(wd,m); if(!s.ok()) return s;
        p.timepoints=m.nT; p.channels=m.nC;
        if(tilesWorkdir(wd,m)){   // many small files: checked from a few threads
            imscore::Manifest im; s=readTileManifest(wd,im); if(!s.ok()) return s;
            p.tiles=true; p.levels=(int)im.lev.size();
            const std::vector<TileId> all=allTiles(im); std::vector<char> have(all.size(),0); std::atomic<size_t> next(0);
            runThreads(16,[&](int){ for(;;){ size_t i=next.fetch_add(1); if(i>=all.size()) break; const TileId& x=all[i];
                have[i]=exists(imscore::tileIdxName(wd+"/chunks",x.r,x.c,x.t,x.tile)); } });
            for(size_t i=0;i<all.size();i++) (have[i]?p.placed:p.missing).push_back(imscore::tileOwner(all[i].r,all[i].c,all[i].t,all[i].tile));
        }
        else for(int t=0;t<m.nT;t++) for(int c=0;c<m.nC;c++) (exists(idxPath(wd,t,c))?p.placed:p.missing).push_back(ownerOf(t,c));
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
// ---------------- the finisher's steps shared by parts and tiles ----------------
// The thumbnail's voxels (imsthumb.h): level thumbnailLevel of timepoint 0, every channel, decoded from the placed
// chunks (chunks(c, r, n, at): those of channel c at that level; a re-chunked level's placed chunks stay valid in the
// raw region). Read before the HDF5 work, without the library mutex.
typedef std::function<void(int c,size_t r,size_t& n,MonoChunkAt& at)> PlacedFn;
Status readThumbnailVolumes(const std::string& partial,const std::vector<MonoIdxLevel>& geo,const PlacedFn& chunks,
                            int nC,int T,std::vector<std::vector<uint8_t> >& vol){
    std::vector<imscore::Vec3> sizes;
    for(const MonoIdxLevel& g:geo){ imscore::Vec3 v={{g.size[0],g.size[1],g.size[2]}}; sizes.push_back(v); }
    const size_t r=imsthumb::thumbnailLevel(sizes);
    FdGuard fd(open(partial.c_str(),O_RDONLY));
    if(fd.fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
    vol.assign((size_t)nC,std::vector<uint8_t>());
    for(int c=0;c<nC;c++){ size_t n=0; MonoChunkAt at; chunks(c,r,n,at);
        Status s=decodeLevel(fd.fd,geo[r],n,at,T,vol[(size_t)c]);
        if(!s.ok()) return Status(s.code,"thumbnail of timepoint 0, channel "+std::to_string(c)+": "+s.message); }
    return Status();
}
// The volumes' bytes as samples of type T
template<class T> std::vector<std::vector<T> > typedVolumes(const std::vector<std::vector<uint8_t> >& vol){
    std::vector<std::vector<T> > v(vol.size());
    for(size_t c=0;c<vol.size();c++){ v[c].resize(vol[c].size()/sizeof(T)); if(!vol[c].empty()) memcpy(v[c].data(),vol[c].data(),v[c].size()*sizeof(T)); }
    return v;
}
// /Thumbnail, where ImarisWriter writes it: after /DataSetInfo, coloured as its channels.
Status writeThumbnail(hid_t f,const std::vector<MonoIdxLevel>& geo,const imscore::Manifest& im,
                      const std::vector<std::vector<uint8_t> >& vol,const std::vector<imscore::ChannelDisplay>& display){
    std::vector<imscore::Vec3> sizes;
    for(const MonoIdxLevel& g:geo){ imscore::Vec3 v={{g.size[0],g.size[1],g.size[2]}}; sizes.push_back(v); }
    float ext[3]; imscore::imsExtentMax(im.X,im.Y,im.Z,im.vox,ext);
    const unsigned dt=geo[0].dtype;
    imscore::Status s=dt==MONO_U8?imsthumb::writeThumbnail(f,sizes,vol,display,ext)
                     :dt==MONO_F32?imsthumb::writeThumbnail(f,sizes,typedVolumes<float>(vol),display,ext)
                     :imsthumb::writeThumbnail(f,sizes,typedVolumes<uint16_t>(vol),display,ext);
    return s.ok()?Status():Status(Code::WriteFailed,s.message);
}
// Every writer's sidecar must name a range that the claims journal gives its owner.
Status checkClaimed(const std::vector<MonoClaim>& recs,const std::vector<MonoClaim>& want){
    std::multimap<std::string,const MonoClaim*> byOwner;
    for(size_t i=0;i<recs.size();i++) if(!recs[i].seal) byOwner.insert(std::make_pair(recs[i].owner,&recs[i]));
    for(const MonoClaim& w:want){ bool found=false; auto range=byOwner.equal_range(w.owner);
        for(auto it=range.first;!found&&it!=range.second;++it) found=it->second->offset==w.offset&&it->second->size==w.size;
        if(!found) return Status(Code::ReadFailed,"the sidecar of "+w.owner+" names bytes ["+u64s(w.offset)+", "+u64s(w.offset+w.size)
                                 +") that the claims journal does not give it (a sidecar of another run? write it again)"); }
    return Status();
}
// Cheap check that every blob was written: the LZ4 headers of its first and last chunk (raw = their chunks' bytes).
struct BlobEnds { std::string owner; MonoChunk a, b; uint64_t rawA, rawB; };
Status checkBlobEnds(const std::string& partial,const std::vector<BlobEnds>& v,int threads){
    FdGuard fd(open(partial.c_str(),O_RDONLY));
    if(fd.fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
    std::atomic<size_t> next(0); FirstError fe;
    std::string x=runThreads(std::max(1,std::min(threads,(int)std::min<size_t>(v.size(),64))),[&](int){
        for(;;){ size_t i=next.fetch_add(1); if(i>=v.size()||fe.set) break; const BlobEnds& e=v[i];
            for(int end=0;end<2;end++){ const MonoChunk& ch=end?e.b:e.a; uint8_t h[16];
                if(preadAll(fd.fd,h,16,ch.offset)!=0||imscore::rbe(h,8)!=(end?e.rawB:e.rawA)||imscore::rbe(h+12,4)+16!=ch.size){
                    fe.put("the chunks of "+e.owner+" are not in "+partial+" at "+u64s(ch.offset)+" (its blob was not written: write it again)"); break; } } } });
    if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x);
    return Status();
}

// What the finished file holds, whichever writers placed the chunks: nT x nC datasets per level (dataset k = t*nC + c)
// over chunks already in the file, and the metadata of a single-node conversion of all of them.
struct FileSpec {
    std::string wd, partial;
    int nT=0, nC=0, threads=1;
    std::vector<MonoIdxLevel> geo;   // each level as the file stores it
    imscore::Manifest im;            // the metadata's source: size, voxel size, channels, timepoints
    std::function<void(int k,size_t r,size_t& n,MonoChunkAt& at)> chunks;            // dataset (k, r): its chunks, (z, y, x) order
    std::function<Status(int r,int t,int c,std::vector<uint64_t>& bins)> hist;        // level r of (t, c): exact 65536 bins
    std::function<Status(int r,int t,int c,hid_t g)> writeHist;                       // set: writes level r of (t, c)'s histograms
                                                                                      // itself (float32), and hist is not used
    imscore::ColorRangeFn colorRange;                                                 // set: the ColorRange (float32)
    std::function<Status(int k,size_t r,hid_t g,MonoVfd& vfd)> data;                  // writes <g>/Data (library mutex held)
    uint64_t armedWant=0;            // chunks indexed through the placement driver
};
// Step 3: the HDF5 structure over the placed chunks, in ImarisWriter's order (imscore.h): root attributes and
// /DataSet, every (t, c) and its levels, /DataSetInfo, then the thumbnail.
Status buildFile(const FileSpec& s,MonoVfd& vfd,const std::vector<std::vector<uint8_t> >& thumbVol,MonoFinishResult& res){
    std::lock_guard<std::mutex> lock(detail::libraryMutex());
    const double ti=seconds(); std::string e;
    hid_t fapl=mono_vfd_fapl(vfd,&e);
    if(fapl<0) return Status(Code::WriteFailed,e);
    hid_t f=H5Fcreate(s.partial.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl);
    std::string why; mono_vfd_ok(vfd,&why);
    if(f<0) return Status(Code::WriteFailed,"cannot build the HDF5 structure in "+s.partial+(why.empty()?std::string(" (HDF5 error above)"):": "+why));
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    std::vector<int> chs,tps; for(int c=0;c<s.nC;c++) chs.push_back(c); for(int t=0;t<s.nT;t++) tps.push_back(t);
    imscore::Status ws=imscore::writeImsRoot(f,lcpl);
    Status bs=ws.ok()?Status():Status(Code::WriteFailed,ws.message);
    std::vector<uint64_t> bins;
    for(int t=0;bs.ok()&&t<s.nT;t++) for(int c=0;bs.ok()&&c<s.nC;c++) for(size_t r=0;bs.ok()&&r<s.geo.size();r++){
        const std::string gp="DataSet/ResolutionLevel "+u64s(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
        hid_t g=H5Gcreate2(f,gp.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
        if(g<0){ bs=Status(Code::WriteFailed,"cannot create "+gp+" (HDF5 error above)"); break; }
        bool ok=imscore::setStrAttr(g,"ImageSizeX",u64s(s.geo[r].size[0]))&&imscore::setStrAttr(g,"ImageSizeY",u64s(s.geo[r].size[1]))&&imscore::setStrAttr(g,"ImageSizeZ",u64s(s.geo[r].size[2]));
        if(ok){ Status ds=s.data(t*s.nC+c,r,g,vfd); if(!ds.ok()) bs=Status(ds.code,gp+": "+ds.message); }
        if(bs.ok()&&ok&&s.writeHist){ Status hs=s.writeHist((int)r,t,c,g); if(!hs.ok()) bs=hs; }
        else if(bs.ok()&&ok){ Status hs=s.hist((int)r,t,c,bins);
            if(!hs.ok()) bs=hs; else ok=imscore::writeHistogramFromBins(g,bins,s.geo[r].dtype==MONO_U8); }
        if(bs.ok()&&!ok) bs=Status(Code::WriteFailed,"cannot write "+gp+" (HDF5 error above)");
        H5Gclose(g);
    }
    std::vector<imscore::ChannelDisplay> display;
    auto colorHist=[&](int r,int c,int t,std::vector<uint64_t>& b)->imscore::Status{
        Status hs=s.hist(r,t,c,b); return hs.ok()?imscore::Status():imscore::Status::fail(hs.message); };
    if(bs.ok()){ ws=imscore::writeImsDataSetInfo(f,s.im,lcpl,chs,tps,colorHist,&display,s.colorRange?&s.colorRange:NULL); if(!ws.ok()) bs=Status(Code::WriteFailed,ws.message); }
    if(bs.ok()) bs=writeThumbnail(f,s.geo,s.im,thumbVol,display);
    H5Pclose(lcpl);
    if(H5Fclose(f)<0&&bs.ok()) bs=Status(Code::WriteFailed,"cannot close "+s.partial+" (HDF5 error above)");
    if(bs.ok()&&!mono_vfd_ok(vfd,&why)) bs=Status(Code::WriteFailed,"the placement driver refused a write: "+why);
    const MonoVfdStats& st=mono_vfd_stats(vfd);
    if(bs.ok()&&(st.armed!=s.armedWant||st.placed!=s.armedWant||st.discarded!=s.armedWant))
        bs=Status(Code::Internal,"indexed "+u64s(st.placed)+" of "+u64s(s.armedWant)+" placed chunks");
    if(!bs.ok()) return Status(bs.code,bs.message+" (the placed chunks are untouched: monofinish can run again)");
    res.indexSeconds=seconds()-ti;
    return Status();
}
// Step 4: reopened with the stock driver, every chunk index must equal the chunks it was built from.
Status checkFile(const FileSpec& s,const MonoVfd& vfd,MonoFinishResult& res){
    std::lock_guard<std::mutex> lock(detail::libraryMutex());
    const double tc=seconds();
    hid_t fapl=H5Pcreate(H5P_FILE_ACCESS); H5Pset_file_locking(fapl,false,true);
    hid_t f=H5Fopen(s.partial.c_str(),H5F_ACC_RDONLY,fapl); H5Pclose(fapl);
    if(f<0) return Status(Code::ReadFailed,"the finished "+s.partial+" does not open with the stock HDF5 driver (HDF5 error above)");
    Status cs;
    for(int t=0;cs.ok()&&t<s.nT;t++) for(int c=0;cs.ok()&&c<s.nC;c++) for(size_t r=0;cs.ok()&&r<s.geo.size();r++){
        const std::string dp="DataSet/ResolutionLevel "+u64s(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c)+"/Data";
        hid_t d=H5Dopen2(f,dp.c_str(),H5P_DEFAULT); uint64_t bad=0; std::string ve; size_t n=0; MonoChunkAt at;
        s.chunks(t*s.nC+c,r,n,at);
        if(d<0) cs=Status(Code::ReadFailed,"cannot open "+dp+" in the finished file");
        else if(!mono_verify_dataset(d,mono_idx_geom(s.geo[r]),n,at,&bad,&ve)) cs=Status(Code::ReadFailed,dp+" in the finished file: "+ve);
        if(d>=0){ H5Dclose(d); }
    }
    if(H5Fclose(f)<0&&cs.ok()) cs=Status(Code::ReadFailed,"cannot close "+s.partial);
    if(!cs.ok()) return cs;
    struct stat st;
    if(stat(s.partial.c_str(),&st)!=0||(uint64_t)st.st_size!=mono_vfd_stats(vfd).eoa)
        return Status(Code::Internal,s.partial+" does not end where HDF5's allocation ends");
    res.fileBytes=(uint64_t)st.st_size;
    res.checkSeconds=seconds()-tc;
    return Status();
}
// Step 5 (--verify): decode every chunk the file's index holds; each level's histogram must equal the writer's
// (float32: decoded only).
Status verifyFile(const FileSpec& s,MonoFinishResult& res){
    const double tv=seconds(); const int T=s.threads;
    FdGuard fd(open(s.partial.c_str(),O_RDONLY));
    if(fd.fd<0) return Status(Code::ReadFailed,"cannot open "+s.partial+": "+strerror(errno));
    std::vector<std::vector<uint64_t> > hist((size_t)T); std::vector<std::vector<uint8_t> > cbuf((size_t)T), sh((size_t)T);
    for(int k=0;k<s.nT*s.nC;k++) for(size_t r=0;r<s.geo.size();r++){
        const int t=k/s.nC, c=k%s.nC; const LevelInfo li=levelInfo(s.geo[r]);
        size_t n=0; MonoChunkAt at; s.chunks(k,r,n,at);
        for(int w=0;w<T;w++) hist[(size_t)w].assign(65536,0);
        std::atomic<size_t> next(0); FirstError fe;
        std::string x=runThreads(T,[&](int w){
            if(sh[(size_t)w].size()<li.raw) sh[(size_t)w].resize(li.raw);
            for(;;){ size_t i=next.fetch_add(1); if(i>=n||fe.set) break; MonoChunk ch; at(i,ch);
                std::vector<uint8_t>& cb=cbuf[(size_t)w]; cb.resize(ch.size); std::string why;
                int re=preadAll(fd.fd,cb.data(),ch.size,ch.offset);
                if(re) why=strerror(re);
                else { imscore::Status ds=countChunk(cb.data(),ch.size,ch.cx,ch.cy,ch.cz,li,sh[(size_t)w].data(),hist[(size_t)w].data()); if(!ds.ok()) why=ds.message; }
                if(!why.empty()){ fe.put("t="+std::to_string(t)+" c="+std::to_string(c)+" level "+u64s(r)+" chunk ("+u64s(ch.cx)+","+u64s(ch.cy)+","+u64s(ch.cz)+") at "+u64s(ch.offset)+": "+why); break; } } });
        if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,"--verify: "+(!fe.msg.empty()?fe.msg:x));
        if(!exactHist(li.dtype)) continue;
        std::vector<uint64_t> want; Status hs=s.hist((int)r,t,c,want); if(!hs.ok()) return Status(hs.code,"--verify: "+hs.message);
        for(size_t i=0;i<65536;i++){ uint64_t m=0; for(int w=0;w<T;w++) m+=hist[(size_t)w][i];
            if(m!=want[i]) return Status(Code::ReadFailed,"--verify: t="+std::to_string(t)+" c="+std::to_string(c)+" level "+u64s(r)
                                         +": the voxels in the file differ from the writer's histogram (value "+u64s(i)+")"); }
    }
    res.verifySeconds=seconds()-tv;
    return Status();
}
// Steps 3 to 6: build, check, verify, record the output and rename the file to it.
Status finishFile(const FileSpec& s,uint64_t rawEnd,bool verify,const std::vector<std::vector<uint8_t> >& thumbVol,MonoFinishResult& res,double t0){
    MonoVfd vfd(rawEnd);
    Status st=buildFile(s,vfd,thumbVol,res); if(!st.ok()) return st;
    st=checkFile(s,vfd,res); if(!st.ok()) return st;
    if(verify){ st=verifyFile(s,res); if(!st.ok()) return st; }
    std::string e; const std::string& out=res.outputFile;
    if(!mono_write_atomic(finishedPath(s.wd),out.data(),out.size(),&e)) return Status(Code::WriteFailed,e);
    if(rename(s.partial.c_str(),out.c_str())!=0){ int re=errno; unlink(finishedPath(s.wd).c_str());
        return Status(Code::WriteFailed,"cannot rename "+s.partial+" to "+out+": "+strerror(re)
                      +(re==EXDEV?" (the output must be on the workdir's filesystem)":"")+"; the finished file stays at "+s.partial); }
    res.totalSeconds=seconds()-t0;
    return Status();
}

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
std::string listOf(const std::vector<std::string>& v){
    std::string list; for(size_t i=0;i<v.size()&&i<20;i++) list+=(i?", ":"")+v[i];
    return v.size()>20?list+", ...":list;
}

// ---------------- parts: monoplace's sidecars (one per (t, c)) ----------------
Status finishParts(const MonoFinishOptions& o,const MonoManifest& m,const std::string& wd,const std::string& partial,int T,double t0,MonoFinishResult& res){
    const int nT=m.nT, nC=m.nC; std::string e;
    std::vector<MonoIdxLevel> geo; Status s=readLayout(wd,geo); if(!s.ok()) return s;
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
            if(!partHistsThere(wd,t,c,geo,&why)&&why.empty()) why="missing histograms of "+ownerOf(t,c);
            if(!why.empty()){ fe.put(why+" (run monoplace for t="+std::to_string(t)+", c="+std::to_string(c)+" again)"); break; }
            have[(size_t)k]=1;
        } });
      if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x); }
    for(int k=0;k<nP;k++) if(!have[(size_t)k]) res.missing.push_back(ownerOf(k/nC,k%nC));
    if(!res.missing.empty())
        return Status(Code::NotFound,u64s(res.missing.size())+" of "+u64s(nP)+" parts are not placed yet: "+listOf(res.missing)+" (run monoplace for them)");

    // 2. each sidecar's range must be a claim of its owner in the journal; then seal: the raw region ends there
    MonoClaims claims;
    if(!claims.open(monoDir(wd),"",&e)) return Status(Code::ReadFailed,e);
    std::vector<MonoClaim> recs, want; bool sealed=false;
    if(!claims.list(recs,&sealed,&e)) return Status(Code::ReadFailed,e);
    for(int k=0;k<nP;k++){ MonoClaim w; w.owner=idx[(size_t)k].owner; w.offset=idx[(size_t)k].claimOffset; w.size=idx[(size_t)k].claimSize; want.push_back(w); }
    s=checkClaimed(recs,want); if(!s.ok()) return s;
    MonoClaim seal;
    if(!claims.seal(seal,&e)) return Status(Code::WriteFailed,e);
    const uint64_t rawEnd=seal.offset; res.rawEnd=rawEnd;
    for(int k=0;k<nP;k++) res.chunks+=idx[(size_t)k].chunks.size();
    std::vector<std::vector<size_t> > first((size_t)nP);
    for(int k=0;k<nP;k++) first[(size_t)k]=levelRanges(idx[(size_t)k]);
    std::vector<BlobEnds> ends;
    for(int k=0;k<nP;k++){ const MonoIdx& ix=idx[(size_t)k]; BlobEnds b; b.owner=ix.owner; b.a=ix.chunks.front(); b.b=ix.chunks.back();
        b.rawA=levelInfo(geo[b.a.level]).raw; b.rawB=levelInfo(geo[b.b.level]).raw; ends.push_back(b); }
    s=checkBlobEnds(partial,ends,T); if(!s.ok()) return s;
    auto placed=[&](int k,size_t r,size_t& n,MonoChunkAt& at){ const MonoChunk* base=idx[(size_t)k].chunks.data()+first[(size_t)k][r];
        n=first[(size_t)k][r+1]-first[(size_t)k][r]; at=[base](size_t i,MonoChunk& ch){ ch=base[i]; }; };
    std::vector<std::vector<uint8_t> > thumbVol;
    s=readThumbnailVolumes(partial,geo,[&](int c,size_t r,size_t& n,MonoChunkAt& at){ placed(c,r,n,at); },nC,T,thumbVol); if(!s.ok()) return s;   // timepoint 0: k = c
    res.readSeconds=seconds()-t0;

    // levels whose chunks a single-node conversion of all nT timepoints lays out differently: re-chunked
    FileSpec fs; fs.wd=wd; fs.partial=partial; fs.nT=nT; fs.nC=nC; fs.threads=T;
    std::vector<char> rechunk;
    if(!singleNodeLayout(geo,nT,fs.geo,rechunk)) res.note="the parts' chunks are not the converter's 1-timepoint layout: no level was re-chunked";
    for(size_t r=0;r<geo.size();r++){ if(rechunk[r]) res.rechunkedLevels.push_back((int)r); else fs.armedWant+=(uint64_t)nP*geo[r].nChunks; }
    const unsigned dtype=geo[0].dtype;
    fs.im.nC=nC; fs.im.nT=nT; fs.im.X=geo[0].size[0]; fs.im.Y=geo[0].size[1]; fs.im.Z=geo[0].size[2];
    fs.im.elem=mono_dtype_bytes(dtype); fs.im.dtype=dtypeName(dtype);
    for(int a=0;a<3;a++) fs.im.vox[a]=m.vox[a];
    fs.im.meta=m.meta;
    std::vector<std::vector<std::vector<MonoChunk> > > newChunks((size_t)nP,std::vector<std::vector<MonoChunk> >(geo.size()));
    fs.chunks=[&](int k,size_t r,size_t& n,MonoChunkAt& at){   // the chunks the file's index holds
        if(rechunk[r]){ const MonoChunk* base=newChunks[(size_t)k][r].data(); n=newChunks[(size_t)k][r].size(); at=[base](size_t i,MonoChunk& ch){ ch=base[i]; }; }
        else placed(k,r,n,at); };
    fs.hist=[&](int r,int t,int c,std::vector<uint64_t>& bins){ return readHist(histPath(wd,r,t,c),geo[(size_t)r],bins); };
    // float32: every part's stored histograms are written as they are, and the ColorRange comes from them: with one
    // timepoint it is the part's own (the same text a single-node conversion writes); with more, ImarisWriter's
    // multi-timepoint channel histogram (channelColorHistMulti) of the parts' level-0 histograms + adjustColorRange.
    // The histograms' ranges are known only as the texts ImarisWriter wrote (3 decimals), so this ColorRange can
    // differ from a single-node conversion's: see monoFinish in mono.h for the bound.
    std::vector<std::vector<PartHist> > fh; std::vector<std::string> fcr;
    if(dtype==MONO_F32){ fh.resize((size_t)nP); fcr.resize((size_t)nP);
        for(int k=0;k<nP;k++){ s=readFloatHist(floatHistPath(wd,k/nC,k%nC),geo,fh[(size_t)k],fcr[(size_t)k]); if(!s.ok()) return s; }
        fs.writeHist=[&](int r,int t,int c,hid_t g)->Status{
            return writeCopiedHist(g,fh[(size_t)(t*nC+c)][(size_t)r])?Status():Status(Code::WriteFailed,"cannot write the histograms (HDF5 error above)"); };
        fs.colorRange=[&](int c,const std::vector<int>& tps,imscore::ChannelDisplay& d,std::string& text)->imscore::Status{
            if(tps.size()==1){ text=fcr[(size_t)(tps[0]*nC+c)]; sscanf(text.c_str(),"%f %f",&d.rangeMin,&d.rangeMax); return imscore::Status(); }
            std::vector<imscore::ColorHist> perTp; for(int t:tps) perTp.push_back(fullHist(fh[(size_t)(t*nC+c)][0]));
            imscore::adjustColorRange(imscore::channelColorHistMulti(perTp),d.rangeMin,d.rangeMax);
            text=imscore::fstr(d.rangeMin,3)+" "+imscore::fstr(d.rangeMax,3); return imscore::Status(); };
    }
    FdGuard rfd(res.rechunkedLevels.empty()?-1:open(partial.c_str(),O_RDONLY));
    if(!res.rechunkedLevels.empty()&&rfd.fd<0) return Status(Code::ReadFailed,"cannot open "+partial+": "+strerror(errno));
    fs.data=[&](int k,size_t r,hid_t g,MonoVfd& vfd)->Status{
        const MonoChunk* base=idx[(size_t)k].chunks.data()+first[(size_t)k][r]; const size_t n=first[(size_t)k][r+1]-first[(size_t)k][r];
        if(rechunk[r]){ Status rs=rechunkLevel(rfd.fd,g,geo[r],fs.geo[r],base,n,T,newChunks[(size_t)k][r]);
            if(rs.ok()) res.rechunkedChunks+=newChunks[(size_t)k][r].size();
            return rs; }
        std::string ie;
        if(!mono_index_dataset(vfd,g,"Data",mono_idx_geom(geo[r]),n,[base](size_t i,MonoChunk& ch){ ch=base[i]; },&ie)) return Status(Code::WriteFailed,ie);
        return Status(); };
    return finishFile(fs,rawEnd,o.verify,thumbVol,res,t0);
}

// ---------------- tiles: the tile workers' sidecars (mono store; one per tile and level) ----------------
// The chunks of every dataset (k, r) in grid order, i = cx + gx*(cy + gy*cz) (= (z, y, x) order): offset and size,
// filled from the tiles' sidecars, which together must cover every chunk exactly once.
struct DenseChunks { std::vector<uint64_t> off; std::vector<uint32_t> size; };
// sum = the exact 65536-bin histogram of level r of (t, c): the sum of its tiles' histograms (read from a few threads),
// which must count every voxel of the level.
Status sumTileHists(const std::string& cd,const imscore::Lev& L,int r,int t,int c,int T,std::vector<uint64_t>& sum){
    const long n=imscore::tilesPerCT(L); const int nt=(int)std::max(1L,std::min<long>(n,T));
    std::vector<std::vector<uint64_t> > part((size_t)nt); std::atomic<long> next(0); FirstError fe;
    std::string x=runThreads(nt,[&](int w){ std::vector<uint64_t>& h=part[(size_t)w]; h.assign(65536,0); std::vector<uint8_t> b;
        for(;;){ long i=next.fetch_add(1); if(i>=n||fe.set) break; const std::string p=imscore::histName(cd,r,c,t,i);
            if(!imscore::readFileBytes(p,b)||b.size()!=imscore::HIST_BYTES){ fe.put("missing or damaged histogram "+p+" (run the worker of "+imscore::tileOwner(r,c,t,i)+" again)"); break; }
            const uint64_t* v=(const uint64_t*)b.data(); for(size_t j=0;j<65536;j++) h[j]+=v[j]; } });
    if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x);
    sum.assign(65536,0); uint64_t total=0;
    for(const std::vector<uint64_t>& h:part) for(size_t j=0;j<65536;j++) sum[j]+=h[j];
    for(size_t j=0;j<65536;j++) total+=sum[j];
    if(total!=L.size[0]*L.size[1]*L.size[2])
        return Status(Code::ReadFailed,"the histograms of "+cd+"/h_L"+std::to_string(r)+"_C"+std::to_string(c)+"_T"+std::to_string(t)+"_* count "+u64s(total)
                      +" voxels, the level has "+u64s(L.size[0]*L.size[1]*L.size[2]));
    return Status();
}
Status finishTiles(const MonoFinishOptions& o,const std::string& wd,const std::string& partial,int T,double t0,MonoFinishResult& res){
    imscore::Manifest im; Status s=readTileManifest(wd,im); if(!s.ok()) return s;
    const int nT=im.nT, nC=im.nC, nR=(int)im.lev.size(); const std::string cd=wd+"/chunks"; std::string e;
    res.timepoints=nT; res.channels=nC; res.levels=nR;
    std::vector<MonoIdxLevel> geo((size_t)nR);
    const unsigned dtype=dtypeCode(im.dtype);
    for(int r=0;r<nR;r++){ const imscore::Lev& L=im.lev[(size_t)r]; MonoIdxLevel& g=geo[(size_t)r]; g.level=(uint32_t)r; g.dtype=dtype; g.nChunks=1;
        for(int a=0;a<3;a++){ g.size[a]=L.size[a]; g.chunk[a]=(uint32_t)L.block[a]; g.grid[a]=(uint32_t)L.nch[a]; g.nChunks*=L.nch[a]; } }
    const int nD=nT*nC*nR;   // dataset (k, r) = d = k*nR + r
    std::vector<DenseChunks> dense((size_t)nD);
    for(int d=0;d<nD;d++){ dense[(size_t)d].off.assign((size_t)geo[(size_t)(d%nR)].nChunks,0); dense[(size_t)d].size.assign((size_t)geo[(size_t)(d%nR)].nChunks,0); }

    // 1. every tile's sidecar: present, of this layout, holding exactly its tile's chunks (many small files: threads)
    const std::vector<TileId> all=allTiles(im);
    std::vector<char> have(all.size(),0); std::vector<MonoClaim> want(all.size()); std::vector<BlobEnds> ends(all.size());
    { std::atomic<size_t> next(0); FirstError fe; std::string x=runThreads(std::min(64,std::max(16,T)),[&](int){
        MonoIdx ix;
        for(;;){ size_t i=next.fetch_add(1); if(i>=all.size()||fe.set) break;
            const TileId& id=all[i]; const imscore::Lev& L=im.lev[(size_t)id.r]; const MonoIdxLevel& g=geo[(size_t)id.r];
            const std::string owner=imscore::tileOwner(id.r,id.c,id.t,id.tile), p=imscore::tileIdxName(cd,id.r,id.c,id.t,id.tile), again=" (run the worker of "+owner+" again)";
            if(!exists(p)) continue;
            std::string why;
            if(!mono_idx_read(p,ix,&why)){ fe.put(why+again); break; }
            if(ix.owner!=owner||ix.timepoint!=id.t||ix.channel!=id.c||ix.levels.size()!=1||!sameGeom(ix.levels[0],g)||ix.chunks.empty()){
                fe.put(p+" is not the sidecar of "+owner+" in this workdir's layout"+again); break; }
            imscore::Status ts=imscore::checkTileChunks(ix.chunks.size(),[&](uint64_t j,imscore::bpSize& cx,imscore::bpSize& cy,imscore::bpSize& cz){
                cx=ix.chunks[j].cx; cy=ix.chunks[j].cy; cz=ix.chunks[j].cz; },L,id.tile,p);
            if(!ts.ok()){ fe.put(ts.message+again); break; }
            struct stat st; const std::string h=imscore::histName(cd,id.r,id.c,id.t,id.tile);
            if(stat(h.c_str(),&st)!=0||(uint64_t)st.st_size!=imscore::HIST_BYTES){ fe.put("missing or damaged histogram "+h+again); break; }
            DenseChunks& D=dense[(size_t)((id.t*nC+id.c)*nR+id.r)];
            for(const MonoChunk& ch:ix.chunks){
                if(ch.mask!=0){ why=p+": a chunk with filter mask "+u64s(ch.mask); break; }
                const size_t j=(size_t)ch.cx+(size_t)g.grid[0]*((size_t)ch.cy+(size_t)g.grid[1]*ch.cz);
                D.off[j]=ch.offset; D.size[j]=ch.size; }
            if(!why.empty()){ fe.put(why+again); break; }
            MonoClaim& w=want[i]; w.owner=owner; w.offset=ix.claimOffset; w.size=ix.claimSize;
            BlobEnds& b=ends[i]; b.owner=owner; b.a=ix.chunks.front(); b.b=ix.chunks.back(); b.rawA=b.rawB=levelInfo(g).raw;
            have[i]=1;
        } });
      if(!fe.msg.empty()||!x.empty()) return Status(Code::ReadFailed,!fe.msg.empty()?fe.msg:x); }
    for(size_t i=0;i<all.size();i++) if(!have[i]) res.missing.push_back(imscore::tileOwner(all[i].r,all[i].c,all[i].t,all[i].tile));
    if(!res.missing.empty())
        return Status(Code::NotFound,u64s(res.missing.size())+" of "+u64s(all.size())+" tiles are not placed yet: "+listOf(res.missing)+" (run their workers)");
    for(int d=0;d<nD;d++) res.chunks+=dense[(size_t)d].off.size();

    // 2. each sidecar's range must be a claim of its owner; seal; then the blob ends and the thumbnail's voxels
    MonoClaims claims; std::vector<MonoClaim> recs; bool sealed=false;
    if(!claims.open(monoDir(wd),"",&e)||!claims.list(recs,&sealed,&e)) return Status(Code::ReadFailed,e);
    s=checkClaimed(recs,want); if(!s.ok()) return s;
    std::vector<MonoClaim>().swap(recs); std::vector<MonoClaim>().swap(want);
    MonoClaim seal;
    if(!claims.seal(seal,&e)) return Status(Code::WriteFailed,e);
    const uint64_t rawEnd=seal.offset; res.rawEnd=rawEnd;
    s=checkBlobEnds(partial,ends,std::min(64,std::max(16,T))); if(!s.ok()) return s;
    std::vector<BlobEnds>().swap(ends);
    auto denseAt=[&](int k,size_t r,size_t& n,MonoChunkAt& at){
        const DenseChunks& D=dense[(size_t)k*nR+r]; const uint64_t* O=D.off.data(); const uint32_t* S=D.size.data();
        const uint32_t gx=geo[r].grid[0], gy=geo[r].grid[1]; const uint16_t lv=(uint16_t)r;
        n=D.off.size();
        at=[O,S,gx,gy,lv](size_t i,MonoChunk& ch){ ch.offset=O[i]; ch.size=S[i]; ch.cx=(uint32_t)(i%gx); ch.cy=(uint32_t)((i/gx)%gy); ch.cz=(uint32_t)(i/((size_t)gx*gy));
            ch.mask=0; ch.level=lv; ch.reserved=0; }; };
    std::vector<std::vector<uint8_t> > thumbVol;
    s=readThumbnailVolumes(partial,geo,[&](int c,size_t r,size_t& n,MonoChunkAt& at){ denseAt(c,r,n,at); },nC,T,thumbVol); if(!s.ok()) return s;   // timepoint 0: k = c
    res.readSeconds=seconds()-t0;

    // 3.-6. the file: every placed chunk indexed as it is (setup laid the levels out for nT timepoints)
    FileSpec fs; fs.wd=wd; fs.partial=partial; fs.nT=nT; fs.nC=nC; fs.threads=T; fs.geo=geo; fs.im=im; fs.armedWant=res.chunks;
    fs.chunks=denseAt;
    std::map<int,std::vector<uint64_t> > level0;   // (t, c) -> level 0's histogram, also used for the ColorRange
    fs.hist=[&](int r,int t,int c,std::vector<uint64_t>& bins)->Status{
        if(r==0){ auto it=level0.find(t*nC+c); if(it!=level0.end()){ bins=it->second; return Status(); } }
        Status hs=sumTileHists(cd,im.lev[(size_t)r],r,t,c,std::min(64,std::max(16,T)),bins);
        if(hs.ok()&&r==0) level0[t*nC+c]=bins;
        return hs; };
    fs.data=[&](int k,size_t r,hid_t g,MonoVfd& vfd)->Status{
        size_t n=0; MonoChunkAt at; denseAt(k,r,n,at); std::string ie;
        if(!mono_index_dataset(vfd,g,"Data",mono_idx_geom(geo[r]),n,at,&ie)) return Status(Code::WriteFailed,ie);
        return Status(); };
    return finishFile(fs,rawEnd,o.verify,thumbVol,res,t0);
}
}  // namespace
#endif

namespace detail {
bool monoPartPlaced(const std::string& workdir,int t,int c) noexcept{
    try{
#ifdef _WIN32
        (void)workdir; (void)t; (void)c; return false;
#else
        const std::string wd=trimSlash(workdir), p=idxPath(wd,t,c); if(!exists(p)) return false;
        MonoIdx x; std::string e;
        if(!mono_idx_read(p,x,&e)||!mono_idx_validate(x,true,&e)||x.owner!=ownerOf(t,c)||x.timepoint!=t||x.channel!=c) return false;
        return partHistsThere(wd,t,c,x.levels,NULL);
#endif
    }
    catch(...){ return false; }
}
}  // namespace detail

Status monoFinish(const MonoFinishOptions& o,MonoFinishResult* result) noexcept{
    try{
#ifdef _WIN32
        (void)o; (void)result; return Status(Code::Unsupported,NOSUP);
#else
        const double t0=seconds();
        MonoFinishResult res;
        const std::string wd=trimSlash(o.workdir), partial=partialPath(wd);
        MonoManifest m; Status s=readMonoManifest(wd,m); if(!s.ok()) return s;
        const int T=threadsOf(o.numThreads);
        const bool tiles=tilesWorkdir(wd,m);
        res.timepoints=m.nT; res.channels=m.nC;
        res.outputFile=!o.outputFile.empty()?o.outputFile:(!m.output.empty()?m.output:wd+"/output.ims");
        std::string e;
        if(!exists(partial)){
            std::vector<uint8_t> b;
            if(exists(finishedPath(wd))&&mono_read_file(finishedPath(wd),b,&e)){
                res.alreadyFinished=true; res.outputFile.assign(b.begin(),b.end());
                if(result) *result=res;
                return Status();
            }
            return Status(Code::NotFound,"no "+partial+(tiles?": no tile was placed yet (run the workers first)":
                                                       undecidedWorkdir(wd,m)?": nothing was placed yet (run monoplace for the parts, or the tile workers)":
                                                       ": no part was placed yet (run monoplace first)"));
        }
        if(!tiles&&exists(monoDir(wd)+"/tiles"))
            return Status(Code::InvalidArgument,wd+" holds both parts (monoplace) and tiles (worker): its single file cannot be finished (use a new workdir)");
        s=tiles?finishTiles(o,wd,partial,T,t0,res):finishParts(o,m,wd,partial,T,t0,res);
        if(result) *result=res;
        return s;
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

std::string monoPlaceLine(const piw::MonoPlaceResult& r){
    char b[512];
    snprintf(b,sizeof b,"monoplace %s: %llu chunks, %d levels, %.2f GB at %llu: scan %.1f s, decode %.1f s, place %.1f s (%.2f GB/s, %s, %d threads), total %.1f s",
             r.owner.c_str(),(unsigned long long)r.chunks,r.levels,(double)r.bytes/1e9,(unsigned long long)r.claimOffset,r.scanSeconds,r.decodeSeconds,
             r.placeSeconds,gbps(r.bytes,r.placeSeconds),r.direct?"direct I/O":"buffered + sync",r.threads,r.totalSeconds);
    return b;
}
void monoPrintFinish(const piw::MonoFinishResult& r,bool verify){
    if(r.alreadyFinished){ printf("monofinish: already finished: %s\n",r.outputFile.c_str()); return; }
    printf("monofinish: %d timepoints x %d channels x %d levels, %llu chunks, %.2f GB -> %s\n",r.timepoints,r.channels,r.levels,
           (unsigned long long)r.chunks,(double)r.fileBytes/1e9,r.outputFile.c_str());
    printf("  sidecars + seal %.1f s, HDF5 structure %.1f s (%.1f MB after the raw region), stock-driver check %.1f s%s, total %.1f s\n",
           r.readSeconds,r.indexSeconds,(double)(r.fileBytes-r.rawEnd)/1e6,r.checkSeconds,
           verify?(", --verify "+std::to_string((long long)(r.verifySeconds+0.5))+" s").c_str():"",r.totalSeconds);
    if(!r.rechunkedLevels.empty()){ std::string l; for(size_t i=0;i<r.rechunkedLevels.size();i++) l+=(i?", ":"")+std::to_string(r.rechunkedLevels[i]);
        printf("  levels %s re-chunked to the %d-timepoint layout (%llu chunks)\n",l.c_str(),r.timepoints,(unsigned long long)r.rechunkedChunks); }
    if(!r.note.empty()) printf("  note: %s\n",r.note.c_str());
}

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
        printf("%s\n",monoPlaceLine(r).c_str());
        return 0;
    }
    if(cmd=="monofinish"){
        piw::MonoFinishOptions o;
        if(pos.empty()||pos.size()>2) return usage("monofinish");
        o.workdir=pos[0]; if(pos.size()>1) o.outputFile=pos[1]; o.verify=verify; o.numThreads=threads;
        piw::MonoFinishResult r; piw::Status s=piw::monoFinish(o,&r);
        if(!s.ok()){ fail(s); return r.missing.empty()?1:3; }
        monoPrintFinish(r,o.verify);
        return 0;
    }
    if(cmd=="monostatus"){
        if(pos.size()!=1) return usage("monostatus");
        piw::MonoProgress p; piw::Status s=piw::monoStatus(pos[0],p); if(!s.ok()) return fail(s);
        if(p.tiles) printf("monostatus: %s: %llu of %llu tiles placed (%d levels, %d timepoints x %d channels), %.2f GB claimed, %s\n",pos[0].c_str(),
               (unsigned long long)p.placed.size(),(unsigned long long)(p.placed.size()+p.missing.size()),p.levels,p.timepoints,p.channels,
               p.claimedBytes>MONO_RAW_BEGIN?(double)(p.claimedBytes-MONO_RAW_BEGIN)/1e9:0.0,
               p.finished?("finished: "+p.finishedFile).c_str():p.sealed?"finishing (sealed)":"open for placements");
        else printf("monostatus: %s: %llu of %d parts placed (%d timepoints x %d channels), %.2f GB claimed, %s\n",pos[0].c_str(),
               (unsigned long long)p.placed.size(),p.timepoints*p.channels,p.timepoints,p.channels,
               p.claimedBytes>MONO_RAW_BEGIN?(double)(p.claimedBytes-MONO_RAW_BEGIN)/1e9:0.0,
               p.finished?("finished: "+p.finishedFile).c_str():p.sealed?"finishing (sealed)":"open for placements");
        for(size_t i=0;i<p.missing.size()&&i<50;i++) printf("  missing %s\n",p.missing[i].c_str());
        if(p.missing.size()>50) printf("  ... and %llu more\n",(unsigned long long)(p.missing.size()-50));
        return p.missing.empty()?0:3;
    }
    return usage(cmd.c_str());
}
