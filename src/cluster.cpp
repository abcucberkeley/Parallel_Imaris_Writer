// The cluster steps as a plan of independent tasks (parallel.h): clusterPlan, clusterTask, clusterStatus,
// clusterFinish, and the task/status/finish subcommands (plan's command line is in src/cli.cpp, with the converter's
// options). The steps themselves are the existing ones: piw::convert + monoPlace + monoFinish (src/mono.h) for a
// time series, setup's tile workers + monoFinish or the packs merge (src/parallel.cpp) for a tiled volume.
//
// The plan is <workdir>/manifest.json. A time-series plan holds what monoPlace and monoFinish read (timepoints,
// channels, voxel size, output, metadata: as monoinit writes it) plus the stages and each task's conversion
// ("convert": reader, files[channel][timepoint], voxel size, bounding box, block and process block); a tile plan is
// setup's manifest plus the stages. "kind" (series or tiles) tells the single-file steps which writers the file takes.
#include "parallel.h"
#include "imscore.h"
#include "imsmeta.h"
#include "monoclaim.h"
#include "monoidx.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <mutex>
#include <thread>
#include <sys/stat.h>
#ifdef _WIN32
#include <direct.h>
#include <stdlib.h>
#else
#include <dirent.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/statvfs.h>
#endif
#endif

namespace piw { namespace detail {
// src/converter.cpp
Status inspectConversion(const ConvertOptions& options, uint64_t size[3], DataType& type, imsmeta::Metadata& metadata);
} }

namespace {
using piw::Code;
using piw::Status;
using nlohmann::ordered_json;

const char* const NOSUP="the single-file cluster output is not supported on this platform (a tile plan with --store packs is)";
const char* const UINT16_ONLY="the single-file cluster output takes uint16 data only for now";
Status internalError(const char* what){ return Status(Code::Internal,std::string("unexpected error: ")+what); }
std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }
double seconds(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
int threadsOf(int n){ return n>0?n:std::max(1,piw::defaultThreads()); }
bool exists(const std::string& p){ struct stat st; return stat(p.c_str(),&st)==0; }
std::string trimSlash(std::string w){ while(w.size()>1&&(w[w.size()-1]=='/'||w[w.size()-1]=='\\')) w.erase(w.size()-1); return w; }
bool absolute(const std::string& p){
#ifdef _WIN32
    return p.size()>=2&&(p[1]==':'||((p[0]=='\\'||p[0]=='/')&&(p[1]=='\\'||p[1]=='/')));
#else
    return !p.empty()&&p[0]=='/';
#endif
}
// p relative to the current folder made absolute: the tasks may run anywhere.
std::string absPath(const std::string& p){
    if(p.empty()||absolute(p)) return p;
    char b[4096];
#ifdef _WIN32
    if(!_getcwd(b,sizeof b)) return p;
    return std::string(b)+"\\"+p;
#else
    if(!getcwd(b,sizeof b)) return p;
    std::string q=p; while(q.compare(0,2,"./")==0) q.erase(0,2);
    return std::string(b)+"/"+q;
#endif
}
// Task indices as ranges, e.g. 0-3,7 (a job array's --array list).
std::string rangesText(const std::vector<long>& v){
    std::string o;
    for(size_t i=0;i<v.size();){ size_t j=i; while(j+1<v.size()&&v[j+1]==v[j]+1) j++;
        o+=(o.empty()?"":",")+std::to_string(v[i])+(j>i?"-"+std::to_string(v[j]):std::string()); i=j+1; }
    return o;
}
const char* typeName(piw::DataType t){
    switch(t){ case piw::DataType::UInt8: return "uint8"; case piw::DataType::UInt16: return "uint16";
               case piw::DataType::UInt32: return "uint32"; case piw::DataType::Float32: return "float32"; }
    return "unknown";
}
bool typeFromName(const std::string& n,piw::DataType& t){
    if(n=="uint8") t=piw::DataType::UInt8; else if(n=="uint16") t=piw::DataType::UInt16;
    else if(n=="uint32") t=piw::DataType::UInt32; else if(n=="float32") t=piw::DataType::Float32; else return false;
    return true;
}
// The single file's names, as src/mono.cpp writes them.
std::string monoDir(const std::string& wd){ return imscore::monoClaimsDir(wd); }
std::string partOwner(int t,int c){ char b[48]; snprintf(b,sizeof b,"P_C%d_T%d",c,t); return b; }
std::string partIdx(const std::string& wd,int t,int c){ return monoDir(wd)+"/"+partOwner(t,c)+".idx"; }
std::string partHist(const std::string& wd,int r,int t,int c){ char b[64]; snprintf(b,sizeof b,"/h_L%d_C%d_T%d.hist",r,c,t); return monoDir(wd)+b; }

// ---------------- the plan of a workdir ----------------
struct PlanFile {
    std::string wd, kind, store, output;     // kind: series | tiles
    bool fromPlan=false;                     // written by clusterPlan (else setup's or monoinit's)
    int nT=0, nC=0, levels=0;
    uint64_t size[3]={0,0,0};
    piw::DataType type=piw::DataType::UInt16;
    std::vector<piw::ClusterStage> stages;
    long tilesPerTask=1;
    piw::ConvertOptions convert;             // series: the conversion of every task (files[c][t]; no metadata)
    imscore::Manifest tiles;                 // tiles: setup's manifest
};
std::vector<piw::ClusterStage> tileStages(const imscore::Manifest& m,long perTask){
    std::vector<piw::ClusterStage> v;
    for(int r=0;r<(int)m.lev.size();r++){ piw::ClusterStage s; s.name="level"+std::to_string(r);
        const long jobs=imscore::jobsAtLevel(m,r); s.tasks=(jobs+perTask-1)/perTask; s.after=r?"level"+std::to_string(r-1):std::string(); v.push_back(s); }
    return v;
}
ordered_json stagesJson(const std::vector<piw::ClusterStage>& v){
    ordered_json a=ordered_json::array();
    for(const piw::ClusterStage& s:v){ ordered_json e; e["name"]=s.name; e["tasks"]=s.tasks; e["after"]=s.after; a.push_back(e); }
    return a;
}
bool getU64s(const nlohmann::json& j,const char* k,size_t n,uint64_t* v){
    if(!j.contains(k)||!j[k].is_array()||j[k].size()!=n) return false;
    for(size_t i=0;i<n;i++) v[i]=j[k][i].get<uint64_t>();
    return true;
}
Status readPlanFile(const std::string& workdir,PlanFile& pf){
    pf=PlanFile(); pf.wd=trimSlash(workdir);
    const std::string p=pf.wd+"/manifest.json"; std::vector<uint8_t> b;
    if(!imscore::readFileBytes(p,b)) return Status(Code::NotFound,"cannot read "+p+" (run plan first)");
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        pf.kind=j.value("kind",std::string()); pf.fromPlan=!pf.kind.empty();
        pf.store=j.value("store",std::string()); pf.output=j.value("output",std::string());
        const bool setup=j.contains("level")&&j.contains("zarrs");
        if(!pf.fromPlan)   // setup's workdir: tiles once a worker marked it (packs: always); monoinit's: a time series
            pf.kind=setup&&(pf.store=="packs"||exists(monoDir(pf.wd)+"/tiles"))?"tiles":"series";
        if(pf.kind=="tiles"){
            if(!setup) return Status(Code::InvalidArgument,p+" is not a tile plan (no level or zarrs)");
            imscore::Status ms=imscore::readManifest(pf.wd,pf.tiles); if(!ms.ok()) return Status(Code::InvalidArgument,ms.message);
            const imscore::Manifest& m=pf.tiles;
            pf.nT=m.nT; pf.nC=m.nC; pf.levels=(int)m.lev.size(); pf.size[0]=m.X; pf.size[1]=m.Y; pf.size[2]=m.Z; pf.store=m.store;
            pf.type=m.elem==1?piw::DataType::UInt8:m.elem==4?piw::DataType::UInt32:piw::DataType::UInt16;
            pf.tilesPerTask=std::max(1L,j.value("tilesPerTask",1L));
            pf.stages=tileStages(m,pf.tilesPerTask);
            return Status();
        }
        if(pf.kind!="series") return Status(Code::InvalidArgument,p+": unknown kind '"+pf.kind+"' (expected series or tiles)");
        pf.nT=j.value("timepoints",0); pf.nC=j.value("channels",0);
        if(pf.nT<1||pf.nC<1) return Status(Code::InvalidArgument,p+" needs timepoints >= 1 and channels >= 1");
        piw::ClusterStage st; st.name="convert"; st.tasks=(long)pf.nT*pf.nC; pf.stages.push_back(st);
        if(!pf.fromPlan) return Status();
        pf.levels=j.value("levels",0); getU64s(j,"size",3,pf.size);
        if(!typeFromName(j.value("dataType",std::string()),pf.type)) return Status(Code::InvalidArgument,p+": unknown dataType");
        const nlohmann::json& c=j.at("convert"); piw::ConvertOptions& o=pf.convert;
        o.reader=c.at("reader").get<std::string>();
        for(auto& ch:c.at("files")){ std::vector<std::string> v; for(auto& f:ch) v.push_back(f.get<std::string>()); o.files.push_back(v); }
        if((int)o.files.size()!=pf.nC) return Status(Code::InvalidArgument,p+": convert.files does not list "+std::to_string(pf.nC)+" channels");
        for(const std::vector<std::string>& v:o.files) if((int)v.size()!=pf.nT) return Status(Code::InvalidArgument,p+": convert.files does not list "+std::to_string(pf.nT)+" timepoints per channel");
        for(int a=0;a<3;a++) o.voxelSize[a]=c.at("voxelSize").at(a).get<float>();
        o.hasBoundingBox=getU64s(c,"boundingBox",6,o.boundingBox);
        o.hasBlockSize=getU64s(c,"blockSize",3,o.blockSize);
        o.hasProcessBlock=getU64s(c,"processBlock",3,o.processBlock);
        o.xOnImarisX=c.value("xOnImarisX",false);
    }catch(const std::exception& e){ return Status(Code::InvalidArgument,"invalid "+p+": "+e.what()); }
    return Status();
}

// ---------------- what is done ----------------
#ifndef _WIN32
// A part's commit point: its sidecar (whole, of this part, every level's histogram there).
bool partDone(const std::string& wd,int t,int c){
    const std::string p=partIdx(wd,t,c); if(!exists(p)) return false;
    MonoIdx x; std::string e;
    if(!mono_idx_read(p,x,&e)||!mono_idx_validate(x,true,&e)||x.owner!=partOwner(t,c)||x.timepoint!=t||x.channel!=c) return false;
    for(size_t r=0;r<x.levels.size();r++){ struct stat st;
        if(stat(partHist(wd,(int)r,t,c).c_str(),&st)!=0||(uint64_t)st.st_size!=imscore::HIST_BYTES) return false; }
    return true;
}
#endif
// A tile's commit point, what monitor waits for: its histogram and its sidecar (mono) or pack (packs).
bool tileDone(const imscore::Manifest& m,const std::string& wd,int r,long i){
    const long perCT=imscore::tilesPerCT(m.lev[(size_t)r]); const int c=(int)(i/((long)m.nT*perCT)); const long rem=i%((long)m.nT*perCT);
    const int t=(int)(rem/perCT); const long ti=rem%perCT; const std::string cd=wd+"/chunks";
    return exists(m.store=="mono"?imscore::tileIdxName(cd,r,c,t,ti):imscore::packName(cd,r,c,t,ti))&&exists(imscore::histName(cd,r,c,t,ti));
}
std::string tileOwnerOf(const imscore::Manifest& m,int r,long i){
    const long perCT=imscore::tilesPerCT(m.lev[(size_t)r]); const int c=(int)(i/((long)m.nT*perCT)); const long rem=i%((long)m.nT*perCT);
    return imscore::tileOwner(r,c,(int)(rem/perCT),rem%perCT);
}
// Runs fn(i) for i in [0, n) on a few threads (many small files on a shared filesystem).
template<class Fn> void forEachIndex(size_t n,int threads,Fn fn){
    std::atomic<size_t> next(0); std::vector<std::thread> th;
    auto body=[&](){ for(;;){ size_t i=next.fetch_add(1); if(i>=n) break; fn(i); } };
    const int k=(int)std::min<size_t>(n,(size_t)std::max(1,threads));
    for(int w=1;w<k;w++) th.push_back(std::thread(body));
    body();
    for(size_t i=0;i<th.size();i++) th[i].join();
}
// The missing tasks of every stage, and the owners (parts or tiles) not placed.
void missingTasks(const PlanFile& pf,std::vector<piw::ClusterStageStatus>& out,std::vector<std::string>* owners){
    out.clear();
    if(pf.kind=="series"){
        piw::ClusterStageStatus s; s.name=pf.stages[0].name; s.tasks=pf.stages[0].tasks;
        std::vector<char> done((size_t)s.tasks,0);
        forEachIndex((size_t)s.tasks,16,[&](size_t i){ done[i]=exists(partIdx(pf.wd,(int)(i/pf.nC),(int)(i%pf.nC))); });
        for(long i=0;i<s.tasks;i++) if(!done[(size_t)i]){ s.missing.push_back(i); if(owners) owners->push_back(partOwner((int)(i/pf.nC),(int)(i%pf.nC))); }
        out.push_back(s); return;
    }
    const imscore::Manifest& m=pf.tiles;
    for(int r=0;r<(int)m.lev.size();r++){
        piw::ClusterStageStatus s; s.name=pf.stages[(size_t)r].name; s.tasks=pf.stages[(size_t)r].tasks;
        const long jobs=imscore::jobsAtLevel(m,r); std::vector<char> done((size_t)jobs,0);
        forEachIndex((size_t)jobs,16,[&](size_t i){ done[i]=tileDone(m,pf.wd,r,(long)i); });
        for(long k=0;k<s.tasks;k++){ bool all=true;
            for(long i=k*pf.tilesPerTask;i<std::min(jobs,(k+1)*pf.tilesPerTask);i++) if(!done[(size_t)i]){ all=false; if(owners) owners->push_back(tileOwnerOf(m,r,i)); }
            if(!all) s.missing.push_back(k); }
        out.push_back(s);
    }
}
// The single file's journal: sealed (finishing or finished) and the finished output's name.
Status journalState(const std::string& wd,bool& sealed,bool& finished,std::string& finishedFile){
    sealed=finished=false; finishedFile.clear(); std::vector<uint8_t> b; std::string e;
    if(exists(monoDir(wd)+"/finished")&&mono_read_file(monoDir(wd)+"/finished",b,&e)){ finished=sealed=true; finishedFile.assign(b.begin(),b.end()); }
    if(!finished&&exists(monoDir(wd)+"/claims.mode")){   // open() would create the journal: only when there is one
        MonoClaims cl; uint64_t end=0;
        if(!cl.open(monoDir(wd),"",&e)||!cl.end(&end,&sealed,&e)) return Status(Code::ReadFailed,e);
    }
    return Status();
}
bool singleFile(const PlanFile& pf){ return pf.kind=="series"||pf.store=="mono"; }
piw::ClusterPlan planOf(const PlanFile& pf){
    piw::ClusterPlan p; p.workdir=pf.wd; p.kind=pf.kind; p.store=pf.kind=="series"?std::string("mono"):pf.store;
    p.outputFile=!pf.output.empty()?pf.output:pf.wd+"/output.ims";
    p.timepoints=pf.nT; p.channels=pf.nC; p.levels=pf.levels; for(int a=0;a<3;a++) p.size[a]=pf.size[a];
    p.dataType=pf.type; p.stages=pf.stages;
    return p;
}

// ---------------- staging a part (series tasks) ----------------
#ifndef _WIN32
const double STAGE_FACTOR=1.15, STAGE_EXTRA=8e9;   // part size estimate = raw bytes x 1.15 + 8 GB (the converter's own memory)
// The job's memory: Slurm's (MB), else the machine's.
double jobMemory(){
    const char* n=getenv("SLURM_MEM_PER_NODE"); const char* c=getenv("SLURM_MEM_PER_CPU"); const char* k=getenv("SLURM_CPUS_PER_TASK");
    if(n&&*n) return atof(n)*1048576.0;
    if(c&&*c) return atof(c)*1048576.0*(k&&*k?std::max(1.0,atof(k)):1.0);
    long pages=sysconf(_SC_PHYS_PAGES), ps=sysconf(_SC_PAGESIZE);
    return pages>0&&ps>0?(double)pages*(double)ps:0.0;
}
// Free bytes of /dev/shm (0: none, as on macOS).
double shmFree(){
#if defined(__linux__)
    struct statvfs v; if(statvfs("/dev/shm",&v)!=0) return 0;
    return (double)v.f_bavail*(double)v.f_frsize;
#else
    return 0;
#endif
}
const char* const SHM_PREFIX="piw_part_";
// Removes the parts that killed tasks of this user on this machine left in /dev/shm: piw_part_<pid>_... of a pid
// that no longer runs.
void removeStaleShm(){
#if defined(__linux__)
    DIR* d=opendir("/dev/shm"); if(!d) return;
    const size_t pl=strlen(SHM_PREFIX); struct dirent* e; std::vector<std::string> stale;
    while((e=readdir(d))){ const char* n=e->d_name; if(strncmp(n,SHM_PREFIX,pl)) continue;
        char* end=NULL; long pid=strtol(n+pl,&end,10); if(end==n+pl||*end!='_'||pid<=0) continue;
        std::string p=std::string("/dev/shm/")+n; struct stat st;
        if(stat(p.c_str(),&st)==0&&st.st_uid==getuid()&&kill((pid_t)pid,0)!=0&&errno==ESRCH) stale.push_back(p); }
    closedir(d);
    for(const std::string& p:stale) unlink(p.c_str());
#endif
}
#endif

// ---------------- the tasks ----------------
Status seriesTask(const PlanFile& pf,long index,const piw::ClusterTaskOptions& o,piw::ClusterTaskResult& res){
#ifdef _WIN32
    (void)pf; (void)index; (void)o; (void)res; return Status(Code::Unsupported,NOSUP);
#else
    if(!pf.fromPlan) return Status(Code::InvalidArgument,pf.wd+" was not made by plan: run its parts with the converter and monoplace");
    const int t=(int)(index/pf.nC), c=(int)(index%pf.nC);
    res.timepoint=t; res.channel=c; res.owners.push_back(partOwner(t,c));
    bool sealed=false, finished=false; std::string ff;
    Status s=journalState(pf.wd,sealed,finished,ff); if(!s.ok()) return s;
    if(sealed) return Status(Code::InvalidArgument,"the single file in "+pf.wd+" was already finished (claims journal sealed): no more tasks can run");
    if(partDone(pf.wd,t,c)){ res.skipped=true; return Status(); }
    const int T=threadsOf(o.numThreads);
    piw::ConvertOptions co=pf.convert;   // one (timepoint, channel); the metadata is the finisher's
    co.files.assign(1,std::vector<std::string>(1,pf.convert.files[(size_t)c][(size_t)t]));
    co.numThreads=T;
    if(o.hasProcessBlock){ co.hasProcessBlock=true; for(int a=0;a<3;a++) co.processBlock[a]=o.processBlock[a]; }
    char name[160]; snprintf(name,sizeof name,"%s%ld_t%d_c%d.ims",SHM_PREFIX,(long)getpid(),t,c);
    std::string dir=o.stageDir;
    if(dir.empty()){
        const double raw=(double)pf.size[0]*(double)pf.size[1]*(double)pf.size[2]*2.0, need=raw*STAGE_FACTOR+STAGE_EXTRA;
        if(need<=0.9*jobMemory()&&need<=shmFree()){ removeStaleShm(); dir="/dev/shm"; res.memoryStage=true; }
    }
    auto inWorkdir=[&](){
        const std::string d=pf.wd+"/stage";
        imscore::Status ms=imscore::makeDirs(d);
        if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
        dir=d; res.memoryStage=false;
        return Status(); };
    if(dir.empty()){ s=inWorkdir(); if(!s.ok()) return s; }
    else if(!res.memoryStage){ imscore::Status ms=imscore::makeDirs(dir); if(!ms.ok()) return Status(Code::WriteFailed,ms.message); }
    // a unique name: the same task may run twice at once (also on two nodes, in a shared folder)
    auto partIn=[&](){ return dir+"/piw_part_"+mono_process_tag()+"_t"+std::to_string(t)+"_c"+std::to_string(c)+".ims"; };
    std::string part=res.memoryStage?dir+"/"+name:partIn();
    co.outputFile=res.part=part;
    const double t0=seconds();
    s=piw::convert(co,o.callbacks);
    if(!s.ok()&&res.memoryStage&&s.code==Code::WriteFailed){   // /dev/shm filled up: once more in the workdir
        unlink(part.c_str());
        Status ws=inWorkdir(); if(!ws.ok()) return ws;
        part=res.part=partIn(); co.outputFile=part;
        s=piw::convert(co,o.callbacks);
    }
    res.convertSeconds=seconds()-t0;
    if(!s.ok()){ unlink(part.c_str()); return Status(s.code,"converting (t="+std::to_string(t)+", c="+std::to_string(c)+"): "+s.message); }
    const double tp=seconds();
    piw::MonoPlaceOptions po; po.workdir=pf.wd; po.timepoint=t; po.channel=c; po.partFile=part; po.numThreads=T;
    s=piw::monoPlace(po,&res.place);
    res.placeSeconds=seconds()-tp;
    unlink(part.c_str());
    return s;
#endif
}
Status tilesTask(const PlanFile& pf,int r,long index,const piw::ClusterTaskOptions& o,piw::ClusterTaskResult& res){
#ifdef _WIN32
    if(pf.store=="mono") return Status(Code::Unsupported,NOSUP);
#else
    if(pf.store=="mono"){
        bool sealed=false, finished=false; std::string ff;
        Status s=journalState(pf.wd,sealed,finished,ff); if(!s.ok()) return s;
        if(sealed) return Status(Code::InvalidArgument,"the single file in "+pf.wd+" was already finished (claims journal sealed): no more tasks can run");
    }
#endif
    const imscore::Manifest& m=pf.tiles; const long jobs=imscore::jobsAtLevel(m,r);
    const long first=index*pf.tilesPerTask, count=std::min(pf.tilesPerTask,jobs-first);
    bool all=true;
    for(long i=first;i<first+count;i++){ res.owners.push_back(tileOwnerOf(m,r,i)); all=all&&tileDone(m,pf.wd,r,i); }
    if(all){ res.skipped=true; return Status(); }
    return piw::detail::tileTask(pf.wd,r,first,count,threadsOf(o.numThreads),&res.lines);
}

// ---------------- plan ----------------
// The text of a series plan's manifest.
std::string seriesManifest(const piw::ConvertOptions& co,const uint64_t size[3],int levels,const imsmeta::Metadata& md,const std::vector<piw::ClusterStage>& stages){
    ordered_json j;
    const int nC=(int)co.files.size(), nT=(int)co.files[0].size();
    const int ax[3]={co.xOnImarisX?1:0,co.xOnImarisX?0:1,2};   // Imaris axis i = input axis ax[i]
    j["kind"]="series"; j["stages"]=stagesJson(stages);
    j["timepoints"]=nT; j["channels"]=nC;
    j["voxel"]={imscore::floatExact(co.voxelSize[ax[0]]),imscore::floatExact(co.voxelSize[ax[1]]),imscore::floatExact(co.voxelSize[ax[2]])};
    j["output"]=co.outputFile;
    j["size"]={(unsigned long long)size[0],(unsigned long long)size[1],(unsigned long long)size[2]};
    j["dataType"]="uint16"; j["levels"]=levels; j["taskIndex"]="timepoint * channels + channel";
    ordered_json c;
    c["reader"]=co.reader;
    c["voxelSize"]={imscore::floatExact(co.voxelSize[0]),imscore::floatExact(co.voxelSize[1]),imscore::floatExact(co.voxelSize[2])};
    if(co.hasBoundingBox){ ordered_json b=ordered_json::array(); for(int i=0;i<6;i++) b.push_back((unsigned long long)co.boundingBox[i]); c["boundingBox"]=b; }
    if(co.hasBlockSize) c["blockSize"]={(unsigned long long)co.blockSize[0],(unsigned long long)co.blockSize[1],(unsigned long long)co.blockSize[2]};
    if(co.hasProcessBlock) c["processBlock"]={(unsigned long long)co.processBlock[0],(unsigned long long)co.processBlock[1],(unsigned long long)co.processBlock[2]};
    c["xOnImarisX"]=co.xOnImarisX;
    ordered_json fs=ordered_json::array();
    for(const std::vector<std::string>& ch:co.files){ ordered_json a=ordered_json::array(); for(const std::string& f:ch) a.push_back(f); fs.push_back(a); }
    c["files"]=fs;
    j["convert"]=c;
    j["metadata"]=imscore::metadataJson(md);
    return j.dump(2)+"\n";
}
Status planValidated(const piw::ClusterPlanOptions& o,piw::ClusterPlan* plan){
    const std::string wd=absPath(trimSlash(o.workdir));
    if(o.kind!="series"&&o.kind!="tiles") return Status(Code::InvalidArgument,"unknown plan kind '"+o.kind+"' (expected series or tiles)");
    piw::ConvertOptions co=o.convert;
    if(co.reader!="tiff"&&co.reader!="zarr") return Status(Code::InvalidArgument,"Reader type \""+co.reader+"\" is not a supported reader type.");
    if(co.files.empty()){
        if(o.folders.empty()||o.patterns.empty()) return Status(Code::InvalidArgument,"plan needs input folders and channel patterns (or the files)");
        Status s=piw::findInputFiles(o.folders,o.patterns,co.reader,co.files); if(!s.ok()) return s;
    }
    if(co.channelNames.empty()&&o.patterns.size()==co.files.size()) co.channelNames=o.patterns;   // as the command line
    for(std::vector<std::string>& ch:co.files) for(std::string& f:ch) f=absPath(f);
    co.outputFile=absPath(co.outputFile.empty()?wd+"/output.ims":co.outputFile);
    uint64_t size[3]={0,0,0}; piw::DataType type=piw::DataType::UInt16; imsmeta::Metadata md;
    Status s=piw::detail::inspectConversion(co,size,type,md); if(!s.ok()) return s;
    const int nC=(int)co.files.size(), nT=(int)co.files[0].size();
    std::string text, store; std::vector<piw::ClusterStage> stages; int levels=0; long perTask=1;
    imscore::Manifest m;
    if(o.kind=="series"){
#ifdef _WIN32
        return Status(Code::Unsupported,NOSUP);
#endif
        if(type!=piw::DataType::UInt16) return Status(Code::Unsupported,std::string(UINT16_ONLY)+"; "+co.files[0][0]+" is "+typeName(type));
        std::vector<imscore::Vec3> sz=imscore::GetOptimalImagePyramid((1024*1024)/2,{size[0],size[1],size[2]},true); levels=(int)sz.size();
        piw::ClusterStage st; st.name="convert"; st.tasks=(long)nT*nC; stages.push_back(st);
        text=seriesManifest(co,size,levels,md,stages); store="mono";
    }
    else{
        if(co.reader!="zarr") return Status(Code::Unsupported,"a tile plan reads Zarr input only");
        if(type!=piw::DataType::UInt16) return Status(Code::Unsupported,"the tile workers take uint16 data only; "+co.files[0][0]+" is "+typeName(type));
        if(co.xOnImarisX) return Status(Code::Unsupported,"a tile plan keeps the input's axes 0, 1, 2 as Imaris X, Y, Z (no xOnImarisX)");
        if(!o.tileSize[0]||!o.tileSize[1]||!o.tileSize[2]) return Status(Code::InvalidArgument,"a tile plan needs a tile size of at least 1 voxel along every axis");
        if(o.tilesPerTask<1) return Status(Code::InvalidArgument,"tilesPerTask must be at least 1");
        if(o.chunkBytes&&o.chunkBytes<4096) return Status(Code::InvalidArgument,"the chunk budget must be at least 4096 bytes");
#ifdef _WIN32
        store=o.store.empty()?std::string("packs"):o.store;
#else
        store=o.store.empty()?std::string("mono"):o.store;
#endif
        if(store!="mono"&&store!="packs") return Status(Code::InvalidArgument,"unknown store '"+store+"' (expected mono or packs)");
#ifdef _WIN32
        if(store=="mono") return Status(Code::Unsupported,NOSUP);
#endif
        m.nC=nC; m.nT=nT; m.X=size[0]; m.Y=size[1]; m.Z=size[2]; m.elem=2; m.store=store; m.meta=md;
        for(int a=0;a<3;a++){ m.vox[a]=co.voxelSize[a]; m.cap[a]=(imscore::bpSize)o.tileSize[a]; m.org[a]=co.hasBoundingBox?co.boundingBox[a]:0; }
        m.zarrs.resize((size_t)nC*nT); for(int c=0;c<nC;c++) for(int t=0;t<nT;t++) m.zarrs[(size_t)c*nT+t]=co.files[(size_t)c][(size_t)t];
        const uint64_t cb=o.chunkBytes?o.chunkBytes:imscore::chunkBudgetBytes();
        m.lev=imscore::makeLevels(m.X,m.Y,m.Z,2,m.cap,(imscore::bpSize)cb,(imscore::bpSize)nT);
        levels=(int)m.lev.size(); perTask=o.tilesPerTask; stages=tileStages(m,perTask);
        ordered_json extra; extra["kind"]="tiles"; extra["stages"]=stagesJson(stages); extra["tilesPerTask"]=perTask;
        extra["output"]=co.outputFile; extra["chunkBytes"]=(unsigned long long)cb;
        text=imscore::manifestText(m,&extra);
    }
    // an existing plan is kept when it is this one; anything else there is refused
    const std::string mp=wd+"/manifest.json";
    std::vector<uint8_t> have;
    if(imscore::readFileBytes(mp,have)){
        if(std::string(have.begin(),have.end())!=text) return Status(Code::InvalidArgument,mp+" already exists with another plan (use a new workdir)");
    }
    else{
        imscore::Status ms=imscore::makeDirs(wd); if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
        if(o.kind=="tiles"){
            ms=imscore::makeDirs(wd+"/chunks"); if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
#ifndef _WIN32
            if(store=="mono"){   // a tile workdir from the start: monoplace refuses it
                ms=imscore::makeDirs(monoDir(wd)); if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
                std::string e; if(!mono_write_atomic(monoDir(wd)+"/tiles","",0,&e)) return Status(Code::WriteFailed,e);
            }
#endif
        }
        ms=imscore::writeFileAtomic(mp,text.data(),text.size()); if(!ms.ok()) return Status(Code::WriteFailed,ms.message);
    }
    if(plan){ PlanFile pf; s=readPlanFile(wd,pf); if(!s.ok()) return s; *plan=planOf(pf); }
    return Status();
}

volatile sig_atomic_t gStop=0;
void onSignal(int){ gStop=1; }
int stopRequested(void*){ return gStop?1:0; }
void printMessage(const char* text,void*){ fputs(text,stdout); fflush(stdout); }
}  // namespace

namespace piw {

Status clusterPlan(const ClusterPlanOptions& options,ClusterPlan* plan) noexcept{
    try{
        if(options.workdir.empty()) return Status(Code::InvalidArgument,"plan needs a workdir");
        return planValidated(options,plan);
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status clusterReadPlan(const std::string& workdir,ClusterPlan& plan) noexcept{
    try{
        PlanFile pf; Status s=readPlanFile(workdir,pf); if(!s.ok()) return s;
        plan=planOf(pf); return Status();
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status clusterTask(const std::string& workdir,const std::string& stage,long index,const ClusterTaskOptions& options,ClusterTaskResult* result) noexcept{
    try{
        const double t0=seconds();
        ClusterTaskResult res; res.index=index;
        PlanFile pf; Status s=readPlanFile(workdir,pf); if(!s.ok()) return s;
        if(options.numThreads<0) return Status(Code::InvalidArgument,"the number of threads cannot be negative");
        int k=-1;   // the stage: its name, or its number
        for(size_t i=0;i<pf.stages.size();i++) if(pf.stages[i].name==stage) k=(int)i;
        if(k<0&&!stage.empty()&&stage.find_first_not_of("0123456789")==std::string::npos){ long n=atol(stage.c_str()); if(n>=0&&n<(long)pf.stages.size()) k=(int)n; }
        if(k<0){ std::string names; for(const ClusterStage& st:pf.stages) names+=(names.empty()?"":", ")+st.name;
            return Status(Code::InvalidArgument,"no stage '"+stage+"' in "+pf.wd+" (its stages: "+names+")"); }
        res.stage=pf.stages[(size_t)k].name;
        if(index<0||index>=pf.stages[(size_t)k].tasks)
            return Status(Code::InvalidArgument,"task "+std::to_string(index)+" out of range [0,"+std::to_string(pf.stages[(size_t)k].tasks)+") for stage "+res.stage);
        s=pf.kind=="series"?seriesTask(pf,index,options,res):tilesTask(pf,k,index,options,res);
        res.totalSeconds=seconds()-t0;
        if(result) *result=res;
        return s;
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status clusterStatus(const std::string& workdir,ClusterStatus& status) noexcept{
    try{
        status=ClusterStatus();
        PlanFile pf; Status s=readPlanFile(workdir,pf); if(!s.ok()) return s;
        status.kind=pf.kind; status.store=pf.kind=="series"?std::string("mono"):pf.store;
        missingTasks(pf,status.stages,nullptr);
        if(singleFile(pf)) return journalState(pf.wd,status.sealed,status.finished,status.finishedFile);
        return Status();
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

Status clusterFinish(const std::string& workdir,const std::string& outputFile,const ClusterFinishOptions& options,MonoFinishResult* result) noexcept{
    try{
        const double t0=seconds();
        MonoFinishResult res;
        PlanFile pf; Status s=readPlanFile(workdir,pf); if(!s.ok()) return s;
        res.outputFile=!outputFile.empty()?outputFile:!pf.output.empty()?pf.output:pf.wd+"/output.ims";
        res.timepoints=pf.nT; res.channels=pf.nC; res.levels=pf.levels;
        bool sealed=false, finished=false; std::string ff;
        if(singleFile(pf)){ s=journalState(pf.wd,sealed,finished,ff); if(!s.ok()) return s; }
        if(!finished){
            std::vector<ClusterStageStatus> st; missingTasks(pf,st,&res.missing);
            std::string list; size_t n=0, all=0;
            for(const ClusterStageStatus& x:st){ all+=(size_t)x.tasks; n+=x.missing.size(); if(!x.missing.empty()) list+=(list.empty()?"":"; ")+x.name+" "+rangesText(x.missing); }
            if(n){ if(result) *result=res;
                return Status(Code::NotFound,u64s(n)+" of "+u64s(all)+" tasks are not done yet: "+list+" (run them)"); }
        }
        if(singleFile(pf)){
            MonoFinishOptions mo; mo.workdir=pf.wd; mo.outputFile=res.outputFile; mo.verify=options.verify; mo.numThreads=options.numThreads;
            s=monoFinish(mo,&res);
            if(s.ok()&&!res.alreadyFinished){ std::string sd=pf.wd+"/stage";   // parts that killed tasks left in the workdir
#ifndef _WIN32
                DIR* d=opendir(sd.c_str()); struct dirent* e;
                if(d){ while((e=readdir(d))) if(!strncmp(e->d_name,"piw_part_",9)) unlink((sd+"/"+e->d_name).c_str()); closedir(d); rmdir(sd.c_str()); }
#endif
            }
        }
        else{
            s=detail::packsFinish(pf.wd,res.outputFile);
            res.totalSeconds=seconds()-t0;
        }
        if(result) *result=res;
        return s;
    }
    catch(const std::exception& e){ return internalError(e.what()); }
    catch(...){ return internalError("unknown exception"); }
}

}  // namespace piw

// ---------------- command line: task, status, finish ----------------
namespace {
int fail(const piw::Status& s){ fprintf(stderr,"ERROR: %s\n",s.message.c_str()); return 1; }
int usage(const char* cmd){
    fprintf(stderr,"usage: parallelimariswriter %s (see src/parallel.h)\n"
        "  plan   <workdir> -F folders -P patterns [the converter's options] [--tiles X,Y,Z [--store mono|packs]\n"
        "         [--tiles-per-task N] [--chunk-bytes N]]\n"
        "  task   <workdir> <stage> <index> [--threads N] [--stage-dir DIR] [-C X,Y,Z]\n"
        "  status <workdir>\n"
        "  finish <workdir> [out.ims] [--verify] [--threads N]\n",cmd);
    return 2;
}
bool parseLong(const char* s,long& v){ char* e=NULL; errno=0; long r=strtol(s,&e,10); if(e==s||*e||errno) return false; v=r; return true; }
bool parseU64s(const char* s,uint64_t* v,int n){
    std::string t=s; size_t p=0;
    for(int i=0;i<n;i++){ size_t q=t.find(',',p); std::string x=t.substr(p,q==std::string::npos?std::string::npos:q-p);
        if(x.empty()||x.find_first_not_of("0123456789")!=std::string::npos) return false;
        v[i]=strtoull(x.c_str(),NULL,10); if(!v[i]) return false;
        if(i<n-1){ if(q==std::string::npos) return false; p=q+1; } else if(q!=std::string::npos) return false; }
    return true;
}
std::string seconds1(double s){ char b[32]; snprintf(b,sizeof b,"%.1f",s); return b; }
}  // namespace

bool isClusterCommand(const char* c){ return c&&(!strcmp(c,"task")||!strcmp(c,"status")||!strcmp(c,"finish")); }

int runCluster(int argc,char** argv){
    const std::string cmd=argc>=2?argv[1]:"";
    std::vector<std::string> pos; long threads=0; bool verify=false; piw::ClusterTaskOptions to;
    for(int i=2;i<argc;i++){ std::string a=argv[i];
        if(a=="--threads"&&cmd!="status"){ if(i+1>=argc||!parseLong(argv[i+1],threads)||threads<1) return usage(cmd.c_str()); i++; }
        else if(a=="--verify"&&cmd=="finish") verify=true;
        else if(a=="--stage-dir"&&cmd=="task"){ if(i+1>=argc) return usage(cmd.c_str()); to.stageDir=argv[++i]; }
        else if(a=="-C"&&cmd=="task"){ if(i+1>=argc||!parseU64s(argv[i+1],to.processBlock,3)) return usage(cmd.c_str()); to.hasProcessBlock=true; i++; }
        else if(a.size()>1&&a[0]=='-') return usage(cmd.c_str());
        else pos.push_back(a); }
    if(cmd=="task"){
        long index=0;
        if(pos.size()!=3||!parseLong(pos[2].c_str(),index)) return usage("task");
        to.numThreads=(int)threads;
        // a time series: the conversion's lines as the converter prints them; SIGTERM/SIGINT (a cancelled job) stop
        // it between blocks, so the staged part is removed (tile tasks keep the default: they stage nothing)
        piw::ClusterPlan p; const bool series=piw::clusterReadPlan(pos[0],p).ok()&&p.kind=="series";
        to.callbacks.message=printMessage;
        if(series){ to.callbacks.cancel=stopRequested; signal(SIGTERM,onSignal); signal(SIGINT,onSignal); }
        piw::ClusterTaskResult r; piw::Status s=piw::clusterTask(pos[0],pos[1],index,to,&r);
        if(series){ signal(SIGTERM,SIG_DFL); signal(SIGINT,SIG_DFL); }
        for(const std::string& l:r.lines) printf("%s\n",l.c_str());
        std::string what="task "+(r.stage.empty()?pos[1]:r.stage)+" "+std::to_string(index);
        if(!s.ok()) return fail(piw::Status(s.code,what+": "+s.message));
        if(r.timepoint>=0) what+=" (t="+std::to_string(r.timepoint)+", c="+std::to_string(r.channel)+")";
        if(r.skipped){ std::string o; for(size_t i=0;i<r.owners.size();i++) o+=(i?", ":"")+r.owners[i];
            printf("%s: already done (%s)\n",what.c_str(),o.c_str()); return 0; }
        if(r.timepoint>=0){
            printf("%s\n",monoPlaceLine(r.place).c_str());
            printf("%s: convert %s s (staged in %s), place %s s, total %s s\n",what.c_str(),seconds1(r.convertSeconds).c_str(),
                   r.memoryStage?"memory, /dev/shm":r.part.substr(0,r.part.find_last_of('/')).c_str(),seconds1(r.placeSeconds).c_str(),seconds1(r.totalSeconds).c_str());
        }
        else printf("%s: %zu tile%s, total %s s\n",what.c_str(),r.owners.size(),r.owners.size()==1?"":"s",seconds1(r.totalSeconds).c_str());
        return 0;
    }
    if(cmd=="status"){
        if(pos.size()!=1) return usage("status");
        piw::ClusterStatus st; piw::Status s=piw::clusterStatus(pos[0],st); if(!s.ok()) return fail(s);
        size_t missing=0;
        printf("status: %s: %s%s, %s\n",pos[0].c_str(),st.kind.c_str(),st.kind=="tiles"?(" ("+st.store+" store)").c_str():"",
               st.finished?("finished: "+st.finishedFile).c_str():st.sealed?"finishing (sealed)":"open for tasks");
        for(const piw::ClusterStageStatus& x:st.stages){ missing+=x.missing.size();
            printf("  stage %s: %ld of %ld tasks done%s%s\n",x.name.c_str(),x.tasks-(long)x.missing.size(),x.tasks,
                   x.missing.empty()?"":"; missing: ",rangesText(x.missing).c_str()); }
        return missing&&!st.finished?3:0;
    }
    if(cmd=="finish"){
        if(pos.empty()||pos.size()>2) return usage("finish");
        piw::ClusterFinishOptions fo; fo.verify=verify; fo.numThreads=(int)threads;
        piw::MonoFinishResult r; piw::Status s=piw::clusterFinish(pos[0],pos.size()>1?pos[1]:std::string(),fo,&r);
        if(!s.ok()){ fail(s); return r.missing.empty()?1:3; }
        piw::ClusterPlan p; piw::Status ps=piw::clusterReadPlan(pos[0],p);
        if(ps.ok()&&p.kind=="tiles"&&p.store=="packs")
            printf("merged %d channels x %d timepoints x %d levels -> %s (%s s)\n",r.channels,r.timepoints,r.levels,r.outputFile.c_str(),seconds1(r.totalSeconds).c_str());
        else monoPrintFinish(r,verify);
        return 0;
    }
    return usage(cmd.c_str());
}
