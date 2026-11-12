#ifndef PARALLEL_IMARIS_IMSCORE_H
#define PARALLEL_IMARIS_IMSCORE_H

// Shared .ims building blocks of the cluster path (header-only): the ported ImarisWriter block layout,
// the chunk codec (2-byte shuffle + LZ4 with ImarisWriter's 16-byte header), the pyramid box-average,
// the workdir manifest, the per-tile .pack/.hist files, and the HDF5 writers for the Imaris 5.5 metadata
// (the values from imsmeta.h, shared with the single-node converter) and histograms. Everything here returns
// its result (Status or bool; HDF5 prints its own error stack), never exits, never changes the working
// directory, and takes its options as arguments (the environment variables are only defaults, e.g.
// chunkBudgetBytes() for PIW_CHUNK_BYTES).
#include "hdf5.h"
#include <lz4.h>
#include <nlohmann/json.hpp>
#include "bpDeriche.h"
#include "imsmeta.h"
#include <vector>
#include <array>
#include <cmath>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>
#include <iomanip>
#include <functional>
#include <map>
#include <sys/stat.h>
#ifdef _WIN32
#include <direct.h>
#endif

namespace imscore {

typedef uint64_t bpSize;
typedef std::array<bpSize,3> Vec3;

// What a call returns: ok(), or a one-line message (no newline) saying what failed.
struct Status {
    bool failed=false; std::string message;
    bool ok() const { return !failed; }
    static Status fail(const std::string& m){ Status s; s.failed=true; s.message=m; return s; }
};

// ===================== ported ImarisWriter block layout =====================
struct Cost { bpSize x,y,z,t; float slice, geom, mem; };
inline void GetOptimalBlockSize(bpSize aB, bpSize sx,bpSize sy,bpSize sz,bpSize st,
    bpSize mnx,bpSize mny,bpSize mnz,bpSize mnt, bpSize& bx,bpSize& by,bpSize& bz,bpSize& bt){
    unsigned long long vB=(unsigned long long)powf(2.f,floorf(logf((float)aB)/logf(2.f)));
    unsigned long long ix=sx,iy=sy,iz=sz,it=st,ixyzt=ix*iy*iz*it; std::vector<Cost> cs;
    for(bpSize vx=1;vx<=vB;vx*=2)for(bpSize vy=1;vy*vx<=vB;vy*=2)for(bpSize vz=1;vz*vy*vx<=vB;vz*=2){
        bpSize vt=(bpSize)(vB/(vx*vy*vz));
        if((sz>1&&vx==vy&&vz>2)||(sz==1&&vx<=4*vy&&vy<=4*vx&&vz==1)){
            if(vx*vy*vz*vt==vB&&vx>=mnx&&vy>=mny&&vz>=mnz&&vt>=mnt){
                unsigned long long nx=1+(ix-1)/vx,ny=1+(iy-1)/vy,nz=1+(iz-1)/vz,nt=1+(it-1)/vt;
                Cost c{vx,vy,vz,vt,1.f+(float)(nx*ny+nx*nz+ny*nz-nx-ny-nz),(float)(vx*vy+vx*vz+vy*vz),(float)(nx*vx*ny*vy*nz*vz*nt*vt)/(float)ixyzt};
                cs.push_back(c);
            }
        }
    }
    if(!cs.empty()){ std::sort(cs.begin(),cs.end(),[](const Cost&a,const Cost&b){ if(a.slice!=b.slice)return a.slice<b.slice; if(a.geom!=b.geom)return a.geom<b.geom; return a.mem<b.mem;});
        Cost o=cs.front(); if(o.mem>2.f) for(auto&c:cs) if(c.mem<=2.f){o=c;break;} bx=o.x;by=o.y;bz=o.z;bt=o.t;
    } else { bpSize l=0; while(((bpSize)1<<l)<aB)++l; bpSize lz=l/3,ly=(l-lz)/2,lx=l-lz-ly; bz=1<<lz;by=1<<ly;bx=1<<lx;bt=1; }
}
inline std::vector<Vec3> GetOptimalBlockSizes(bpSize aB, const std::vector<Vec3>& s, bpSize st){
    std::vector<Vec3> o; for(size_t i=0;i<s.size();++i){ bpSize mnx=1,mny=1,mnz=1,mnt=1;
        if(i>0){ bool rx=(s[i-1][0]/2)==s[i][0],ry=(s[i-1][1]/2)==s[i][1],rz=(s[i-1][2]/2)==s[i][2];
            mnx=rx?o[i-1][0]/2:o[i-1][0]; mny=ry?o[i-1][1]/2:o[i-1][1]; mnz=rz?o[i-1][2]/2:o[i-1][2]; }
        bpSize bx,by,bz,bt; GetOptimalBlockSize(aB,s[i][0],s[i][1],s[i][2],st,mnx,mny,mnz,mnt,bx,by,bz,bt); o.push_back({bx,by,bz}); }
    return o;
}
inline std::vector<Vec3> GetOptimalImagePyramid(bpSize aMax, Vec3 img, bool rZ){
    std::vector<Vec3> r{img}; Vec3 s=img;
    while((unsigned long long)s[0]*s[1]*s[2]>aMax){ unsigned long long lx=s[0],ly=s[1],lz=rZ?s[2]:1;
        bool rx=lx>1&&(10*lx)*(10*lx)>ly*lz, ry=ly>1&&(10*ly)*(10*ly)>lx*lz, rz=lz>1&&(10*lz)*(10*lz)>lx*ly;
        if(!rx&&!ry&&!rz)break;
        if(rx)s[0]/=2;
        if(ry)s[1]/=2;
        if(rz)s[2]/=2;
        r.push_back(s);
    }
    return r;
}
// Chunk-size budget in BYTES. Bigger chunks = far fewer chunks = much faster combine (the combine cost
// is dominated by per-chunk overhead: ~27M tiny chunks on the real data). Trade-off: NOT byte-identical
// to the monolithic writer, and it re-tiles so the conversion must be re-run. Default 1 MiB (the
// ImarisWriter choice); override with env PIW_CHUNK_BYTES (e.g. 8388608 for ~8 MiB chunks).
inline bpSize chunkBudgetBytes(){ bpSize b=1u<<20; const char* e=getenv("PIW_CHUNK_BYTES"); if(e){ char* end=NULL; unsigned long long v=strtoull(e,&end,10); if(end!=e&&v>=4096) b=(bpSize)v; } return b; }

// ===================== chunk codec (bpShuffle 2-byte + bpLZ4) =====================
inline void shuffle2(const uint8_t* s,size_t n,uint8_t* d){ size_t h=n/2; for(size_t i=0;i<h;i++){d[i]=s[2*i];d[h+i]=s[2*i+1];} }
inline void unshuffle2(const uint8_t* s,size_t n,uint8_t* d){ size_t h=n/2; for(size_t i=0;i<h;i++){d[2*i]=s[i];d[2*i+1]=s[h+i];} }
inline void wbe(uint8_t* d,uint64_t v,int nb){ for(int i=0;i<nb;i++) d[nb-1-i]=(uint8_t)((v>>(8*i))&0xff); }
inline uint64_t rbe(const uint8_t* d,int nb){ uint64_t v=0; for(int i=0;i<nb;i++)v=(v<<8)|d[i]; return v; }
// false if LZ4 failed (or nb is beyond what one LZ4 block takes)
inline bool encodeChunk(const uint8_t* vox,size_t nb,std::vector<uint8_t>& out){
    if(nb>(size_t)LZ4_MAX_INPUT_SIZE){ out.clear(); return false; }
    std::vector<uint8_t> sh(nb); shuffle2(vox,nb,sh.data()); int bd=LZ4_compressBound((int)nb); out.resize(16+bd);
    int c=LZ4_compress_default((const char*)sh.data(),(char*)out.data()+16,(int)nb,bd);
    if(c<=0&&nb>0){ out.clear(); return false; }
    wbe(out.data(),nb,8); wbe(out.data()+8,nb,4); wbe(out.data()+12,(uint64_t)c,4); out.resize(16+c);
    return true;
}
// The LZ4 stage of decodeChunk: the chunk's still shuffled bytes into sh (expectRaw bytes, the header must agree).
inline Status decodeChunkShuffled(const uint8_t* c,size_t cs,uint8_t* sh,size_t expectRaw){
    if(cs<16) return Status::fail("chunk of "+std::to_string(cs)+" bytes is shorter than its 16-byte header");
    uint64_t t=rbe(c,8), comp=rbe(c+12,4);
    if(t!=expectRaw) return Status::fail("chunk header says "+std::to_string(t)+" raw bytes, expected "+std::to_string(expectRaw));
    if(comp!=cs-16) return Status::fail("chunk header says "+std::to_string(comp)+" compressed bytes, chunk has "+std::to_string(cs-16));
    if(t>(uint64_t)LZ4_MAX_INPUT_SIZE) return Status::fail("chunk header says "+std::to_string(t)+" raw bytes, more than one LZ4 block holds");
    int d=LZ4_decompress_safe((const char*)c+16,(char*)sh,(int)(cs-16),(int)t);
    if(d<0||(uint64_t)d!=t) return Status::fail("LZ4 could not decode a chunk (corrupt data)");
    return Status();
}
// Decode one chunk written by encodeChunk; expectRaw = the chunk's voxel bytes (the header must agree).
inline Status decodeChunk(const uint8_t* c,size_t cs,std::vector<uint8_t>& vox,size_t expectRaw){
    std::vector<uint8_t> sh(expectRaw);
    Status s=decodeChunkShuffled(c,cs,sh.data(),expectRaw); if(!s.ok()) return s;
    vox.resize(expectRaw); unshuffle2(sh.data(),expectRaw,vox.data());
    return Status();
}
// Box-average one pyramid step; dims/strides [x,y,z] (x fastest), di a multiple of st.
inline void boxDown(const std::vector<uint16_t>& in,const bpSize di[3],const bpSize st[3],std::vector<uint16_t>& out,bpSize dOut[3]){
    for(int a=0;a<3;a++) dOut[a]=di[a]/st[a];
    out.assign((size_t)dOut[0]*dOut[1]*dOut[2],0); const float dn=(float)(st[0]*st[1]*st[2]);
    for(bpSize oz=0;oz<dOut[2];oz++)for(bpSize oy=0;oy<dOut[1];oy++)for(bpSize ox=0;ox<dOut[0];ox++){ float s=0;
        for(bpSize dz=0;dz<st[2];dz++)for(bpSize dy=0;dy<st[1];dy++)for(bpSize dx=0;dx<st[0];dx++)
            s+=in[(ox*st[0]+dx)+(oy*st[1]+dy)*di[0]+(oz*st[2]+dz)*(size_t)di[0]*di[1]];
        out[ox+oy*dOut[0]+oz*(size_t)dOut[0]*dOut[1]]=(uint16_t)(s/dn); }
}

// ===================== files (portable: 64-bit offsets, atomic replace, mkdir -p) =====================
inline int fseek64(FILE* f,int64_t o,int w){
#ifdef _WIN32
    return _fseeki64(f,o,w);
#else
    return fseeko(f,(off_t)o,w);
#endif
}
inline int64_t ftell64(FILE* f){
#ifdef _WIN32
    return _ftelli64(f);
#else
    return (int64_t)ftello(f);
#endif
}
inline bool isDir(const std::string& p){ struct stat s; return stat(p.c_str(),&s)==0&&(s.st_mode&S_IFMT)==S_IFDIR; }
inline bool fileExists(const std::string& p){ struct stat s; return stat(p.c_str(),&s)==0; }
// rename that replaces an existing target (POSIX rename does; on Windows the target is removed first)
inline bool renameReplace(const std::string& from,const std::string& to){
#ifdef _WIN32
    remove(to.c_str());
#endif
    return rename(from.c_str(),to.c_str())==0;
}
// mkdir -p
inline Status makeDirs(const std::string& path){
    std::string p=path;
    for(size_t i=1;i<=p.size();++i){
        bool sep=i==p.size()||p[i]=='/'
#ifdef _WIN32
            ||p[i]=='\\'
#endif
            ;
        if(!sep) continue;
        std::string pre=p.substr(0,i);
        if(pre.empty()||isDir(pre)) continue;
#ifdef _WIN32
        if(pre.size()==2&&pre[1]==':') continue;    // drive letter
        int rc=_mkdir(pre.c_str());
#else
        int rc=mkdir(pre.c_str(),0777);
#endif
        int e=errno;
        if(rc!=0&&!(e==EEXIST&&isDir(pre))) return Status::fail("cannot create directory "+pre+": "+strerror(e));
    }
    while(p.size()>1&&(p.back()=='/'
#ifdef _WIN32
        ||p.back()=='\\'
#endif
        )) p.pop_back();
    if(!isDir(p)) return Status::fail("cannot create directory "+path);
    return Status();
}
// write p.tmp, then rename it to p: readers see the old file or the whole new one
inline Status writeFileAtomic(const std::string& p,const void* d,size_t n){
    std::string t=p+".tmp"; FILE* f=fopen(t.c_str(),"wb");
    if(!f) return Status::fail("cannot create "+t+": "+strerror(errno));
    bool ok=(n==0||fwrite(d,1,n,f)==n); int e=errno; ok=(fclose(f)==0)&&ok;
    if(!ok){ remove(t.c_str()); return Status::fail("cannot write "+t+": "+strerror(e?e:errno)); }
    if(!renameReplace(t,p)){ e=errno; remove(t.c_str()); return Status::fail("cannot rename "+t+" to "+p+": "+strerror(e)); }
    return Status();
}
inline bool readFileBytes(const std::string& p,std::vector<uint8_t>& b){
    b.clear(); FILE* f=fopen(p.c_str(),"rb"); if(!f) return false;
    int64_t n=-1; if(fseek64(f,0,SEEK_END)==0) n=ftell64(f);
    bool ok=n>=0&&fseek64(f,0,SEEK_SET)==0;
    if(ok&&n>0){ b.resize((size_t)n); ok=fread(b.data(),1,(size_t)n,f)==(size_t)n; }
    fclose(f); if(!ok) b.clear(); return ok;
}

// ===================== manifest (JSON) =====================
struct Lev { Vec3 size, block, nch, tb, nt; };   // tb = tile size in blocks; nt = tile grid
// meta = the metadata setup resolved (imsmeta::resolveMetadata of the zarrs and setup's options)
struct Manifest { std::vector<std::string> zarrs; int nC=1,nT=1; bpSize X,Y,Z,elem; float vox[3]; bpSize cap[3]={0,0,0}; std::vector<Lev> lev; imsmeta::Metadata meta; };
inline const std::string& zarrAt(const Manifest& m,int c,int t){ return m.zarrs[(size_t)c*m.nT+t]; }
inline bpSize levStride(const Manifest& m,int r,int a){ bpSize s=m.lev[r-1].size[a]/m.lev[r].size[a]; return s?s:1; }
inline long jobsAtLevel(const Manifest& m,int r){ const Lev& L=m.lev[r]; return (long)m.nC*m.nT*L.nt[0]*L.nt[1]*L.nt[2]; }
inline long tilesPerCT(const Lev& L){ return (long)L.nt[0]*L.nt[1]*L.nt[2]; }
// the level-L block range [cb0,cb1) of tile `tile` (x fastest), clipped to the chunk grid
inline void tileBlockRange(const Lev& L,long tile,bpSize cb0[3],bpSize cb1[3]){
    bpSize t[3]={(bpSize)tile%L.nt[0],((bpSize)tile/L.nt[0])%L.nt[1],(bpSize)tile/(L.nt[0]*L.nt[1])};
    for(int a=0;a<3;a++){ cb0[a]=t[a]*L.tb[a]; cb1[a]=std::min(cb0[a]+L.tb[a],L.nch[a]); }
}
inline double voxClean(float f){ char b[32]; snprintf(b,sizeof b,"%.7g",(double)f); return atof(b); }
inline double floatExact(float f){ char b[32]; snprintf(b,sizeof b,"%.9g",(double)f); return atof(b); }   // reads back to f
// The pyramid of an X*Y*Z volume of elem-byte voxels: per level its size, block (chunk) size, chunk grid, and
// the grid of cap-voxel tiles (one worker each). chunkBytes = the chunk budget, chunkBudgetBytes() by default.
inline std::vector<Lev> makeLevels(bpSize X,bpSize Y,bpSize Z,bpSize elem,const bpSize cap[3],bpSize chunkBytes){
    std::vector<Lev> lev;   // only the BLOCK budget changes with chunkBytes; the pyramid (level count) does not
    auto ls=GetOptimalImagePyramid((1024*1024)/elem,{X,Y,Z},true),bs=GetOptimalBlockSizes(chunkBytes/elem,ls,1);
    for(size_t r=0;r<ls.size();++r){ Lev L; L.size=ls[r]; L.block=bs[r];
        for(int a=0;a<3;a++){ L.nch[a]=(ls[r][a]+bs[r][a]-1)/bs[r][a]; L.tb[a]=std::max((bpSize)1,(cap[a]+bs[r][a]-1)/bs[r][a]); L.nt[a]=(L.nch[a]+L.tb[a]-1)/L.tb[a]; }
        lev.push_back(L); }
    return lev;
}
// The manifest's "metadata" object: what the finishers write (names, colors, wavelengths in nm with 0 = none,
// time stamps); setup and monoinit resolve it once, so every step and every rerun writes the same.
inline nlohmann::ordered_json metadataJson(const imsmeta::Metadata& md){
    using nlohmann::ordered_json;
    ordered_json j, names=ordered_json::array(), colors=ordered_json::array(), em=ordered_json::array(), ex=ordered_json::array(), times=ordered_json::array();
    for(size_t c=0;c<md.names.size();c++){ const piw::Color& k=md.colors[c];
        names.push_back(md.names[c]); colors.push_back({floatExact(k.red),floatExact(k.green),floatExact(k.blue)});
        em.push_back(floatExact(md.emission[c])); ex.push_back(floatExact(md.excitation[c])); }
    for(int64_t t:md.times) times.push_back(imsmeta::timeString(t));
    j["channelNames"]=names; j["channelColors"]=colors; j["emissionWavelengths"]=em; j["excitationWavelengths"]=ex; j["timePoints"]=times;
    return j;
}
// metadataJson's object back, for nC channels and nT timepoints (throws nlohmann's exceptions on a malformed one)
inline Status readMetadataJson(const nlohmann::json& md,int nC,int nT,imsmeta::Metadata& mm){
    mm=imsmeta::Metadata();
    for(auto& n:md.at("channelNames")) mm.names.push_back(n.get<std::string>());
    for(auto& k:md.at("channelColors")){ piw::Color v={k.at(0).get<float>(),k.at(1).get<float>(),k.at(2).get<float>()}; mm.colors.push_back(v); }
    for(auto& w:md.at("emissionWavelengths")) mm.emission.push_back(w.get<float>());
    for(auto& w:md.at("excitationWavelengths")) mm.excitation.push_back(w.get<float>());
    for(auto& t:md.at("timePoints")){ int64_t v; if(!imsmeta::parseTime(t.get<std::string>(),v)) return Status::fail("bad time point '"+t.get<std::string>()+"'"); mm.times.push_back(v); }
    const size_t c=(size_t)nC;
    if(mm.names.size()!=c||mm.colors.size()!=c||mm.emission.size()!=c||mm.excitation.size()!=c||mm.times.size()!=(size_t)nT)
        return Status::fail("the metadata lists do not match "+std::to_string(nC)+" channels and "+std::to_string(nT)+" timepoints");
    return Status();
}
inline Status writeManifest(const std::string& wd,const Manifest& m){
    using nlohmann::ordered_json; ordered_json j;
    long total=0; ordered_json jpl=ordered_json::array(); for(size_t r=0;r<m.lev.size();++r){ long n=jobsAtLevel(m,(int)r); jpl.push_back(n); total+=n; }
    j["jobsTotal"]=total; j["jobsPerLevel"]=jpl;       // submit one array per level: worker <wd> <r> 0..jobsPerLevel[r]-1
    j["channels"]=m.nC; j["timepoints"]=m.nT; j["levels"]=(int)m.lev.size();
    j["size"]={(unsigned long long)m.X,(unsigned long long)m.Y,(unsigned long long)m.Z}; j["elem"]=(unsigned long long)m.elem;
    j["voxel"]={voxClean(m.vox[0]),voxClean(m.vox[1]),voxClean(m.vox[2])};
    j["tile"]={(unsigned long long)m.cap[0],(unsigned long long)m.cap[1],(unsigned long long)m.cap[2]};
    ordered_json lv=ordered_json::array();
    for(const Lev& L:m.lev){ ordered_json e;
        e["size"]={(unsigned long long)L.size[0],(unsigned long long)L.size[1],(unsigned long long)L.size[2]};
        e["block"]={(unsigned long long)L.block[0],(unsigned long long)L.block[1],(unsigned long long)L.block[2]};
        e["nchunks"]={(unsigned long long)L.nch[0],(unsigned long long)L.nch[1],(unsigned long long)L.nch[2]};
        e["tileBlocks"]={(unsigned long long)L.tb[0],(unsigned long long)L.tb[1],(unsigned long long)L.tb[2]};
        e["ntiles"]={(unsigned long long)L.nt[0],(unsigned long long)L.nt[1],(unsigned long long)L.nt[2]};
        lv.push_back(e); }
    j["level"]=lv;
    ordered_json zs=ordered_json::array(); for(int c=0;c<m.nC;c++){ ordered_json ch=ordered_json::array(); for(int t=0;t<m.nT;t++) ch.push_back(zarrAt(m,c,t)); zs.push_back(ch); }
    j["zarrs"]=zs;
    j["metadata"]=metadataJson(m.meta);
    std::string s=j.dump(2)+"\n";
    return writeFileAtomic(wd+"/manifest.json",s.data(),s.size());
}
inline Status readManifest(const std::string& wd,Manifest& m){
    m=Manifest(); std::string p=wd+"/manifest.json"; std::vector<uint8_t> b;
    if(!readFileBytes(p,b)) return Status::fail("cannot read "+p+" (run setup?)");
    try{
        nlohmann::json j=nlohmann::json::parse(b.begin(),b.end());
        if(!j.contains("size")||!j.contains("level")||!j.contains("zarrs")) return Status::fail(p+" lacks size, level or zarrs");
        m.nC=j.value("channels",1); m.nT=j.value("timepoints",1); m.elem=j.value("elem",(bpSize)2);
        { auto&s=j["size"]; m.X=s[0].get<bpSize>(); m.Y=s[1].get<bpSize>(); m.Z=s[2].get<bpSize>(); }
        m.vox[0]=m.vox[1]=m.vox[2]=1.f;
        if(j.contains("voxel")){ auto&v=j["voxel"]; for(int a=0;a<3;a++) m.vox[a]=v[a].get<float>(); }
        if(j.contains("tile")){ auto&c=j["tile"]; for(int a=0;a<3;a++) m.cap[a]=c[a].get<bpSize>(); }
        for(auto& e:j["level"]){ Lev L; for(int a=0;a<3;a++){ L.size[a]=e["size"][a].get<bpSize>(); L.block[a]=e["block"][a].get<bpSize>(); L.nch[a]=e["nchunks"][a].get<bpSize>(); L.tb[a]=e["tileBlocks"][a].get<bpSize>(); L.nt[a]=e["ntiles"][a].get<bpSize>(); } m.lev.push_back(L); }
        if(m.nC<1||m.nT<1||m.lev.empty()||m.X==0||m.Y==0||m.Z==0||(m.elem!=1&&m.elem!=2&&m.elem!=4)) return Status::fail(p+" has no levels, an empty size, or an unknown element size");
        for(const Lev& L:m.lev) for(int a=0;a<3;a++) if(!L.size[a]||!L.block[a]||!L.nch[a]||!L.tb[a]||!L.nt[a]) return Status::fail(p+" has a level with a zero size, block or grid");
        m.zarrs.assign((size_t)m.nC*m.nT,std::string());
        auto&z=j["zarrs"]; if((int)z.size()!=m.nC) return Status::fail(p+": zarrs lists "+std::to_string(z.size())+" channels, expected "+std::to_string(m.nC));
        for(int c=0;c<m.nC;c++){ if((int)z[c].size()!=m.nT) return Status::fail(p+": channel "+std::to_string(c)+" lists "+std::to_string(z[c].size())+" timepoints, expected "+std::to_string(m.nT));
            for(int t=0;t<m.nT;t++) m.zarrs[(size_t)c*m.nT+t]=z[c][t].get<std::string>(); }
        if(j.contains("metadata")){ Status s=readMetadataJson(j["metadata"],m.nC,m.nT,m.meta); if(!s.ok()) return Status::fail(p+": "+s.message); }
        else{   // a manifest from before the metadata: the defaults for its zarrs
            piw::ConvertOptions o; o.files.assign(m.nC,std::vector<std::string>());
            for(int c=0;c<m.nC;c++) for(int t=0;t<m.nT;t++) o.files[c].push_back(zarrAt(m,c,t));
            std::string e=imsmeta::resolveMetadata(o,m.meta); if(!e.empty()) return Status::fail(p+": "+e);
        }
    }catch(const std::exception& e){ return Status::fail("invalid "+p+": "+e.what()); }
    return Status();
}

// ===================== per-tile .pack + partial-histogram files =====================
// One .pack file per tile holds ALL that tile's block chunks (avoids millions of tiny files on the
// shared FS). Layout: [chunk0 bytes][chunk1 bytes]...[N*(u64 cz,cy,cx,size)][u64 N] -- the index sits
// at the END so the writer streams the body and only buffers the small index.
inline std::string packName(const std::string& d,int r,int c,int t,long tile){ char b[128]; snprintf(b,sizeof b,"/L%d_C%d_T%d_%ld.pack",r,c,t,tile); return d+b; }
inline std::string histName(const std::string& d,int r,int c,int t,long tile){ char b[128]; snprintf(b,sizeof b,"/h_L%d_C%d_T%d_%ld.hist",r,c,t,tile); return d+b; }
static const size_t HIST_BYTES=65536*sizeof(uint64_t);
// Read and check a pack's index (idx = N*{cz,cy,cx,size}): the body sizes + index + count must add up to the
// file size exactly, so a truncated, padded or trailer-less pack is refused.
inline Status readPackIndex(FILE* f,const std::string& path,std::vector<uint64_t>& idx){
    idx.clear(); int64_t n=-1; if(fseek64(f,0,SEEK_END)==0) n=ftell64(f);
    uint64_t N=0;
    if(n<8||fseek64(f,n-8,SEEK_SET)!=0||fread(&N,8,1,f)!=1) return Status::fail(path+": truncated pack (no trailer)");
    if(N==0||N>(uint64_t)(n-8)/32) return Status::fail(path+": bad pack trailer (chunk count "+std::to_string(N)+")");
    idx.resize(N*4);
    if(fseek64(f,n-8-(int64_t)(N*32),SEEK_SET)!=0||fread(idx.data(),8,N*4,f)!=N*4) return Status::fail(path+": cannot read the pack index");
    uint64_t body=0; for(uint64_t i=0;i<N;i++){ uint64_t sz=idx[i*4+3]; if(sz<16||sz>(uint64_t)n) return Status::fail(path+": bad chunk size in the pack index"); body+=sz; }
    if(body+N*32+8!=(uint64_t)n) return Status::fail(path+": pack size does not match its index (truncated or corrupt)");
    return Status();
}
inline Status readPackIndex(const std::string& path,std::vector<uint64_t>& idx){
    FILE* f=fopen(path.c_str(),"rb"); if(!f){ idx.clear(); return Status::fail("missing pack "+path); }
    Status s=readPackIndex(f,path,idx); fclose(f); return s;
}
// The index must hold exactly the blocks of tile `tile` of level L, each once.
inline Status checkPackTile(const std::vector<uint64_t>& idx,const Lev& L,long tile,const std::string& path){
    bpSize cb0[3],cb1[3]; tileBlockRange(L,tile,cb0,cb1);
    uint64_t want=(cb1[0]-cb0[0])*(cb1[1]-cb0[1])*(cb1[2]-cb0[2]), N=idx.size()/4;
    if(N!=want) return Status::fail(path+": "+std::to_string(N)+" chunks, the tile has "+std::to_string(want));
    std::vector<bool> seen(want,false);
    for(uint64_t i=0;i<N;i++){ bpSize cz=idx[i*4],cy=idx[i*4+1],cx=idx[i*4+2];
        if(cx<cb0[0]||cx>=cb1[0]||cy<cb0[1]||cy>=cb1[1]||cz<cb0[2]||cz>=cb1[2]) return Status::fail(path+": chunk outside its tile");
        uint64_t k=(cx-cb0[0])+(cy-cb0[1])*(cb1[0]-cb0[0])+(cz-cb0[2])*(cb1[0]-cb0[0])*(cb1[1]-cb0[1]);
        if(seen[k]) return Status::fail(path+": chunk listed twice");
        seen[k]=true; }
    return Status();
}
typedef std::function<Status(bpSize cz,bpSize cy,bpSize cx,const uint8_t* bytes,size_t size)> PackChunkFn;
// Call cb for every chunk of a pack, in file order; stops at the first failure (a bad pack or cb's own).
// With tileLev, the pack must also hold exactly the chunks of tile `tile` of that level (checkPackTile).
inline Status forEachPackedChunk(const std::string& path,const PackChunkFn& cb,const Lev* tileLev=NULL,long tile=0){
    FILE* f=fopen(path.c_str(),"rb"); if(!f) return Status::fail("missing pack "+path);
    std::vector<uint64_t> idx; Status s=readPackIndex(f,path,idx);
    if(s.ok()&&tileLev) s=checkPackTile(idx,*tileLev,tile,path);
    if(s.ok()&&fseek64(f,0,SEEK_SET)!=0) s=Status::fail(path+": cannot seek");
    std::vector<uint8_t> buf;
    for(uint64_t i=0;s.ok()&&i<idx.size()/4;i++){ uint64_t sz=idx[i*4+3]; buf.resize(sz);
        if(fread(buf.data(),1,sz,f)!=sz){ s=Status::fail(path+": short read"); break; }
        s=cb(idx[i*4],idx[i*4+1],idx[i*4+2],buf.data(),sz); }
    fclose(f); return s;
}
// sum = the exact 65536-bin histogram of (r,c,t): the sum of its tiles' partial histograms (all must exist)
inline Status sumPartialHists(const Manifest& m,const std::string& cd,int r,int c,int t,std::vector<uint64_t>& sum){
    const Lev& L=m.lev[r]; long perCT=tilesPerCT(L);
    sum.assign(65536,0); std::vector<uint8_t> b;
    for(long ti=0;ti<perCT;ti++){ std::string p=histName(cd,r,c,t,ti);
        if(!readFileBytes(p,b)) return Status::fail("missing histogram "+p);
        if(b.size()!=HIST_BYTES) return Status::fail(p+": "+std::to_string(b.size())+" bytes, expected "+std::to_string(HIST_BYTES));
        const uint64_t* h=(const uint64_t*)b.data(); for(int i=0;i<65536;i++) sum[i]+=h[i]; }
    return Status();
}

// ===================== HDF5 helpers =====================
// Turns HDF5's automatic error printing off for one scope, around probes that may fail by design
// (H5Lexists on a missing path, opening an optional file); restores the previous handler afterwards.
class H5Quiet {
    H5E_auto2_t f_=NULL; void* d_=NULL;
public:
    H5Quiet(){ H5Eget_auto2(H5E_DEFAULT,&f_,&d_); H5Eset_auto2(H5E_DEFAULT,NULL,NULL); }
    ~H5Quiet(){ H5Eset_auto2(H5E_DEFAULT,f_,d_); }
    H5Quiet(const H5Quiet&)=delete; H5Quiet& operator=(const H5Quiet&)=delete;
};
// true if every component of an HDF5 path exists (H5Lexists alone fails when a parent group is missing)
inline bool h5PathExists(hid_t f,const std::string& path){ H5Quiet q; size_t p=0;
    while((p=path.find('/',p+1))!=std::string::npos) if(H5Lexists(f,path.substr(0,p).c_str(),H5P_DEFAULT)<=0) return false;
    return H5Lexists(f,path.c_str(),H5P_DEFAULT)>0; }
static const int FID=32004;
inline size_t lz4_noop(unsigned,size_t,const unsigned int*,size_t,size_t*,void**){return 0;}
// The LZ4 filter id is registered with a no-op body: chunks are always written/read raw (H5D*_chunk).
inline bool registerLZ4(){
    static const H5Z_class2_t H5Z_LZ4[1]={{H5Z_CLASS_T_VERS,(H5Z_filter_t)FID,1,1,"HDF5 lz4 filter; see http://www.hdfgroup.org/services/contributions.html",NULL,NULL,(H5Z_func_t)lz4_noop}};
    htri_t avail; { H5Quiet q; avail=H5Zfilter_avail(FID); }
    return avail>0||H5Zregister(H5Z_LZ4)>=0;
}
inline bool setLZ4(hid_t d){ unsigned bs=1u<<30; return H5Pset_filter(d,FID,H5Z_FLAG_MANDATORY,1,&bs)>=0; }
inline bool setStrAttr(hid_t o,const char* n,const std::string& v){
    hsize_t dim=v.size()?v.size():1; hid_t sp=H5Screate_simple(1,&dim,NULL),t=H5Tcopy(H5T_C_S1); bool ok=sp>=0&&t>=0;
    hid_t a=ok?H5Acreate2(o,n,t,sp,H5P_DEFAULT,H5P_DEFAULT):-1; ok=a>=0&&H5Awrite(a,t,v.size()?v.data():"")>=0;
    if(a>=0)H5Aclose(a);
    if(t>=0)H5Tclose(t);
    if(sp>=0)H5Sclose(sp);
    return ok; }
// an Imaris string attribute (1-D array of 1-byte strings, no NUL); "" if absent
inline std::string readStrAttr(hid_t o,const char* n){ if(H5Aexists(o,n)<=0) return std::string();
    hid_t a=H5Aopen(o,n,H5P_DEFAULT); if(a<0) return std::string(); hid_t t=H5Aget_type(a),s=H5Aget_space(a); hssize_t np=H5Sget_simple_extent_npoints(s);
    std::vector<char> b((size_t)(np>0?np:1)+1,0); H5Aread(a,t,b.data()); H5Aclose(a);H5Tclose(t);H5Sclose(s); return std::string(b.data()); }
inline std::string fstr(float v,int p){ std::ostringstream o; o.setf(std::ios::fixed); o<<std::setprecision(p)<<v; return o.str(); }
inline bool writeU64(hid_t g,const char* n,const std::vector<uint64_t>& v){
    hsize_t d=v.size(); hid_t sp=H5Screate_simple(1,&d,NULL); if(sp<0) return false;
    hid_t ds=H5Dcreate2(g,n,H5T_STD_U64LE,sp,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT); bool ok=ds>=0&&H5Dwrite(ds,H5T_NATIVE_UINT64,H5S_ALL,H5S_ALL,H5P_DEFAULT,v.data())>=0;
    if(ds>=0)H5Dclose(ds);
    H5Sclose(sp); return ok; }
inline hid_t imsCreate(const std::string& path){ hid_t fapl=H5Pcreate(H5P_FILE_ACCESS); H5Pset_libver_bounds(fapl,H5F_LIBVER_V18,H5F_LIBVER_V18); hid_t f=H5Fcreate(path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl); return f; }
inline hid_t imsDtype(bpSize elem){ return elem==2?H5Tcopy(H5T_STD_U16LE):(elem==1?H5Tcopy(H5T_STD_U8LE):H5Tcopy(H5T_IEEE_F32LE)); }
// DataSetInfo/Channel Color as ImarisWriter writes it (bpConvColorToString)
inline std::string imsColorString(const piw::Color& k){ return fstr(k.red,3)+" "+fstr(k.green,3)+" "+fstr(k.blue,3); }
// The per-channel attributes of imsmeta's metadata (Name, the wavelengths that were given, Color) on a
// DataSetInfo/Channel group, for source channel c; an attribute already there is replaced, and a
// wavelength attribute without a value is removed (linkparts edits a copied channel group with this).
inline bool setChannelMeta(hid_t ch,const imsmeta::Metadata& md,int c){
    auto put=[&](const char* n,const std::string& v){ if(H5Aexists(ch,n)>0&&H5Adelete(ch,n)<0) return false; return setStrAttr(ch,n,v); };
    auto wave=[&](const char* n,float nm){ if(nm>0) return put(n,imsmeta::wavelengthString(nm)); return !(H5Aexists(ch,n)>0)||H5Adelete(ch,n)>=0; };
    bool ok=put("Name",md.names[c]); ok&=wave("LSMEmissionWavelength",md.emission[c]); ok&=wave("LSMExcitationWavelength",md.excitation[c]);
    ok&=put("Color",imsColorString(md.colors[c])); return ok;
}
// The DataSetInfo/TimeInfo attributes for the source timepoints tps (TimePoint1.. = their time stamps), by name
inline std::map<std::string,std::string> timeInfoAttrs(const imsmeta::Metadata& md,const std::vector<int>& tps){
    std::map<std::string,std::string> ti;
    ti["FileTimePoints"]=std::to_string(tps.size()); ti["DatasetTimePoints"]=std::to_string(tps.size());
    for(size_t i=0;i<tps.size();i++) ti["TimePoint"+std::to_string(i+1)]=imsmeta::timeString(md.times[tps[i]]);
    return ti;
}
// DataSetInfo/TimeInfo for the source timepoints tps, attributes in ImarisWriter's (name) order
inline bool writeTimeInfo(hid_t out,const imsmeta::Metadata& md,hid_t lcpl,const std::vector<int>& tps){
    hid_t ti=H5Gcreate2(out,"DataSetInfo/TimeInfo",lcpl,H5P_DEFAULT,H5P_DEFAULT); if(ti<0) return false;
    bool ok=true; for(const auto& a:timeInfoAttrs(md,tps)) ok&=setStrAttr(ti,a.first.c_str(),a.second);
    return H5Gclose(ti)>=0&&ok;
}

// ===================== histograms + color range =====================
// trim a 65536-bin histogram like bpHistogramBuilder<uint16>::GetHistogram (min 0, keep >=257 span). Written in
// bpWriterHDF5::WriteHistogramImpl's order (Min, Max, then the dataset; 256 bins, then 1024): the group's object
// header then stores its links and attributes in the same order as a single-node file.
inline bool writeHistogramFromBins(hid_t g,const std::vector<uint64_t>& bins){
    bool any=false; for(uint64_t v:bins){ if(v){any=true;break;} } if(!any)return true;
    uint64_t vMax=65535; while(256<vMax&&bins[vMax]==0)--vMax; uint64_t nb=vMax+1; std::vector<uint64_t> tr(bins.begin(),bins.begin()+nb);
    auto rs=[](const std::vector<uint64_t>& in,uint64_t nN){ std::vector<uint64_t> o(nN,0); uint64_t on=in.size(); for(uint64_t i=0;i<on;i++) o[i*nN/on]+=in[i]; return o; };
    std::vector<uint64_t> h1024=nb>1024?rs(tr,1024):tr, h256=rs(h1024,256); std::string mn="0.000",mx=fstr((float)vMax,3);
    bool ok=setStrAttr(g,"HistogramMin",mn); ok&=setStrAttr(g,"HistogramMax",mx); ok&=writeU64(g,"Histogram",h256);
    if(h1024.size()!=256){ ok&=setStrAttr(g,"HistogramMin1024",mn); ok&=setStrAttr(g,"HistogramMax1024",mx); ok&=writeU64(g,"Histogram1024",h1024); }
    return ok;
}
// per-channel auto ColorRange from the (exact, summed) level-0 histogram
struct ColorHist { std::vector<uint64_t> bins; float hmin,hmax; };
inline void adjustColorRange(const ColorHist& h,float& rMin,float& rMax){
    bpSize vSize=h.bins.size(); if(vSize==0){rMin=0;rMax=0;return;}
    std::vector<float> vOriginal(vSize),vBins(vSize); for(bpSize i=0;i<vSize;i++) vOriginal[i]=(float)h.bins[i];
    bpDeriche::FilterGauss(vOriginal.data(),vBins.data(),vSize,5.0f*vSize/256);
    float vPrevious=-1.0f; bpSize vFirstModeBinId=0;
    for(bpSize b=0;b+1<vSize;++b){ if(vBins[b]>vPrevious&&vBins[b]>vBins[b+1]){ vFirstModeBinId=b; break; } vPrevious=vBins[b]; }
    bpSize vHighPercentileBinId=vSize-1; uint64_t vTotalVolumeSum=0; for(bpSize b=0;b<vSize;b++) vTotalVolumeSum+=vBins[b];
    uint64_t vVolumeSum=0; for(bpSize b=0;b<vSize;b++){ vVolumeSum+=vBins[b]; if(vTotalVolumeSum&&vVolumeSum/(float)vTotalVolumeSum>0.998f){ vHighPercentileBinId=b; break; } }
    float vMin=h.hmin+(h.hmax-h.hmin)*vFirstModeBinId/vSize, vMax=h.hmin+(h.hmax-h.hmin)*vHighPercentileBinId/vSize;
    vMax=vMax+(vMax-vMin)*0.2f; vMax=(float)static_cast<int32_t>(std::min(vMax,h.hmax)); rMin=vMin; rMax=vMax;
}
// The level-0 channel histogram used for the color range (bpMultiresolutionImsImage::
// GetChannelHistogram): single timepoint -> GetHistogram(1024) of the trimmed histogram;
// many timepoints -> each timepoint's trimmed 65536-bin histogram remapped (GetBin of
// GetBinValue) into a common [0,maxMax] layout, then resampled to 1024. perTp[t] is the
// full 65536-bin level-0 histogram for timepoint t (bin i = count of value i).
inline ColorHist channelColorHistMulti(const std::vector<std::vector<uint64_t>>& perTp){
    std::vector<std::vector<uint64_t>> hs; std::vector<uint32_t> vmaxs; uint32_t gMax=0;
    for(const auto& bins:perTp){ uint32_t vMax=65535; while(256u<vMax&&bins[vMax]==0)--vMax; hs.emplace_back(bins.begin(),bins.begin()+vMax+1); vmaxs.push_back(vMax); if(vMax>gMax)gMax=vMax; }
    ColorHist h; h.hmin=0.f; h.hmax=(float)gMax;
    if(hs.empty()){ h.bins.assign(256,0); return h; }
    if(hs.size()==1){ uint64_t nb=hs[0].size(); if(nb>1024){ std::vector<uint64_t> r(1024,0); for(uint64_t i=0;i<nb;i++) r[i*1024/nb]+=hs[0][i]; h.bins=std::move(r);} else h.bins=std::move(hs[0]); return h; }
    std::vector<uint64_t> layout(65536,0);
    for(size_t t=0;t<hs.size();++t){ uint32_t vMax=vmaxs[t]; bpSize sz=hs[t].size();
        for(bpSize b=0;b<sz;b++){ float val=(float)vMax*b/(float)sz; bpSize dest= val<=0.f?0:(val>=(float)gMax?65535:(bpSize)(val*65536.f/((float)gMax+1.f))); layout[dest]+=hs[t][b]; } }
    std::vector<uint64_t> r(1024,0); for(uint64_t i=0;i<65536;i++) r[i*1024/65536]+=layout[i]; h.bins=std::move(r);
    return h;
}

// ===================== Imaris metadata =====================
// The full 65536-bin histogram of (level r, source channel c, source timepoint t), wherever a writer keeps it.
typedef std::function<Status(int r,int c,int t,std::vector<uint64_t>& bins)> LevelHistFn;
// The metadata is written in ImarisWriter's order, so that even listings in native (storage) order match a
// single-node file: writeImsRoot (bpWriterHDF5's constructor, then /DataSet with the first data block), the
// /DataSet tree (each level's groups in increasing t, then c), then writeImsDataSetInfo (WriteMetadata, after
// the data), then the thumbnail. writeImsMeta writes both parts at once, before the data.
// Root attributes + the empty /DataSet group.
inline Status writeImsRoot(hid_t out,hid_t lcpl){
    const Status bad=Status::fail("cannot write the Imaris metadata (HDF5 error above)");
    bool ok=setStrAttr(out,"ImarisDataSet","ImarisDataSet"); ok&=setStrAttr(out,"ImarisVersion","5.5.0"); ok&=setStrAttr(out,"DataSetInfoDirectoryName","DataSetInfo"); ok&=setStrAttr(out,"ThumbnailDirectoryName","Thumbnail"); ok&=setStrAttr(out,"DataSetDirectoryName","DataSet");
    { uint32_t one=1; hsize_t d=1; hid_t sp=H5Screate_simple(1,&d,NULL); hid_t a=sp<0?-1:H5Acreate2(out,"NumberOfDataSets",H5T_STD_U32LE,sp,H5P_DEFAULT,H5P_DEFAULT); ok&=a>=0&&H5Awrite(a,H5T_NATIVE_UINT32,&one)>=0; if(a>=0)H5Aclose(a); if(sp>=0)H5Sclose(sp); }
    if(!ok) return bad;
    { hid_t g=H5Gcreate2(out,"DataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); if(g<0) return bad; H5Gclose(g); }
    return Status();
}
// The full DataSetInfo (Channels w/ ColorRange, Image, ImarisDataSet, Log, TimeInfo): its sections and each
// section's attributes sorted by name, as bpWriterHDF5::WriteMetadata walks its std::maps. The values are m.meta's,
// as the single-node converter writes them. srcCh = source channel indices written as Channel 0,1,... (name, color
// and wavelengths from the SOURCE index); srcTps = source timepoints, aggregated for each channel's ColorRange (from
// their level-0 histograms, hist(0,c,t)) and written as TimeInfo TimePoint1,2,... (no value is longer than the 1024
// characters above which ImarisWriter writes a string as a dataset: imsmeta's names are at most that long).
inline Status writeImsDataSetInfo(hid_t out,const Manifest& m,hid_t lcpl,const std::vector<int>& srcCh,
                                  const std::vector<int>& srcTps,const LevelHistFn& hist){
    const Status bad=Status::fail("cannot write the Imaris metadata (HDF5 error above)");
    bool ok=true;
    std::map<std::string,std::map<std::string,std::string> > sec;   // section -> attribute -> value
    bpSize X=m.X,Y=m.Y,Z=m.Z;
    std::map<std::string,std::string>& img=sec["Image"];
    img["Name"]="(name not specified)"; img["Description"]="(description not specified)";
    img["ExtMin0"]=fstr(0.f,9); img["ExtMin1"]=fstr(0.f,9); img["ExtMin2"]=fstr(0.f,9);
    img["ExtMax0"]=fstr((float)X*m.vox[0],9); img["ExtMax1"]=fstr((float)Y*m.vox[1],9); img["ExtMax2"]=fstr((float)Z*m.vox[2],9);
    img["X"]=std::to_string(X); img["Y"]=std::to_string(Y); img["Z"]=std::to_string(Z); img["Unit"]="um";
    img["ResampleDimensionX"]="true"; img["ResampleDimensionY"]="true"; img["ResampleDimensionZ"]="true";
    img["RecordingDate"]=srcTps.empty()?std::string():imsmeta::timeString(m.meta.times[srcTps[0]]);
    for(size_t oc=0;oc<srcCh.size();oc++){ int c=srcCh[oc];
        std::vector<std::vector<uint64_t>> perTp;
        for(int t:srcTps){ perTp.emplace_back(); Status s=hist(0,c,t,perTp.back()); if(!s.ok()) return s; }
        float rMin=0,rMax=0; adjustColorRange(channelColorHistMulti(perTp),rMin,rMax);
        std::map<std::string,std::string>& ch=sec["Channel "+std::to_string(oc)];
        ch["Name"]=m.meta.names[c]; ch["Description"]="(description not specified)";
        if(m.meta.emission[c]>0) ch["LSMEmissionWavelength"]=imsmeta::wavelengthString(m.meta.emission[c]);
        if(m.meta.excitation[c]>0) ch["LSMExcitationWavelength"]=imsmeta::wavelengthString(m.meta.excitation[c]);
        ch["ColorMode"]="BaseColor"; ch["Color"]=imsColorString(m.meta.colors[c]);
        ch["ColorRange"]=fstr(rMin,3)+" "+fstr(rMax,3); ch["ColorOpacity"]="0.000"; ch["GammaCorrection"]="1.000";
    }
    sec["TimeInfo"]=timeInfoAttrs(m.meta,srcTps);
    std::map<std::string,std::string>& ids=sec["ImarisDataSet"];
    ids["NumberOfImages"]="1"; ids["Creator"]=imsmeta::creatorName(); ids["Version"]=piw::version();
    sec["Log"]["Entries"]="0";
    for(const auto& s:sec){
        hid_t g=H5Gcreate2(out,("DataSetInfo/"+s.first).c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT); if(g<0) return bad;
        for(const auto& a:s.second) ok&=setStrAttr(g,a.first.c_str(),a.second);
        H5Gclose(g);
        if(!ok) return bad;
    }
    return Status();
}
// Root attributes, /DataSet and /DataSetInfo at once, with the histograms summed from the per-tile partial
// histograms in cd (the tile path).
inline Status writeImsMeta(hid_t out,const Manifest& m,const std::string& cd,hid_t lcpl,
                           const std::vector<int>& srcCh,const std::vector<int>& srcTps){
    Status s=writeImsRoot(out,lcpl); if(!s.ok()) return s;
    return writeImsDataSetInfo(out,m,lcpl,srcCh,srcTps,[&](int r,int c,int t,std::vector<uint64_t>& bins){ return sumPartialHists(m,cd,r,c,t,bins); });
}

} // namespace imscore

#endif
