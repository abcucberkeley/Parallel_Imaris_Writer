// Parallel .ims tool (self-contained, multi-channel + multi-timepoint). Work is split by
// (channel, timepoint, spatial tile, pyramid level): many workers can share one volume.
// Each worker encodes the chunks of its tile at one level (level 0 from the zarr, coarser levels by
// downsampling the finer level's chunks -- so LEVELS RUN IN ORDER) plus a partial histogram, and stores
// them as the manifest says: "mono" (the default) writes them straight into the one .ims being built (at a
// byte range it claims, with a sidecar; monitor/monofinish then index them, see mono.h), "packs" writes one
// .pack file per tile, which a parallel merge (OpenMP read, single HDF5 writer) assembles into ONE .ims.
// Either way the .ims is byte-identical to a monolithic conversion. The subcommands are listed in
// parallel.h; the shared .ims building blocks are in imscore.h. Every failure prints a message on
// stderr and gives a non-zero exit code.
#include "imscore.h"
#include "imsthumb.h"
#include "mono.h"
#include "monoclaim.h"
#include "monoidx.h"
#include "zarr.h"
#include "parallelreadzarr.h"
#include <atomic>
#include <cerrno>
#include <chrono>
#include <ctime>
#include <memory>
#include <mutex>
#include <dirent.h>
#include <unistd.h>
#ifndef _WIN32
#include <fcntl.h>
#endif
#include <omp.h>
#include "parallel.h"

using namespace imscore;

static int fail(const std::string& msg){ fprintf(stderr,"ERROR: %s\n",msg.c_str()); return 1; }
static int fail(const Status& s){ return fail(s.message); }
static int usage(const char* u){ fprintf(stderr,"usage: parallelimariswriter %s  (see parallel.h)\n",u); return 2; }
static const char* UINT16_ONLY="the tile workers and the pack-based combine (worker/monitor/combineone/link) take uint16 data only (the single-file store: uint8 too)";
static const char* PACKS_ONLY="combineone and link read the packs of setup --store packs";
static bool parseLong(const char* s,long& v){ char* e=NULL; errno=0; long r=strtol(s,&e,10); if(e==s||*e||errno) return false; v=r; return true; }
static std::string dirOf(const std::string& p){ size_t s=p.find_last_of("/"
#ifdef _WIN32
    "\\"
#endif
    ); return s==std::string::npos?std::string("."):(s==0?std::string("/"):p.substr(0,s)); }

// ===================== worker: one (channel, timepoint, tile) at one level =====================
static double now(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static std::string fmt1(double v){ char b[32]; snprintf(b,sizeof b,"%.1f",v); return b; }
#ifdef _WIN32
static const char* NOSUP_MONO="the single-file store is not supported on this platform (use setup --store packs)";
#endif
// Whether a manifest's data can go through the tile workers: uint16, and uint8 in the single-file store.
static Status tileTypeOk(const Manifest& m,const std::string& wd){
    if(m.dtype=="uint16"||(m.dtype=="uint8"&&m.store=="mono")) return Status();
    return Status::fail(std::string(UINT16_ONLY)+"; "+wd+" holds "+m.dtype+" data"+(m.dtype=="uint8"?" in the packs store":""));
}
// Voxel buffers from calloc/malloc (the reader's buffers are malloc'd too): calloc'd pages read as zero without
// a pass that writes them. T: uint8_t or uint16_t, the manifest's sample type.
template<class T> using Vox=std::unique_ptr<T,void(*)(void*)>;
template<class T> static Vox<T> newVox(size_t n){ return Vox<T>((T*)calloc(n?n:1,sizeof(T)),free); }
// Voxel i of a decoded chunk of nb voxels, from its still shuffled bytes sh (uint16: byte 0 of every voxel first,
// then byte 1; uint8: not shuffled).
template<class T> static inline unsigned voxAt(const uint8_t* sh,size_t nb,size_t i);
template<> inline unsigned voxAt<uint16_t>(const uint8_t* sh,size_t nb,size_t i){ return (unsigned)sh[i]|((unsigned)sh[nb+i]<<8); }
template<> inline unsigned voxAt<uint8_t>(const uint8_t* sh,size_t,size_t i){ return sh[i]; }

// Decodes chunk (cx,cy,cz) of level F from its shuffled bytes sh straight into the region [fo, fo+fn) of F (x
// fastest): the voxels inside the region and inside F's image; nothing else of the region is touched.
template<class T> static void scatterChunk(const uint8_t* sh,const Lev& F,bpSize cx,bpSize cy,bpSize cz,const bpSize fo[3],const bpSize fn[3],T* fine){
    const bpSize b[3]={F.block[0],F.block[1],F.block[2]}, c0[3]={cx*b[0],cy*b[1],cz*b[2]};
    bpSize lo[3],hi[3];
    for(int a=0;a<3;a++){ lo[a]=std::max(c0[a],fo[a]); hi[a]=std::min(std::min(c0[a]+b[a],fo[a]+fn[a]),F.size[a]); if(lo[a]>=hi[a]) return; }
    const size_t nb=(size_t)(b[0]*b[1]*b[2]), nx=(size_t)(hi[0]-lo[0]);
    for(bpSize z=lo[2];z<hi[2];z++) for(bpSize y=lo[1];y<hi[1];y++){
        const size_t i0=(size_t)((lo[0]-c0[0])+(y-c0[1])*b[0]+(z-c0[2])*b[0]*b[1]);
        T* d=fine+(size_t)((lo[0]-fo[0])+(y-fo[1])*fn[0]+(z-fo[2])*fn[0]*fn[1]);
        for(size_t x=0;x<nx;x++) d[x]=(T)voxAt<T>(sh,nb,i0+x);
    }
}
// Box-averages chunk (cx,cy,cz) of level F, from its shuffled bytes sh, straight into the coarse tile: tile voxel o
// (td voxels per axis, x fastest) is the average of the finer voxels [fo + o*st, fo + (o+1)*st), zero beyond F's
// image -- what boxDown gives for the region [fo, fo+fn) of F. Each box lies inside one chunk when every reduced
// axis (st 2) has an even block size (downInChunks); chunks write disjoint tile voxels.
static bool downInChunks(const Lev& F,const bpSize st[3]){
    for(int a=0;a<3;a++) if(st[a]>2||(st[a]==2&&F.block[a]%2)) return false;
    return true;
}
template<class T> static void downChunk(const uint8_t* sh,const Lev& F,bpSize cx,bpSize cy,bpSize cz,const bpSize fo[3],const bpSize fn[3],const bpSize st[3],
                      const bpSize td[3],T* tile){
    const bpSize b[3]={F.block[0],F.block[1],F.block[2]}, c0[3]={cx*b[0],cy*b[1],cz*b[2]};
    bpSize olo[3],ohi[3]; bool inside=true;
    for(int a=0;a<3;a++){ const bpSize lo=std::max(c0[a],fo[a]), hi=std::min(c0[a]+b[a],fo[a]+fn[a]); if(lo>=hi) return;
        olo[a]=(lo-fo[a])/st[a]; ohi[a]=(hi-fo[a])/st[a]; inside=inside&&c0[a]+b[a]<=F.size[a]; }
    const size_t nb=(size_t)(b[0]*b[1]*b[2]);
    const unsigned shift=(unsigned)((st[0]-1)+(st[1]-1)+(st[2]-1));
    size_t off[8]; int n=0;   // the box's voxels relative to its first one, in the chunk
    for(bpSize dz=0;dz<st[2];dz++) for(bpSize dy=0;dy<st[1];dy++) for(bpSize dx=0;dx<st[0];dx++) off[n++]=(size_t)(dx+dy*b[0]+dz*b[0]*b[1]);
    for(bpSize oz=olo[2];oz<ohi[2];oz++) for(bpSize oy=olo[1];oy<ohi[1];oy++){
        const bpSize fz=fo[2]+oz*st[2], fy=fo[1]+oy*st[1];
        T* out=tile+(size_t)(oy*td[0]+oz*td[0]*td[1]);
        for(bpSize ox=olo[0];ox<ohi[0];ox++){
            const bpSize fx=fo[0]+ox*st[0]; const size_t i0=(size_t)((fx-c0[0])+(fy-c0[1])*b[0]+(fz-c0[2])*b[0]*b[1]);
            uint32_t sum=0;
            if(inside) for(int k=0;k<n;k++) sum+=voxAt<T>(sh,nb,i0+off[k]);
            else for(bpSize dz=0;dz<st[2];dz++) for(bpSize dy=0;dy<st[1];dy++) for(bpSize dx=0;dx<st[0];dx++)
                if(fx+dx<F.size[0]&&fy+dy<F.size[1]&&fz+dz<F.size[2]){ const size_t i=i0+(size_t)(dx+dy*b[0]+dz*b[0]*b[1]); sum+=voxAt<T>(sh,nb,i); }
            out[ox]=(T)(sum>>shift);
        }
    }
}
// What assembling does with each decoded finer chunk (its shuffled bytes): scatterChunk or downChunk.
typedef std::function<void(const uint8_t* sh,bpSize cx,bpSize cy,bpSize cz)> ChunkSink;
// The finer level F's blocks [cb0, cb1) under the region [fo, fo+fn), and the tiles that hold them.
static std::vector<long> finerTiles(const Lev& F,const bpSize fo[3],const bpSize fn[3],bpSize cb0[3],bpSize cb1[3]){
    bpSize ft0[3],ft1[3];
    for(int a=0;a<3;a++){ cb0[a]=fo[a]/F.block[a]; cb1[a]=std::min((fo[a]+fn[a]-1)/F.block[a]+1,F.nch[a]); ft0[a]=cb0[a]/F.tb[a]; ft1[a]=std::min((cb1[a]-1)/F.tb[a],F.nt[a]-1); }
    std::vector<long> v;
    for(bpSize z=ft0[2];z<=ft1[2];z++) for(bpSize y=ft0[1];y<=ft1[1];y++) for(bpSize x=ft0[0];x<=ft1[0];x++) v.push_back((long)(x+y*F.nt[0]+z*F.nt[0]*F.nt[1]));
    return v;
}
static bool blockIn(bpSize cx,bpSize cy,bpSize cz,const bpSize cb0[3],const bpSize cb1[3]){
    return cx>=cb0[0]&&cx<cb1[0]&&cy>=cb0[1]&&cy<cb1[1]&&cz>=cb0[2]&&cz<cb1[2]; }
static void firstError(std::atomic<bool>& failed,std::string& err,const std::string& msg){
    #pragma omp critical(piw_err)
    { if(!failed.load()){ err=msg; failed=true; } }
}

// packs store: every chunk of level fr under the region [fo, fo+fn), from the finer tiles' packs, decoded and
// handed to put (from several threads).
static Status assembleFromPacks(const Manifest& m,const std::string& cd,int fr,int c,int t,const bpSize fo[3],const bpSize fn[3],const ChunkSink& put){
    const Lev& F=m.lev[fr]; bpSize cb0[3],cb1[3]; const std::vector<long> tiles=finerTiles(F,fo,fn,cb0,cb1);
    const size_t raw=(size_t)(F.block[0]*F.block[1]*F.block[2])*m.elem;
    std::atomic<bool> failed(false); std::string err;
    #pragma omp parallel for schedule(dynamic)
    for(long i=0;i<(long)tiles.size();i++){
        if(failed.load()) continue;
        std::vector<uint8_t> sh(raw); const std::string pp=packName(cd,fr,c,t,tiles[(size_t)i]);
        Status s=forEachPackedChunk(pp,[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz)->Status{
            if(!blockIn(cx,cy,cz,cb0,cb1)) return Status();
            Status d=decodeChunkShuffled(by,sz,sh.data(),raw); if(!d.ok()) return Status::fail(pp+": "+d.message);
            put(sh.data(),cx,cy,cz); return Status();
        },&F,tiles[(size_t)i]);
        if(!s.ok()) firstError(failed,err,s.message);
    }
    if(failed) return Status::fail("level "+std::to_string(fr+1)+" needs level "+std::to_string(fr)+"'s packs (run its workers first): "+err);
    return Status();
}

#ifndef _WIN32
// The sidecar code of the manifest's sample type
static unsigned monoDtype(const Manifest& m){ return m.dtype=="uint8"?MONO_U8:MONO_U16; }
// The single file, for reading: O_DIRECT where the filesystem takes it (probed: some refuse only the reads),
// macOS F_NOCACHE, else buffered. Reads need 4096-aligned offsets, sizes and buffers.
struct MonoReader {
    int fd=-1; bool direct=false;
    MonoReader(){}
    ~MonoReader(){ if(fd>=0) close(fd); }
    MonoReader(const MonoReader&)=delete; MonoReader& operator=(const MonoReader&)=delete;
    Status open(const std::string& path){
#ifdef O_DIRECT
        fd=::open(path.c_str(),O_RDONLY|O_DIRECT);
        if(fd>=0){ void* p=NULL; ssize_t k=-1; int e=ENOMEM;
            if(posix_memalign(&p,4096,4096)==0){ k=pread(fd,p,4096,0); e=errno; free(p); }
            if(k>=0) direct=true;
            else { close(fd); fd=-1; if(e!=EINVAL) return Status::fail("cannot read "+path+": "+strerror(e)); } }
#endif
        if(fd<0&&(fd=::open(path.c_str(),O_RDONLY))<0) return Status::fail("cannot open "+path+": "+strerror(errno));
#ifdef __APPLE__
        direct=fcntl(fd,F_NOCACHE,1)==0;
#endif
        return Status();
    }
    int readAt(void* buf,size_t n,uint64_t off) const {   // 0, or an errno (EIO: the file ends early)
        char* p=(char*)buf; size_t got=0;
        while(got<n){ ssize_t k=pread(fd,p+got,n-got,(off_t)(off+got)); if(k<0&&errno==EINTR) continue; if(k<0) return errno; if(k==0) return EIO; got+=(size_t)k; }
        return 0;
    }
};
struct NeedChunk { uint64_t off; uint32_t size, cx, cy, cz; };
// mono store: every chunk of level fr under the region [fo, fo+fn), from the single file, decoded and handed to put
// (from several threads). The finer tiles' sidecars (commit points: level fr's workers must all have finished) name
// every chunk; only the chunks under the region are read, in runs of nearby chunks. A short read (this node may
// see an old size of a file other nodes write) is retried once with the file opened again.
static Status assembleFromMono(const Manifest& m,const std::string& wd,int fr,int c,int t,const bpSize fo[3],const bpSize fn[3],const ChunkSink& put,bool* direct){
    const Lev& F=m.lev[fr]; bpSize cb0[3],cb1[3]; const std::vector<long> tiles=finerTiles(F,fo,fn,cb0,cb1);
    const std::string cd=wd+"/chunks", path=monoFilePath(wd), hint=" (run level "+std::to_string(fr)+"'s workers first; re-run the worker of a damaged sidecar)";
    std::vector<NeedChunk> need;
    for(long ft:tiles){
        const std::string p=tileIdxName(cd,fr,c,t,ft); MonoIdx x; std::string e;
        if(!fileExists(p)) return Status::fail("level "+std::to_string(fr+1)+" needs "+p+hint);
        if(!mono_idx_read(p,x,&e)) return Status::fail(e+hint);
        bool geo=x.owner==tileOwner(fr,c,t,ft)&&x.timepoint==t&&x.channel==c&&x.levels.size()==1&&x.levels[0].level==(uint32_t)fr&&x.levels[0].dtype==monoDtype(m);
        for(int a=0;geo&&a<3;a++) geo=x.levels[0].size[a]==F.size[a]&&x.levels[0].chunk[a]==F.block[a]&&x.levels[0].grid[a]==F.nch[a];
        if(!geo) return Status::fail(p+" is not the sidecar of that tile in this workdir's layout"+hint);
        Status s=checkTileChunks(x.chunks.size(),[&](uint64_t i,bpSize& cx,bpSize& cy,bpSize& cz){ cx=x.chunks[i].cx; cy=x.chunks[i].cy; cz=x.chunks[i].cz; },F,ft,p);
        if(!s.ok()) return Status::fail(s.message+hint);
        for(const MonoChunk& ch:x.chunks) if(blockIn(ch.cx,ch.cy,ch.cz,cb0,cb1)){ NeedChunk n={ch.offset,ch.size,ch.cx,ch.cy,ch.cz}; need.push_back(n); }
    }
    std::sort(need.begin(),need.end(),[](const NeedChunk& a,const NeedChunk& b){ return a.off<b.off; });
    // runs: chunks at most 256 KiB apart, up to 64 MiB (or one bigger chunk), 4096-aligned
    struct Run { uint64_t off, end; size_t first, last; };
    std::vector<Run> runs; size_t maxRun=4096;
    for(size_t i=0;i<need.size();){
        Run r; r.first=i; r.off=need[i].off&~(uint64_t)4095; uint64_t e=need[i].off+need[i].size; i++;
        while(i<need.size()&&need[i].off<=e+(256u<<10)&&need[i].off+need[i].size-r.off<=(64u<<20)){ e=std::max(e,need[i].off+need[i].size); i++; }
        r.last=i; r.end=(e+4095)&~(uint64_t)4095; runs.push_back(r); maxRun=std::max(maxRun,(size_t)(r.end-r.off));
    }
    MonoReader rd; Status s=rd.open(path); if(!s.ok()) return s;
    if(direct) *direct=rd.direct;
    const size_t raw=(size_t)(F.block[0]*F.block[1]*F.block[2])*m.elem;
    std::atomic<bool> failed(false); std::string err;
    #pragma omp parallel
    {
        void* buf=NULL; std::vector<uint8_t> sh(raw);
        if(posix_memalign(&buf,4096,maxRun)){ buf=NULL; firstError(failed,err,"out of memory"); }
        #pragma omp for schedule(dynamic)
        for(long k=0;k<(long)runs.size();k++){
            if(failed.load()) continue;
            const Run& r=runs[(size_t)k]; const size_t n=(size_t)(r.end-r.off); std::string why;
            int re=rd.readAt(buf,n,r.off);
            if(re==EIO){ MonoReader again; Status o=again.open(path); if(!o.ok()) why=o.message; else re=again.readAt(buf,n,r.off); }
            if(re&&why.empty()) why="cannot read "+path+" at "+std::to_string(r.off)+": "+(re==EIO?std::string("the file ends early"):std::string(strerror(re)));
            for(size_t i=r.first;why.empty()&&i<r.last;i++){ const NeedChunk& q=need[i];
                Status d=decodeChunkShuffled((const uint8_t*)buf+(q.off-r.off),q.size,sh.data(),raw);
                if(!d.ok()) why=path+": level "+std::to_string(fr)+" chunk ("+std::to_string(q.cx)+","+std::to_string(q.cy)+","+std::to_string(q.cz)+") at "+std::to_string(q.off)+": "+d.message;
                else put(sh.data(),q.cx,q.cy,q.cz); }
            if(!why.empty()) firstError(failed,err,why);
        }
        free(buf);
    }
    if(failed) return Status::fail(err+hint);
    return Status();
}
#endif

// The chunks of one tile, encoded in (z, y, x) order, and the histogram of its in-image voxels (OpenMP over the
// blocks: each block is shuffled straight from the tile and counted on the way).
struct TileChunks { std::vector<std::vector<uint8_t> > comp; std::vector<uint64_t> hist; uint64_t bytes=0; };
template<class T> static Status encodeTile(const T* tile,const bpSize td[3],const Lev& L,const bpSize cb0[3],const bpSize cb1[3],TileChunks& o){
    const bpSize b0=L.block[0],b1=L.block[1],b2=L.block[2]; const size_t nb=(size_t)(b0*b1*b2);
    const bpSize nbx=cb1[0]-cb0[0],nby=cb1[1]-cb0[1]; const long long n=(long long)(nbx*nby*(cb1[2]-cb0[2]));
    o.comp.assign((size_t)n,std::vector<uint8_t>()); o.hist.assign(65536,0); o.bytes=0;
    std::atomic<bool> bad(false);
    #pragma omp parallel
    {
        std::vector<uint64_t> h(65536,0); std::vector<uint8_t> sh(sizeof(T)*nb); uint8_t* lo=sh.data(); uint8_t* hi=sh.data()+(sizeof(T)==2?nb:0);
        #pragma omp for schedule(dynamic)
        for(long long k=0;k<n;k++){
            const bpSize ix=(bpSize)k%nbx, iy=((bpSize)k/nbx)%nby, iz=(bpSize)k/(nbx*nby), by=cb0[1]+iy, bz=cb0[2]+iz;
            const size_t xv=(size_t)std::min(b0,L.size[0]-(cb0[0]+ix)*b0);
            for(bpSize z=0;z<b2;z++) for(bpSize y=0;y<b1;y++){
                const T* src=tile+(size_t)(ix*b0+(iy*b1+y)*td[0]+(iz*b2+z)*td[0]*td[1]);
                const size_t j0=(size_t)(y*b0+z*b0*b1);
                if(sizeof(T)==1) memcpy(lo+j0,src,b0);   // 1-byte samples are not shuffled
                else for(size_t x=0;x<b0;x++){ const unsigned v=src[x]; lo[j0+x]=(uint8_t)v; hi[j0+x]=(uint8_t)(v>>8); }
                if(by*b1+y<L.size[1]&&bz*b2+z<L.size[2]) for(size_t x=0;x<xv;x++) h[src[x]]++;
            }
            if(!encodeChunkShuffled(sh.data(),sizeof(T)*nb,o.comp[(size_t)k])) bad=true;
        }
        #pragma omp critical(piw_hist)
        for(size_t i=0;i<65536;i++) o.hist[i]+=h[i];
    }
    if(bad) return Status::fail("LZ4 could not compress a chunk");
    for(const std::vector<uint8_t>& v:o.comp) o.bytes+=v.size();
    return Status();
}
// packs store: every chunk of the tile in ONE file (body, then the index, then the count), then its histogram;
// the pack's name is the commit point the monitor waits for.
static Status storePack(const std::string& cd,int r,int c,int t,long ti,const bpSize cb0[3],const bpSize cb1[3],const TileChunks& o){
    const std::string pp=packName(cd,r,c,t,ti), ptmp=pp+".tmp"; FILE* pf=fopen(ptmp.c_str(),"wb");
    if(!pf) return Status::fail("cannot create "+ptmp+": "+strerror(errno));
    std::vector<uint64_t> pidx; bool wok=true; size_t k=0;
    for(bpSize bz=cb0[2];bz<cb1[2];bz++)for(bpSize by=cb0[1];by<cb1[1];by++)for(bpSize bx=cb0[0];bx<cb1[0];bx++,k++){
        const std::vector<uint8_t>& v=o.comp[k];
        wok=wok&&fwrite(v.data(),1,v.size(),pf)==v.size(); pidx.push_back(bz); pidx.push_back(by); pidx.push_back(bx); pidx.push_back(v.size()); }
    uint64_t N=pidx.size()/4;
    if(wok&&N) wok=fwrite(pidx.data(),8,pidx.size(),pf)==pidx.size();
    if(wok) wok=fwrite(&N,8,1,pf)==1;
    int e=errno; wok=(fclose(pf)==0)&&wok;
    if(!wok){ remove(ptmp.c_str()); return Status::fail("cannot write "+ptmp+": "+strerror(e?e:errno)); }
    Status s=writeFileAtomic(histName(cd,r,c,t,ti),o.hist.data(),o.hist.size()*sizeof(uint64_t));
    if(!s.ok()){ remove(ptmp.c_str()); return s; }
    if(!renameReplace(ptmp,pp)){ e=errno; remove(ptmp.c_str()); return Status::fail("cannot rename "+ptmp+" to "+pp+": "+strerror(e)); }
    return Status();
}
#ifndef _WIN32
// mono store: claims the tile's range of the single file (a re-run gets the same range), writes the chunks there,
// then the histogram and the sidecar (the commit point).
static Status storeMono(const Manifest& m,const std::string& wd,int r,int c,int t,long ti,const bpSize cb0[3],const bpSize cb1[3],
                        const TileChunks& o,int threads,bool* direct){
    const Lev& L=m.lev[r]; const std::string cd=wd+"/chunks", owner=tileOwner(r,c,t,ti); std::string e;
    MonoClaims claims; MonoClaim rec;
    if(!claims.open(monoClaimsDir(wd),"",&e)||!claims.claim(owner,o.bytes,rec,&e)) return Status::fail(e);
    MonoIdx x; x.owner=owner; x.timepoint=t; x.channel=c; x.claimOffset=rec.offset; x.claimSize=rec.size; x.blobBytes=o.bytes;
    MonoIdxLevel g; g.level=(uint32_t)r; g.dtype=monoDtype(m); g.nChunks=o.comp.size();
    for(int a=0;a<3;a++){ g.size[a]=L.size[a]; g.chunk[a]=(uint32_t)L.block[a]; g.grid[a]=(uint32_t)L.nch[a]; }
    x.levels.push_back(g); x.chunks.reserve(o.comp.size());
    MonoBlobWriter w; uint64_t off=rec.offset; size_t k=0;
    bool ok=w.open(monoFilePath(wd),rec.offset,rec.size,threads,&e);
    for(bpSize bz=cb0[2];ok&&bz<cb1[2];bz++)for(bpSize by=cb0[1];ok&&by<cb1[1];by++)for(bpSize bx=cb0[0];ok&&bx<cb1[0];bx++,k++){
        const std::vector<uint8_t>& v=o.comp[k];
        ok=w.append(v.data(),v.size(),&e);
        MonoChunk ch; memset(&ch,0,sizeof ch);
        ch.offset=off; ch.cx=(uint32_t)bx; ch.cy=(uint32_t)by; ch.cz=(uint32_t)bz; ch.size=(uint32_t)v.size(); ch.level=(uint16_t)r;
        x.chunks.push_back(ch); off+=v.size(); }
    if(ok) ok=w.finish(&e);
    if(!ok) return Status::fail(e+" (nothing was committed; run the worker again)");
    if(direct) *direct=w.direct();
    if(!mono_write_atomic(histName(cd,r,c,t,ti),o.hist.data(),HIST_BYTES,&e)||!mono_idx_write(tileIdxName(cd,r,c,t,ti),x,&e)) return Status::fail(e);
    return Status();
}
#endif

// One tile: its level-r voxels (level 0 from the zarr, at the manifest's origin; coarser levels downsampled from the
// finer level's region under it), zero past the level's size, encoded, stored. line: its timing line (empty for an
// empty tile).
template<class T> static Status workTileT(const Manifest& m,const std::string& wd,int r,long index,std::string& line){
    const std::string cd=wd+"/chunks"; const Lev& Lr=m.lev[r]; const long perCT=tilesPerCT(Lr); const bool mono=m.store=="mono";
    const int c=(int)(index/((long)m.nT*perCT)); const long rem=index%((long)m.nT*perCT); const int t=(int)(rem/perCT); const long ti=rem%perCT;
    const int threads=std::max(1,omp_get_max_threads());
    const double t0=now();
    bpSize cb0[3],cb1[3]; tileBlockRange(Lr,ti,cb0,cb1);
    if(cb0[0]>=Lr.nch[0]||cb0[1]>=Lr.nch[1]||cb0[2]>=Lr.nch[2]) return Status();   // empty tile
    const bpSize blk[3]={Lr.block[0],Lr.block[1],Lr.block[2]};
    const bpSize to[3]={cb0[0]*blk[0],cb0[1]*blk[1],cb0[2]*blk[2]};                                  // tile voxel origin
    const bpSize td[3]={(cb1[0]-cb0[0])*blk[0],(cb1[1]-cb0[1])*blk[1],(cb1[2]-cb0[2])*blk[2]};      // tile voxel extent (block-aligned)
    const size_t nv=(size_t)(td[0]*td[1]*td[2]);
    Vox<T> tile(NULL,free); double tDown=0; bool readDirect=false;
    if(r==0){
        const bpSize ex=std::min(to[0]+td[0],m.X),ey=std::min(to[1]+td[1],m.Y),ez=std::min(to[2]+td[2],m.Z), rx=ex-to[0],ry=ey-to[1],rz=ez-to[2];
        Vox<T> reg((T*)readZarrParallelHelper(zarrAt(m,c,t).c_str(),m.org[0]+to[0],m.org[1]+to[1],m.org[2]+to[2],m.org[0]+ex,m.org[1]+ey,m.org[2]+ez,0),free);
        if(!reg) return Status::fail("cannot read "+zarrAt(m,c,t));
        if(rx==td[0]&&ry==td[1]&&rz==td[2]) tile=std::move(reg);   // the whole tile is inside the image
        else { tile=newVox<T>(nv); if(!tile) return Status::fail("out of memory");
            for(bpSize z=0;z<rz;z++) for(bpSize y=0;y<ry;y++) memcpy(tile.get()+(size_t)(y*td[0]+z*td[0]*td[1]),reg.get()+(size_t)(y*rx+z*rx*ry),(size_t)rx*sizeof(T)); }
    } else {
        // the finer level's chunks under the tile, box-averaged straight into it (or, for a block layout where a box
        // may span chunks, assembled into the finer region first and then downsampled)
        const bpSize st[3]={levStride(m,r,0),levStride(m,r,1),levStride(m,r,2)};
        const bpSize fo[3]={to[0]*st[0],to[1]*st[1],to[2]*st[2]}, fn[3]={td[0]*st[0],td[1]*st[1],td[2]*st[2]};
        const Lev& F=m.lev[r-1]; const bool inChunks=downInChunks(F,st);
        Vox<T> fine(NULL,free); tile=newVox<T>(nv);
        if(!inChunks) fine=newVox<T>((size_t)(fn[0]*fn[1]*fn[2]));
        if(!tile||(!inChunks&&!fine)) return Status::fail("out of memory");
        T* Tv=tile.get(); T* Fv=fine.get();
        const ChunkSink put=inChunks?ChunkSink([&](const uint8_t* sh,bpSize cx,bpSize cy,bpSize cz){ downChunk<T>(sh,F,cx,cy,cz,fo,fn,st,td,Tv); })
                                    :ChunkSink([&](const uint8_t* sh,bpSize cx,bpSize cy,bpSize cz){ scatterChunk<T>(sh,F,cx,cy,cz,fo,fn,Fv); });
        Status s;
        if(mono){
#ifdef _WIN32
            s=Status::fail(NOSUP_MONO);
#else
            s=assembleFromMono(m,wd,r-1,c,t,fo,fn,put,&readDirect);
#endif
        } else s=assembleFromPacks(m,cd,r-1,c,t,fo,fn,put);
        if(!s.ok()) return s;
        if(!inChunks){ const double t1=now();
            bpSize dOut[3]; boxDown(fine.get(),fn,st,tile.get(),dOut);   // tile <- downsample(fine); dims td = fn/st
            tDown=now()-t1; }
    }
    const double tRead=now()-t0-tDown;
    // zero voxels past the true level size (floor-halving tail; the writer zero-pads them)
    const bpSize ex=std::min(td[0],Lr.size[0]-to[0]), ey=std::min(td[1],Lr.size[1]-to[1]), ez=std::min(td[2],Lr.size[2]-to[2]);
    if(ex<td[0]||ey<td[1]||ez<td[2])
        for(bpSize z=0;z<td[2];z++) for(bpSize y=0;y<td[1];y++){ T* row=tile.get()+(size_t)(y*td[0]+z*td[0]*td[1]);
            if(y>=ey||z>=ez) memset(row,0,(size_t)td[0]*sizeof(T)); else memset(row+ex,0,(size_t)(td[0]-ex)*sizeof(T)); }
    const double te=now();
    TileChunks o; Status s=encodeTile(tile.get(),td,Lr,cb0,cb1,o); if(!s.ok()) return Status::fail(s.message+" of "+tileOwner(r,c,t,ti));
    tile.reset();
    const double tEnc=now()-te, tw=now(); bool writeDirect=false;
    if(mono){
#ifdef _WIN32
        s=Status::fail(NOSUP_MONO);
#else
        s=storeMono(m,wd,r,c,t,ti,cb0,cb1,o,std::min(threads,8),&writeDirect);
#endif
    } else s=storePack(cd,r,c,t,ti,cb0,cb1,o);
    if(!s.ok()) return s;
    const double tWrite=now()-tw;
    std::string rd=r==0?"read "+fmt1(tRead)+" s":"read + downsample level "+std::to_string(r-1)+" "+fmt1(tRead)+" s"+(mono?(readDirect?" (direct I/O)":" (buffered)"):"")
                                                   +(tDown>0?", downsample "+fmt1(tDown)+" s":"");
    std::string wr=mono?"place "+fmt1(tWrite)+" s"+(writeDirect?" (direct I/O)":" (buffered + sync)"):"write pack "+fmt1(tWrite)+" s";
    char b[512]; snprintf(b,sizeof b,"worker %s: %zu chunks, %.2f GB -> %.2f GB: %s, encode %s s, %s, total %s s, %d threads",
           tileOwner(r,c,t,ti).c_str(),o.comp.size(),(double)nv*sizeof(T)/1e9,(double)o.bytes/1e9,rd.c_str(),fmt1(tEnc).c_str(),wr.c_str(),fmt1(now()-t0).c_str(),threads);
    line=b;
    return Status();
}
static Status workTile(const Manifest& m,const std::string& wd,int r,long index,std::string& line){
    return m.dtype=="uint8"?workTileT<uint8_t>(m,wd,r,index,line):workTileT<uint16_t>(m,wd,r,index,line); }
// The calling thread's OpenMP thread count for the duration of a call (n > 0), as the converter's library calls.
struct OmpThreads {
    bool set; int prev;
    explicit OmpThreads(int n):set(n>0),prev(omp_get_max_threads()){ if(set) omp_set_num_threads(n); }
    ~OmpThreads(){ if(set) omp_set_num_threads(prev); }
    OmpThreads(const OmpThreads&)=delete; OmpThreads& operator=(const OmpThreads&)=delete;
};
// Whether a tile's commit point is there: its histogram and its sidecar (mono store) or pack (packs store), what
// monitor waits for.
static bool tileDone(const Manifest& m,const std::string& wd,int r,int c,int t,long ti){
    const std::string cd=wd+"/chunks";
    return fileExists(m.store=="mono"?tileIdxName(cd,r,c,t,ti):packName(cd,r,c,t,ti))&&fileExists(histName(cd,r,c,t,ti));
}
// Tiles index .. index+count-1 of level r (clipped to the level's last tile), one after the other. In the mono store
// the workdir is marked as a tile workdir first (monoplace then refuses it). skipDone: tiles whose commit point is
// there are not done again. onLine: each tile's timing line.
static Status workTiles(const Manifest& m,const std::string& wd,int r,long index,long count,bool skipDone,
                        const std::function<void(const std::string&)>& onLine){
    const long jobs=jobsAtLevel(m,r);
    if(m.store=="mono"){
#ifdef _WIN32
        return Status::fail(NOSUP_MONO);
#else
        // the single file is built from tiles: refuse a workdir that holds placed parts, and mark it for monoplace
        if(fileExists(monoClaimsDir(wd)+"/layout.json")) return Status::fail(wd+" holds parts placed by monoplace: its single file cannot also take tiles (use a new workdir)");
        Status s=makeDirs(monoClaimsDir(wd)); if(!s.ok()) return s;
        int fd=::open((monoClaimsDir(wd)+"/tiles").c_str(),O_WRONLY|O_CREAT,0666);
        if(fd<0) return Status::fail("cannot create "+monoClaimsDir(wd)+"/tiles: "+strerror(errno));
        close(fd);
#endif
    }
    const long last=count>jobs-index?jobs:index+count, perCT=tilesPerCT(m.lev[r]);
    for(long i=index;i<last;i++){
        if(skipDone){ const int c=(int)(i/((long)m.nT*perCT)); const long rem=i%((long)m.nT*perCT);
            if(tileDone(m,wd,r,c,(int)(rem/perCT),rem%perCT)) continue; }
        std::string line; Status s=workTile(m,wd,r,i,line); if(!s.ok()) return s;
        if(!line.empty()&&onLine) onLine(line); }
    return Status();
}
static int doWorker(const std::string& wd,int r,long index,long count){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    s=tileTypeOk(m,wd); if(!s.ok()) return fail(s);
    int L=(int)m.lev.size();
    if(r<0||r>=L){ fprintf(stderr,"level %d out of range [0,%d)\n",r,L); return 1; }
    const long jobs=jobsAtLevel(m,r);
    if(index<0||index>=jobs){ fprintf(stderr,"index %ld out of range [0,%ld) for level %d\n",index,jobs,r); return 1; }
    if(count<1) return fail("worker: the tile count must be at least 1");
    s=workTiles(m,wd,r,index,count,false,[](const std::string& l){ printf("%s\n",l.c_str()); fflush(stdout); });
    return s.ok()?0:fail(s);
}

// ===================== gather: parallel-read packs, single HDF5 writer =====================
static std::vector<int> iota0(int n){ std::vector<int> v(n); for(int i=0;i<n;i++) v[i]=i; return v; }
// write /DataSet/ResolutionLevel r/TimePoint tOut/Channel cOut/{ImageSize,Histogram,Data} from packs (r,cSrc,tSrc).
// Every tile's pack must hold exactly its tile's chunks, so a missing or damaged pack fails the dataset.
static Status writeOneDataset(hid_t out,const Manifest& m,const std::string& cd,hid_t lcpl,hid_t dtype,int r,int tSrc,int cSrc,int tOut,int cOut){
    const Lev& L=m.lev[r];
    std::string dp="DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(tOut)+"/Channel "+std::to_string(cOut);
    std::vector<uint64_t> bins; Status s=sumPartialHists(m,cd,r,cSrc,tSrc,bins); if(!s.ok()) return s;
    hid_t g=H5Gcreate2(out,dp.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT); if(g<0) return Status::fail("cannot create "+dp+" (HDF5 error above)");
    bool ok=setStrAttr(g,"ImageSizeX",std::to_string(L.size[0])); ok&=setStrAttr(g,"ImageSizeY",std::to_string(L.size[1])); ok&=setStrAttr(g,"ImageSizeZ",std::to_string(L.size[2]));
    ok&=writeHistogramFromBins(g,bins);
    hsize_t fd[3]={L.block[2]*L.nch[2],L.block[1]*L.nch[1],L.block[0]*L.nch[0]}, chk[3]={L.block[2],L.block[1],L.block[0]};
    hid_t dcpl=H5Pcreate(H5P_DATASET_CREATE); ok&=H5Pset_chunk(dcpl,3,chk)>=0; if(m.elem==2||m.elem==4) ok&=H5Pset_shuffle(dcpl)>=0; ok&=setLZ4(dcpl);
    hid_t sp=H5Screate_simple(3,fd,NULL); hid_t d=ok?H5Dcreate2(g,"Data",dtype,sp,H5P_DEFAULT,dcpl,H5P_DEFAULT):-1;
    if(d<0){ H5Sclose(sp);H5Pclose(dcpl);H5Gclose(g); return Status::fail("cannot write "+dp+" (HDF5 error above)"); }
    long perCT=tilesPerCT(L); std::atomic<bool> failed(false); std::string err; uint64_t written=0;
    #pragma omp parallel for schedule(dynamic)
    for(long tile=0;tile<perCT;tile++){
        if(failed.load()) continue;
        Status ts=forEachPackedChunk(packName(cd,r,cSrc,tSrc,tile),[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz)->Status{
            if(failed.load()) return Status::fail("stopped");          // another tile failed
            hsize_t off[3]={cz*L.block[2],cy*L.block[1],cx*L.block[0]}; herr_t rc;
            #pragma omp critical
            { rc=H5Dwrite_chunk(d,H5P_DEFAULT,0,off,sz,(void*)by); if(rc>=0) written++; }
            return rc<0?Status::fail("H5Dwrite_chunk failed in "+dp+" (HDF5 error above)"):Status();
        },&L,tile);
        if(!ts.ok()){
            #pragma omp critical(piw_err)
            { if(!failed.load()){ err=ts.message; failed=true; } }
        }
    }
    uint64_t want=(uint64_t)L.nch[0]*L.nch[1]*L.nch[2];
    if(!failed&&written!=want) err=dp+": wrote "+std::to_string(written)+" of "+std::to_string(want)+" chunks";
    ok=H5Dclose(d)>=0&&ok; H5Sclose(sp);H5Pclose(dcpl); ok=H5Gclose(g)>=0&&ok;
    if(!err.empty()) return Status::fail(err);
    return ok?Status():Status::fail("cannot write "+dp+" (HDF5 error above)");
}
// /Thumbnail (imsthumb.h) of source timepoint tSrc, channels srcCh, from the packs of the thumbnail level; display =
// the channel settings writeImsMeta wrote for srcCh.
static Status writePackThumbnail(hid_t out,const Manifest& m,const std::string& cd,const std::vector<int>& srcCh,int tSrc,
                                 const std::vector<ChannelDisplay>& display){
    std::vector<Vec3> sizes; for(const Lev& L:m.lev) sizes.push_back(L.size);
    const int r=(int)imsthumb::thumbnailLevel(sizes); const Lev& L=m.lev[r];
    const size_t rawBytes=(size_t)L.block[0]*L.block[1]*L.block[2]*sizeof(uint16_t);
    std::vector<std::vector<uint16_t> > vol(srcCh.size());
    for(size_t oc=0;oc<srcCh.size();oc++){
        vol[oc].assign((size_t)L.size[0]*L.size[1]*L.size[2],0);
        long perCT=tilesPerCT(L); std::atomic<bool> failed(false); std::string err;
        #pragma omp parallel for schedule(dynamic)
        for(long tile=0;tile<perCT;tile++){
            if(failed.load()) continue;
            std::vector<uint8_t> vox;
            Status ts=forEachPackedChunk(packName(cd,r,srcCh[oc],tSrc,tile),[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz)->Status{
                Status ds=decodeChunk(by,sz,vox,rawBytes); if(!ds.ok()) return ds;
                chunkToVolume((const uint16_t*)vox.data(),L.block.data(),cx,cy,cz,L.size.data(),vol[oc].data()); return Status();
            },&L,tile);
            if(!ts.ok()){
                #pragma omp critical(piw_err)
                { if(!failed.load()){ err=ts.message; failed=true; } }
            }
        }
        if(failed) return Status::fail("thumbnail: "+err);
    }
    float ext[3]; imsExtentMax(m.X,m.Y,m.Z,m.vox,ext);
    return imsthumb::writeThumbnail(out,sizes,vol,display,ext);
}
// close the file; on any failure remove the half-written output
static Status closeOutput(hid_t out,const std::string& outPath,Status s){
    if(H5Fclose(out)<0&&s.ok()) s=Status::fail("cannot close "+outPath+" (HDF5 error above)");
    if(!s.ok()){ remove(outPath.c_str()); return Status::fail(s.message+"; removed "+outPath); }
    return Status();
}
static int finishOutput(hid_t out,const std::string& outPath,Status s){ s=closeOutput(out,outPath,s); return s.ok()?0:fail(s); }

// packs store: every tile's packs and histograms into one new .ims (checked by checkPacks first)
static Status combinePacks(const Manifest& m,const std::string& wd,const std::string& outPath){
    std::string cd=wd+"/chunks"; int nT=m.nT, nC=m.nC, nR=(int)m.lev.size();
    hid_t out=imsCreate(outPath); if(out<0) return Status::fail("cannot create "+outPath+" (HDF5 error above)");
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    std::vector<ChannelDisplay> display;
    Status s=writeImsMeta(out,m,cd,lcpl,iota0(nC),iota0(nT),&display);
    hid_t dtype=imsDtype(m.elem);
    // parallel-read each (r,t,c)'s tile .pack files across threads; single writer does H5Dwrite_chunk
    for(int r=0;r<nR&&s.ok();r++) for(int t=0;t<nT&&s.ok();t++) for(int c=0;c<nC&&s.ok();c++) s=writeOneDataset(out,m,cd,lcpl,dtype,r,t,c,t,c);
    if(s.ok()) s=writePackThumbnail(out,m,cd,iota0(nC),0,display);
    H5Tclose(dtype); H5Pclose(lcpl);
    return closeOutput(out,outPath,s);
}
static int doCombine(const Manifest& m,const std::string& wd,const std::string& outPath){
    Status s=combinePacks(m,wd,outPath); if(!s.ok()) return fail(s);
    printf("merged %d channels x %d timepoints x %d levels -> %s\n",m.nC,m.nT,(int)m.lev.size(),outPath.c_str()); return 0;
}

// one (timepoint, channel) -> a single-tp/channel .ims (renumbered to TimePoint 0 / Channel 0). Independent
// of every other (t,c), so all nT*nC of these can run as one parallel array -- no shared HDF5 file, no single
// writer. Stitched afterward by `link` (external links). This is the fast combine.
static int combineOne(const std::string& wd,int T,int C,const std::string& outPath){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    if(m.elem!=2) return fail(std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
    if(m.store!="packs") return fail(std::string(PACKS_ONLY)+"; "+wd+" uses the single-file store");
    std::string cd=wd+"/chunks"; int nR=(int)m.lev.size();
    if(T<0||T>=m.nT||C<0||C>=m.nC){ fprintf(stderr,"(t=%d,c=%d) out of range (nT=%d,nC=%d)\n",T,C,m.nT,m.nC); return 1; }
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    std::vector<ChannelDisplay> display;
    s=writeImsMeta(out,m,cd,lcpl,std::vector<int>{C},std::vector<int>{T},&display);
    hid_t dtype=imsDtype(m.elem);
    for(int r=0;r<nR&&s.ok();r++) s=writeOneDataset(out,m,cd,lcpl,dtype,r,T,C,0,0);
    if(s.ok()) s=writePackThumbnail(out,m,cd,std::vector<int>{C},T,display);
    H5Tclose(dtype); H5Pclose(lcpl);
    if(finishOutput(out,outPath,s)) return 1;
    printf("combined (t=%d,c=%d) x %d levels -> %s\n",T,C,nR,outPath.c_str()); return 0;
}
static std::string partName(int t,int c){ char b[64]; snprintf(b,sizeof b,"part_t%d_c%d.ims",t,c); return b; }
// number of /DataSet/ResolutionLevel <n> groups, and each level's ImageSize (from TimePoint 0/Channel 0)
static Status partLevels(hid_t f,const std::string& p,std::vector<Vec3>& sizes){
    sizes.clear();
    for(int r=0;;r++){ std::string g="/DataSet/ResolutionLevel "+std::to_string(r);
        if(!h5PathExists(f,g)) break;
        g+="/TimePoint 0/Channel 0"; hid_t gr=h5PathExists(f,g)?H5Gopen2(f,g.c_str(),H5P_DEFAULT):-1;
        if(gr<0) return Status::fail(p+": no "+g);
        Vec3 v; const char* k[3]={"ImageSizeX","ImageSizeY","ImageSizeZ"};
        for(int a=0;a<3;a++) v[a]=strtoull(readStrAttr(gr,k[a]).c_str(),NULL,10);
        H5Gclose(gr); sizes.push_back(v); }
    if(sizes.empty()) return Status::fail(p+" has no ResolutionLevels");
    return Status();
}
// Every part_t{t}_c{c}.ims in partDir must open and have the levels and level sizes of `want`.
static Status checkParts(const std::string& partDir,int nT,int nC,const std::vector<Vec3>& want){
    int bad=0; std::string first;
    for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){ std::string p=partDir+"/"+partName(t,c); Status s;
        hid_t f; { H5Quiet q; f=H5Fopen(p.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); }
        if(f<0) s=Status::fail("cannot open part "+p);
        else { std::vector<Vec3> got; s=partLevels(f,p,got);
            if(s.ok()&&got!=want) s=Status::fail(p+" has "+std::to_string(got.size())+" levels / sizes that differ from the expected "+std::to_string(want.size()));
            H5Fclose(f); }
        if(!s.ok()){ if(bad<10) fprintf(stderr,"%s\n",s.message.c_str()); if(!bad) first=s.message; bad++; } }
    if(bad) return Status::fail(std::to_string(bad)+" of "+std::to_string(nT*nC)+" parts missing or inconsistent (first: "+first+")");
    return Status();
}
// Level r of TimePoint 0/Channel 0 of part p (uint16), decoded from its raw chunks (the LZ4 filter is registered
// for writing only, so H5Dread cannot decode them) into a dense S[0]*S[1]*S[2] volume. False (no error) for another
// voxel type.
static Status readPartLevel(const std::string& p,size_t r,const Vec3& S,std::vector<uint16_t>& vol,bool& u16){
    const std::string dp="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint 0/Channel 0/Data";
    const Status bad=Status::fail("thumbnail: cannot read "+p+":"+dp+" (HDF5 error above)");
    hid_t f=H5Fopen(p.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); if(f<0) return Status::fail("thumbnail: cannot open part "+p);
    hid_t d=H5Dopen2(f,dp.c_str(),H5P_DEFAULT), ty=d<0?-1:H5Dget_type(d), pl=d<0?-1:H5Dget_create_plist(d);
    u16=ty>=0&&H5Tget_class(ty)==H5T_INTEGER&&H5Tget_size(ty)==2;
    hsize_t ck[3]={0,0,0}; Status s=(d<0||ty<0||pl<0)?bad:Status();
    if(s.ok()&&u16&&H5Pget_chunk(pl,3,ck)!=3) s=bad;
    if(s.ok()&&u16){
        const bpSize b[3]={ck[2],ck[1],ck[0]}, size[3]={S[0],S[1],S[2]}; const size_t raw=(size_t)(b[0]*b[1]*b[2]*2);
        vol.assign((size_t)(S[0]*S[1]*S[2]),0);
        std::vector<uint8_t> cbuf,vox;
        for(bpSize cz=0;s.ok()&&cz*b[2]<S[2];cz++) for(bpSize cy=0;s.ok()&&cy*b[1]<S[1];cy++) for(bpSize cx=0;s.ok()&&cx*b[0]<S[0];cx++){
            hsize_t off[3]={cz*b[2],cy*b[1],cx*b[0]}, n=0; uint32_t fm=0;
            if(H5Dget_chunk_storage_size(d,off,&n)<0||n==0){ s=bad; break; }
            cbuf.resize((size_t)n);
            if(H5Dread_chunk(d,H5P_DEFAULT,off,&fm,cbuf.data())<0){ s=bad; break; }
            Status ds=decodeChunk(cbuf.data(),cbuf.size(),vox,raw);
            if(!ds.ok()) s=Status::fail("thumbnail: "+p+":"+dp+": "+ds.message);
            else chunkToVolume((const uint16_t*)vox.data(),b,cx,cy,cz,size,vol.data());
        }
    }
    if(pl>=0){ H5Pclose(pl); }
    if(ty>=0){ H5Tclose(ty); }
    if(d>=0){ H5Dclose(d); }
    H5Fclose(f);
    return s;
}
// /Thumbnail of a link file from its parts: channel c from TimePoint 0/Channel 0 of part_t0_c{c}.ims (the thumbnail
// level of `sizes`, every part's level sizes). display = the link file's channel settings. Parts that are not
// uint16 give no thumbnail.
static Status writePartsThumbnail(hid_t out,const std::string& partDir,const std::vector<Vec3>& sizes,const float ext[3],
                                  const std::vector<ChannelDisplay>& display){
    const size_t r=imsthumb::thumbnailLevel(sizes);
    std::vector<std::vector<uint16_t> > vol(display.size());
    for(size_t c=0;c<display.size();c++){ bool u16=false;
        Status s=readPartLevel(partDir+"/"+partName(0,(int)c),r,sizes[r],vol[c],u16);
        if(!s.ok()) return s;
        if(!u16) return Status(); }
    return imsthumb::writeThumbnail(out,sizes,vol,display,ext);
}
// link .ims: a tiny file of external links into the per-(tp,channel) parts. Imaris follows the links and
// presents them as one dataset. `partRel` is the parts' folder relative to this file (they must live together).
static int buildLink(const std::string& wd,const std::string& outPath,const std::string& partRel){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    if(m.elem!=2) return fail(std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
    if(m.store!="packs") return fail(std::string(PACKS_ONLY)+"; "+wd+" uses the single-file store");
    std::string cd=wd+"/chunks"; int nT=m.nT,nC=m.nC,nR=(int)m.lev.size();
    std::vector<Vec3> want; for(const Lev& L:m.lev) want.push_back(L.size);
    std::string outDir=dirOf(outPath), partDir=partRel.empty()?outDir:outDir+"/"+partRel;
    s=checkParts(partDir,nT,nC,want); if(!s.ok()) return fail(s.message+" (run combineone for every (t,c) first)");
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    std::vector<ChannelDisplay> display;
    s=writeImsMeta(out,m,cd,lcpl,iota0(nC),iota0(nT),&display);
    if(s.ok()){ float ext[3]; imsExtentMax(m.X,m.Y,m.Z,m.vox,ext); s=writePartsThumbnail(out,partDir,want,ext,display); }
    int made=0,failed=0;
    for(int r=0;r<nR&&s.ok();r++)for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){
        std::string linkPath="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
        std::string target="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint 0/Channel 0";
        std::string pf=(partRel.empty()?std::string():partRel+"/")+partName(t,c);
        if(H5Lcreate_external(pf.c_str(),target.c_str(),out,linkPath.c_str(),lcpl,H5P_DEFAULT)<0) failed++; else made++;
    }
    H5Pclose(lcpl);
    if(s.ok()&&failed) s=Status::fail(std::to_string(failed)+" external links could not be created (HDF5 error above)");
    if(finishOutput(out,outPath,s)) return 1;
    printf("link -> %s: %d external links (%d failed) into %s/part_t{t}_c{c}.ims\n",outPath.c_str(),made,failed,partRel.empty()?".":partRel.c_str());
    return 0;
}

// ---- MONOLITHIC mode: parts come from the single-shot converter (one .ims per (tp,channel), NO packs).
// The link master then sources its level count + DataSetInfo from the parts themselves. ----
static bool setStrAttrOW(hid_t o,const char* n,const std::string& v){ if(H5Aexists(o,n)>0&&H5Adelete(o,n)<0) return false; return setStrAttr(o,n,v); }
static std::string partColorRange(const std::string& partDir,int c){          // channel c's ColorRange from part_t0_c{c}
    std::string p=partDir+"/"+partName(0,c); hid_t f=H5Fopen(p.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); if(f<0) return std::string();
    std::string cr; if(h5PathExists(f,"/DataSetInfo/Channel 0")){ hid_t g=H5Gopen2(f,"DataSetInfo/Channel 0",H5P_DEFAULT); cr=readStrAttr(g,"ColorRange"); H5Gclose(g); }
    H5Fclose(f); return cr; }

static int linkParts(const std::string& wd,const std::string& outPath,const std::string& partRel){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    int nT=m.nT,nC=m.nC;
    std::string outDir=dirOf(outPath);
    std::string partDir=partRel.empty()?outDir:outDir+"/"+partRel;
    std::string p00=partDir+"/"+partName(0,0);
    hid_t ref; { H5Quiet q; ref=H5Fopen(p00.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); }
    if(ref<0){ fprintf(stderr,"cannot open part %s (run the per-(tp,channel) convert jobs first)\n",p00.c_str()); return 1; }
    std::vector<Vec3> sizes; s=partLevels(ref,p00,sizes);
    if(!s.ok()){ H5Fclose(ref); return fail(s); }
    int nR=(int)sizes.size();
    s=checkParts(partDir,nT,nC,sizes); if(!s.ok()){ H5Fclose(ref); return fail(s.message+" (every part must come from the same converter settings)"); }
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); H5Fclose(ref); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    bool ok=true;
    for(const char* a : {"ImarisDataSet","ImarisVersion","DataSetInfoDirectoryName","ThumbnailDirectoryName","DataSetDirectoryName"}){
        if(H5Aexists(ref,a)>0){ hid_t sa=H5Aopen(ref,a,H5P_DEFAULT),t=H5Aget_type(sa),sp=H5Aget_space(sa);
            hssize_t n=H5Sget_simple_extent_npoints(sp); std::vector<char> b((size_t)(n>0?n:1)+1,0); ok&=H5Aread(sa,t,b.data())>=0;
            hid_t da=H5Acreate2(out,a,t,sp,H5P_DEFAULT,H5P_DEFAULT); ok&=da>=0&&H5Awrite(da,t,b.data())>=0; if(da>=0)H5Aclose(da); H5Tclose(t);H5Sclose(sp);H5Aclose(sa); } }
    { uint32_t one=1; hsize_t d=1; hid_t sp=H5Screate_simple(1,&d,NULL); hid_t a=H5Acreate2(out,"NumberOfDataSets",H5T_STD_U32LE,sp,H5P_DEFAULT,H5P_DEFAULT); ok&=a>=0&&H5Awrite(a,H5T_NATIVE_UINT32,&one)>=0; if(a>=0)H5Aclose(a); H5Sclose(sp); }
    ok&=H5Ocopy(ref,"DataSetInfo",out,"DataSetInfo",H5P_DEFAULT,lcpl)>=0;           // Image, Channel 0, TimeInfo, ... verbatim from part_t0_c0
    H5Fclose(ref);
    int failed=0;
    for(int c=1;ok&&c<nC;c++){ std::string dst="DataSetInfo/Channel "+std::to_string(c);   // duplicate the Channel-0 template for the other channels
        if(H5Lexists(out,dst.c_str(),H5P_DEFAULT)>0) H5Ldelete(out,dst.c_str(),H5P_DEFAULT);
        if(H5Ocopy(out,"DataSetInfo/Channel 0",out,dst.c_str(),H5P_DEFAULT,lcpl)<0){ fprintf(stderr,"channel %d template copy failed\n",c); failed++; } }
    std::vector<ChannelDisplay> display;   // for the thumbnail: each channel's color and its part's ColorRange
    for(int c=0;ok&&c<nC;c++){ display.push_back(imsChannelDisplay(m.meta,c));
        std::string gp="DataSetInfo/Channel "+std::to_string(c); if(H5Lexists(out,gp.c_str(),H5P_DEFAULT)<=0) continue;
        hid_t g=H5Gopen2(out,gp.c_str(),H5P_DEFAULT);                        // per-channel name/wavelengths/color/range
        ok&=setChannelMeta(g,m.meta,c);
        std::string cr=partColorRange(partDir,c); if(!cr.empty()) ok&=setStrAttrOW(g,"ColorRange",cr);
        if(!cr.empty()) sscanf(cr.c_str(),"%f %f",&display.back().rangeMin,&display.back().rangeMax);
        H5Gclose(g); }
    if(ok&&H5Lexists(out,"DataSetInfo/TimeInfo",H5P_DEFAULT)>0) ok&=H5Ldelete(out,"DataSetInfo/TimeInfo",H5P_DEFAULT)>=0;  // rebuild TimeInfo for nT (part had 1)
    if(ok) ok&=writeTimeInfo(out,m.meta,lcpl,iota0(nT));                   // the manifest's time stamps, as the converter writes them
    if(ok&&h5PathExists(out,"/DataSetInfo/Image")){ hid_t g=H5Gopen2(out,"DataSetInfo/Image",H5P_DEFAULT);
        ok&=g>=0&&setStrAttrOW(g,"RecordingDate",imsmeta::timeString(m.meta.times[0])); if(g>=0) H5Gclose(g); }
    if(ok){ hid_t g=H5Gcreate2(out,"DataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); ok&=g>=0; if(g>=0) H5Gclose(g); }
    int made=0;
    for(int r=0;ok&&r<nR;r++)for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){
        std::string linkPath="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
        std::string target="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint 0/Channel 0";
        std::string pf=(partRel.empty()?std::string():partRel+"/")+partName(t,c);
        if(H5Lcreate_external(pf.c_str(),target.c_str(),out,linkPath.c_str(),lcpl,H5P_DEFAULT)<0) failed++; else made++; }
    if(ok&&!failed){   // the thumbnail, from the parts' voxels; the extent from the copied /DataSetInfo/Image
        float ext[3]={0,0,0}; hid_t ig=H5Lexists(out,"DataSetInfo/Image",H5P_DEFAULT)>0?H5Gopen2(out,"DataSetInfo/Image",H5P_DEFAULT):-1;
        if(ig>=0){ for(int a=0;a<3;a++){ const std::string k=std::to_string(a);
                ext[a]=strtof(readStrAttr(ig,("ExtMax"+k).c_str()).c_str(),NULL)-strtof(readStrAttr(ig,("ExtMin"+k).c_str()).c_str(),NULL); }
            H5Gclose(ig); }
        s=writePartsThumbnail(out,partDir,sizes,ext,display);
    }
    H5Pclose(lcpl);
    if(!ok) s=Status::fail("cannot write the link file's metadata (HDF5 error above)");
    else if(failed) s=Status::fail(std::to_string(failed)+" channel copies or external links failed (HDF5 error above)");
    if(finishOutput(out,outPath,s)) return 1;
    printf("linkparts -> %s: %d levels x %d tp x %d ch = %d external links (%d failed); DataSetInfo assembled from parts\n",
           outPath.c_str(),nR,nT,nC,made,failed);
    return 0;
}

// ===================== monitor: wait for every expected tile, check them, then combine =====================
// packs store: every pack must hold exactly its tile's chunks and every histogram must be whole before anything is
// written
static Status checkPacks(const Manifest& m,const std::string& wd){
    const std::string cd=wd+"/chunks";
    for(int r=0;r<(int)m.lev.size();r++) for(int c=0;c<m.nC;c++) for(int t=0;t<m.nT;t++) for(long ti=0;ti<tilesPerCT(m.lev[r]);ti++){
        std::string pp=packName(cd,r,c,t,ti); std::vector<uint64_t> idx;
        Status s=readPackIndex(pp,idx); if(s.ok()) s=checkPackTile(idx,m.lev[r],ti,pp);
        if(!s.ok()) return Status::fail(s.message+" (re-run its worker)");
        std::string hp=histName(cd,r,c,t,ti); struct stat st;
        if(stat(hp.c_str(),&st)!=0||(size_t)st.st_size!=HIST_BYTES) return Status::fail(hp+" is missing or not "+std::to_string(HIST_BYTES)+" bytes (re-run its worker)"); }
    return Status();
}
// A tile is done when its histogram and its pack (packs store) or sidecar (mono store) exist: the worker writes
// that one last. Only the expected names count, so stale or foreign files in chunks/ are ignored.
// PIW_MONITOR_TIMEOUT=<seconds> gives up (exit 1, listing missing tiles) instead of waiting forever for a worker
// that failed. Then the packs store checks every pack and merges them into a new file; the mono store runs
// monofinish (mono.h), which checks every sidecar and builds the index of the placed chunks.
static int doMonitor(const std::string& wd,const std::string& out,const std::vector<std::string>& finishArgs){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    s=tileTypeOk(m,wd); if(!s.ok()) return fail(s);
    const std::string cd=wd+"/chunks"; const bool mono=m.store=="mono";
    struct Want { int r,c,t; long tile; };
    std::vector<Want> want;   // one pack or sidecar per (level,channel,timepoint,tile) = per worker
    for(int r=0;r<(int)m.lev.size();r++) for(int c=0;c<m.nC;c++) for(int t=0;t<m.nT;t++) for(long ti=0;ti<tilesPerCT(m.lev[r]);ti++) want.push_back({r,c,t,ti});
    auto doneName=[&](const Want& w){ return mono?tileIdxName(cd,w.r,w.c,w.t,w.tile):packName(cd,w.r,w.c,w.t,w.tile); };
    double timeout=0; { const char* e=getenv("PIW_MONITOR_TIMEOUT"); if(e&&*e){ char* end=NULL; timeout=strtod(e,&end); if(end==e||*end||timeout<0) return fail(std::string("bad PIW_MONITOR_TIMEOUT '")+e+"' (seconds)"); } }
    std::vector<char> done(want.size(),0); size_t nDone=0; time_t t0=time(NULL);
    for(;;){
        for(size_t i=0;i<want.size();i++){ const Want& w=want[i];
            if(!done[i]&&fileExists(doneName(w))&&fileExists(histName(cd,w.r,w.c,w.t,w.tile))){ done[i]=1; nDone++; } }
        printf("monitor: %llu/%llu tiles\n",(unsigned long long)nDone,(unsigned long long)want.size()); fflush(stdout);
        if(nDone>=want.size()) break;
        double el=difftime(time(NULL),t0);
        if(timeout>0&&el>=timeout){ int shown=0;
            for(size_t i=0;i<want.size()&&shown<10;i++) if(!done[i]){ fprintf(stderr,"missing: %s\n",doneName(want[i]).c_str()); shown++; }
            return fail("monitor gave up after "+std::to_string((long)el)+" s (PIW_MONITOR_TIMEOUT): "+std::to_string(want.size()-nDone)+" tiles missing"); }
        double nap=timeout>0?std::min(5.0,timeout-el):5.0; sleep((unsigned)std::max(1.0,nap));
    }
    if(mono){   // monofinish <workdir> <out> [--verify] [--threads N]: it reports and returns like the command
        printf("all tiles placed -> finishing %s\n",out.c_str()); fflush(stdout);
        std::vector<std::string> a={"parallelimariswriter","monofinish",wd,out}; a.insert(a.end(),finishArgs.begin(),finishArgs.end());
        std::vector<char*> av; for(std::string& x:a) av.push_back(&x[0]);
        return runMono((int)av.size(),av.data());
    }
    if(!finishArgs.empty()) return fail("monitor: --verify and --threads are for the single-file store");
    s=checkPacks(m,wd); if(!s.ok()) return fail(s);
    printf("all tiles present -> merging %s\n",out.c_str());
    return doCombine(m,wd,out);
}

// ===================== setup =====================
static std::vector<std::string> listZarrs(const std::string& folder,const std::string& pattern){
    std::vector<std::string> out; DIR* d=opendir(folder.c_str()); if(!d)return out; struct dirent* e;
    while((e=readdir(d))){ std::string n=e->d_name; if(n.size()>5&&n.substr(n.size()-5)==".zarr"&&n.find(pattern)!=std::string::npos) out.push_back(folder+"/"+n); }
    closedir(d); std::sort(out.begin(),out.end()); return out;
}
static int doSetup(int argc,char** argv){
    std::string folder=argv[2],patterns=argv[3],wd=argv[4];
    // [vx,vy,vz] then --store mono|packs and the converter's metadata options (--name value or --name=value);
    // store default: env PIW_STORE, else mono (packs on Windows)
    int argi=6; const char* voxArg=NULL; if(argc>6&&strncmp(argv[6],"--",2)!=0){ voxArg=argv[6]; argi=7; }
    const char* envStore=getenv("PIW_STORE");
    std::string store=envStore&&*envStore?envStore:
#ifdef _WIN32
        "packs";
#else
        "mono";
#endif
    piw::ConvertOptions mo;
    for(;argi<argc;argi++){ std::string a=argv[argi], name, value;
        if(a.compare(0,2,"--")!=0) return usage("setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--store mono|packs] [--option value ...]");
        size_t eq=a.find('='); name=a.substr(2,eq==std::string::npos?std::string::npos:eq-2);
        if(name!="store"&&!imsmeta::isMetadataOption(name)) return fail("unknown setup option --"+name);
        if(eq!=std::string::npos) value=a.substr(eq+1); else if(argi+1<argc) value=argv[++argi]; else return fail("--"+name+" needs a value");
        if(name=="store"){ store=value; continue; }
        std::string e=imsmeta::parseMetadataOption(name,value,mo); if(!e.empty()) return fail(e); }
    if(store!="mono"&&store!="packs") return fail("unknown store '"+store+"' (expected mono or packs)");
#ifdef _WIN32
    if(store=="mono") return fail(NOSUP_MONO);
#endif
    if(wd.find_first_of("/"
#ifdef _WIN32
        "\\:"
#endif
        )==std::string::npos){ std::string f=folder; while(f.size()>1&&f.back()=='/')f.pop_back(); wd=f+"/"+wd; }   // a bare name: inside the input folder
    bpSize cap[3]={0,0,0}; sscanf(argv[5],"%llu,%llu,%llu",(unsigned long long*)&cap[0],(unsigned long long*)&cap[1],(unsigned long long*)&cap[2]);
    if(!cap[0]||!cap[1]||!cap[2]){ fprintf(stderr,"bad tile size '%s' (expected X,Y,Z voxels)\n",argv[5]); return 1; }
    std::vector<std::string> pats; { std::stringstream ss(patterns); std::string p; while(std::getline(ss,p,',')) if(!p.empty()) pats.push_back(p); }
    if(pats.empty()){ fprintf(stderr,"no channel patterns given\n"); return 1; }
    Manifest m; m.nC=(int)pats.size(); m.store=store; for(int a=0;a<3;a++) m.cap[a]=cap[a];
    std::vector<std::vector<std::string>> perCh;
    for(auto&p:pats){ auto zs=listZarrs(folder,p); if(zs.empty()){ fprintf(stderr,"no .zarr matching '%s' in %s\n",p.c_str(),folder.c_str()); return 1; } perCh.push_back(zs); }
    m.nT=(int)perCh[0].size();
    for(int c=0;c<m.nC;c++) if((int)perCh[c].size()!=m.nT){ fprintf(stderr,"channel '%s' has %zu timepoints, expected %d\n",pats[c].c_str(),perCh[c].size(),m.nT); return 1; }
    m.zarrs.resize((size_t)m.nC*m.nT); for(int c=0;c<m.nC;c++)for(int t=0;t<m.nT;t++) m.zarrs[(size_t)c*m.nT+t]=perCh[c][t];
    std::string ztype;
    try{ zarr Z(zarrAt(m,0,0).c_str()); m.X=Z.get_shape(0); m.Y=Z.get_shape(1); m.Z=Z.get_shape(2); m.elem=Z.dtypeBytes(); ztype=Z.get_dtype(); }
    catch(const std::string& e){ return fail("cannot open "+zarrAt(m,0,0)+": "+e); }
    catch(const std::exception& e){ return fail("cannot open "+zarrAt(m,0,0)+": "+e.what()); }
    if(!m.X||!m.Y||!m.Z) return fail(zarrAt(m,0,0)+" has an empty shape");
    {   // the sample type from the dtype (NumPy's typestr after a byte-order mark): uint8, uint16, uint32 or float32
        std::string tk=ztype; if(!tk.empty()&&(tk[0]=='<'||tk[0]=='>'||tk[0]=='|')) tk.erase(0,1);
        m.dtype=tk=="u1"?"uint8":tk=="u2"?"uint16":tk=="u4"?"uint32":tk=="f4"?"float32":"";
        if(m.dtype.empty()) return fail(zarrAt(m,0,0)+" has samples of type '"+ztype+"'; only uint8, uint16, uint32 and float32 data are supported");
    }
    m.vox[0]=m.vox[1]=m.vox[2]=0.108f;   // the single-node converter's default
    if(voxArg&&(sscanf(voxArg,"%g,%g,%g",&m.vox[0],&m.vox[1],&m.vox[2])!=3||!(m.vox[0]>0&&m.vox[1]>0&&m.vox[2]>0&&std::isfinite(m.vox[0])&&std::isfinite(m.vox[1])&&std::isfinite(m.vox[2]))))
        return fail(std::string("bad voxel size '")+voxArg+"' (expected vx,vy,vz > 0)");
    // the metadata, resolved as the single-node converter resolves it (channel names default to the patterns)
    mo.files=perCh; if(mo.channelNames.empty()) mo.channelNames=pats;
    { std::string e=imsmeta::resolveMetadata(mo,m.meta); if(!e.empty()) return fail(e); }
    bpSize chunkBytes=chunkBudgetBytes();   // only the BLOCK budget changes; the pyramid (level count) is unchanged
    m.lev=makeLevels(m.X,m.Y,m.Z,m.elem,cap,chunkBytes,(bpSize)m.nT);   // the block layout of an nT-timepoint file
    Status s=makeDirs(wd+"/chunks"); if(!s.ok()) return fail(s);
    s=writeManifest(wd,m); if(!s.ok()) return fail(s);
    long total=0; printf("setup: %llux%llux%llu elem=%llu, %d channels, %d timepoints, %zu levels, %s store\n",(unsigned long long)m.X,(unsigned long long)m.Y,(unsigned long long)m.Z,(unsigned long long)m.elem,m.nC,m.nT,m.lev.size(),
                         m.store=="mono"?"single-file":"packs");
    printf("  workdir: %s\n  tile: %llu,%llu,%llu voxels\n  chunk budget %llu bytes -> L0 block %llu,%llu,%llu voxels\n",wd.c_str(),(unsigned long long)cap[0],(unsigned long long)cap[1],(unsigned long long)cap[2],(unsigned long long)chunkBytes,(unsigned long long)m.lev[0].block[0],(unsigned long long)m.lev[0].block[1],(unsigned long long)m.lev[0].block[2]);
    printf("  run one worker array PER LEVEL, in order (level r+1 after level r):\n");
    for(int r=0;r<(int)m.lev.size();++r){ long jr=jobsAtLevel(m,r); total+=jr; printf("    level %d: worker <workdir> %d <index> [count]   index 0..%ld  (%lld tiles/(ch*tp))\n",r,r,jr-1,(long long)((long long)m.lev[r].nt[0]*m.lev[r].nt[1]*m.lev[r].nt[2])); }
    printf("  total worker jobs across all levels: %ld\n",total);
    printf("  then: monitor <workdir> [out.ims]  (%s)\n",m.store=="mono"?"indexes the chunks the workers placed in the single file":"merges the packs");
    if(tileTypeOk(m,wd).ok()==false) fprintf(stderr,"WARNING: %s; this %s input can only go through per-(timepoint,channel) converter jobs + monoplace or linkparts\n",UINT16_ONLY,m.dtype.c_str());
    return 0;
}

// ===================== the tile steps as library calls (src/cluster.cpp) =====================
namespace piw { namespace detail {
std::mutex& libraryMutex();   // src/converter.cpp
piw::Status tileTask(const std::string& wd,int level,long index,long count,int threads,std::vector<std::string>* lines) noexcept{
    try{
        Manifest m; imscore::Status s=readManifest(wd,m); if(!s.ok()) return piw::Status(piw::Code::InvalidArgument,s.message);
        s=tileTypeOk(m,wd); if(!s.ok()) return piw::Status(piw::Code::Unsupported,s.message);
        if(level<0||level>=(int)m.lev.size()) return piw::Status(piw::Code::InvalidArgument,"level "+std::to_string(level)+" out of range [0,"+std::to_string(m.lev.size())+")");
        const long jobs=jobsAtLevel(m,level);
        if(index<0||index>=jobs||count<1) return piw::Status(piw::Code::InvalidArgument,"tiles "+std::to_string(index)+" (+"+std::to_string(count)+") out of range [0,"+std::to_string(jobs)+") for level "+std::to_string(level));
        OmpThreads th(threads);
        s=workTiles(m,wd,level,index,count,true,[lines](const std::string& l){ if(lines) lines->push_back(l); });
        if(s.ok()) return piw::Status();
        return piw::Status(s.message.find(" needs ")!=std::string::npos?piw::Code::NotFound:piw::Code::WriteFailed,s.message);
    }
    catch(const std::exception& e){ return piw::Status(piw::Code::Internal,std::string("unexpected error: ")+e.what()); }
    catch(...){ return piw::Status(piw::Code::Internal,"unexpected error: unknown exception"); }
}
piw::Status packsFinish(const std::string& wd,const std::string& out) noexcept{
    try{
        Manifest m; imscore::Status s=readManifest(wd,m); if(!s.ok()) return piw::Status(piw::Code::InvalidArgument,s.message);
        if(m.elem!=2) return piw::Status(piw::Code::Unsupported,std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
        if(m.store!="packs") return piw::Status(piw::Code::InvalidArgument,wd+" does not use the packs store");
        s=checkPacks(m,wd); if(!s.ok()) return piw::Status(piw::Code::ReadFailed,s.message);
        std::lock_guard<std::mutex> lock(libraryMutex());
        if(!registerLZ4()) return piw::Status(piw::Code::Internal,"cannot register the LZ4 filter id with HDF5");
        s=combinePacks(m,wd,out);
        return s.ok()?piw::Status():piw::Status(piw::Code::WriteFailed,s.message);
    }
    catch(const std::exception& e){ return piw::Status(piw::Code::Internal,std::string("unexpected error: ")+e.what()); }
    catch(...){ return piw::Status(piw::Code::Internal,"unexpected error: unknown exception"); }
}
} }  // namespace piw::detail

bool isParallelCommand(const char* c){ return c && (!strcmp(c,"setup")||!strcmp(c,"worker")||!strcmp(c,"monitor")||!strcmp(c,"combineone")||!strcmp(c,"link")||!strcmp(c,"linkparts")||!strcmp(c,"layout")); }

// Splits argv[2..] into positional arguments and the options --verify and --threads N (monitor).
static bool monitorArgs(int argc,char** argv,std::vector<std::string>& pos,std::vector<std::string>& finishArgs){
    for(int i=2;i<argc;i++){ std::string a=argv[i];
        if(a=="--verify") finishArgs.push_back(a);
        else if(a=="--threads"){ long n; if(i+1>=argc||!parseLong(argv[i+1],n)||n<1) return false; finishArgs.push_back(a); finishArgs.push_back(argv[++i]); }
        else if(a.size()>2&&a.compare(0,2,"--")==0) return false;
        else pos.push_back(a); }
    return true;
}
int runParallel(int argc,char** argv){
    if(!registerLZ4()) return fail("cannot register the LZ4 filter id with HDF5");
    std::string cmd=argc>=2?argv[1]:"";
    if(cmd=="layout"&&argc>=5){ Vec3 img{(bpSize)atoll(argv[2]),(bpSize)atoll(argv[3]),(bpSize)atoll(argv[4])}; bpSize e=argc>5?atoll(argv[5]):2, nT=argc>6?atoll(argv[6]):1;
        if(!img[0]||!img[1]||!img[2]||(e!=1&&e!=2&&e!=4)||nT<1) return fail("layout needs X Y Z > 0, elemSize 1, 2 or 4 and timepoints >= 1");
        bpSize cb=chunkBudgetBytes(); auto ls=GetOptimalImagePyramid((1024*1024)/e,img,true),bs=GetOptimalBlockSizes(cb/e,ls,nT); printf("chunk budget %llu bytes, %llu timepoints\n",(unsigned long long)cb,(unsigned long long)nT); printf("%zu levels\n",ls.size()); for(size_t i=0;i<ls.size();++i) printf("  L%zu size=%llu,%llu,%llu block=%llu,%llu,%llu\n",i,(unsigned long long)ls[i][0],(unsigned long long)ls[i][1],(unsigned long long)ls[i][2],(unsigned long long)bs[i][0],(unsigned long long)bs[i][1],(unsigned long long)bs[i][2]); return 0; }
    if(cmd=="setup"&&argc>=6) return doSetup(argc,argv);
    if(cmd=="worker"&&argc>=5){ long r,i,n=1; if(!parseLong(argv[3],r)||!parseLong(argv[4],i)||(argc>5&&!parseLong(argv[5],n))||argc>6) return usage("worker <workdir> <level> <index> [count]");
        return doWorker(argv[2],(int)r,i,n); }
    if(cmd=="monitor"){ std::vector<std::string> pos, fa;
        if(!monitorArgs(argc,argv,pos,fa)||pos.empty()||pos.size()>2) return usage("monitor <workdir> [out.ims] [--verify] [--threads N]");
        return doMonitor(pos[0],pos.size()>1?pos[1]:pos[0]+"/output.ims",fa); }
    if(cmd=="combineone"&&argc>=6){ long t,c; if(!parseLong(argv[3],t)||!parseLong(argv[4],c)) return fail("combineone needs a numeric timepoint and channel");
        return combineOne(argv[2],(int)t,(int)c,argv[5]); }
    if(cmd=="link"&&argc>=5) return buildLink(argv[2],argv[3],argv[4]);
    if(cmd=="linkparts"&&argc>=5) return linkParts(argv[2],argv[3],argv[4]);
    fprintf(stderr,"usage: parallelimariswriter setup|worker|monitor|combineone|link|linkparts|layout ...  (see parallel.h)\n"); return 2;
}
