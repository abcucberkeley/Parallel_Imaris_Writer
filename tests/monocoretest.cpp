// Mono-core test: the pieces of the single-file cluster output, on synthetic data (no ImarisWriter).
//
//   placement : three small odd-sized volumes (uint16 level 0, uint16 level 1, uint8) are compressed with the
//               project's codec (2-byte shuffle + LZ4 with ImarisWriter's 16-byte header), claimed by four
//               owners and written by the blob writer in reverse/shuffled chunk order, the datasets interleaved
//               in the file. Sidecars are written and read back, the journal sealed, and the HDF5 structure
//               built through the placement VFD. Then, with the stock driver: H5Dchunk_iter addresses and
//               sizes equal the placed ones, H5Dread through a real LZ4 decoder returns the original voxels,
//               the raw region is unchanged and the file ends at the end of allocation. Finishing again over
//               the same raw region gives a valid file again.
//   negatives : wrong armed size, an armed chunk never written, chunks outside the raw region, unsorted and
//               duplicate chunks, a first metadata block too big for the header, opening (not creating) a file
//               through the driver; corrupt, truncated and inconsistent sidecars.
//   claims    : both mechanisms (link, fcntl): 8 forked processes x 200 claims -> a gap-free journal holding
//               every process's claims once; idempotent re-claims, a size mismatch, a claimer that died before
//               writing its receipt, find() without a receipt, the seal (idempotent; later claims fail).
//
//   usage: monoCoreTest <workdir>
//          monoCoreTest --bench <file> <cz,cy,cx> [-v] <count:nz,ny,nx>...   index-pass benchmark: count
//              datasets of nz*ny*nx chunks per argument (one argument per level); fake chunk sizes; -v reopens
//              with the stock driver and checks every chunk address and size
//          monoCoreTest --claims-run <link|fcntl> <dir> <resdir> <nproc> <nclaims> <gidBase> <startEpoch>
//          monoCoreTest --claims-check <dir> <resdir> <expected>
//              claim stress across nodes: run --claims-run on each node (distinct gidBase, same start time),
//              then --claims-check once
//          monoCoreTest --place-bench <file> <GB> <threads> [pieceMiB] [chunkKB]   blob-writer throughput
// Exit 0 = pass, 1 = fail, 77 = skipped (Windows: the single-file path is not supported there).
#ifdef _WIN32
#include <cstdio>
int main(){ std::printf("monoCoreTest: skipped (the single-file cluster output is not supported on Windows)\n"); return 77; }
#else
#include "monoclaim.h"
#include "monoidx.h"
#include "monovfd.h"
#include "hdf5.h"
#include <lz4.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <vector>
#include <dirent.h>
#include <fcntl.h>
#include <ftw.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static int gFail=0;
#define CHECK(cond,...) do{ if(!(cond)){ gFail++; std::printf("  FAIL (line %d): ",__LINE__); std::printf(__VA_ARGS__); std::printf("\n"); std::fflush(stdout); } }while(0)

static double wall(){ return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count(); }
static double secs(){ return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
static int rmCb(const char* p,const struct stat*,int,struct FTW*){ return remove(p); }
static void rmrf(const std::string& p){ nftw(p.c_str(),rmCb,64,FTW_DEPTH|FTW_PHYS); }
static void mkdirp(const std::string& p){ for(size_t i=1;i<=p.size();i++) if(i==p.size()||p[i]=='/') mkdir(p.substr(0,i).c_str(),0777); }
static uint64_t fileSize(const std::string& p){ struct stat st; return stat(p.c_str(),&st)==0?(uint64_t)st.st_size:0; }
static uint64_t hashRange(const std::string& p,uint64_t off,uint64_t len){
    int fd=open(p.c_str(),O_RDONLY); if(fd<0) return 0;
    std::vector<uint8_t> b(1<<20); uint64_t h=14695981039346656037ull;
    while(len>0){ size_t n=(size_t)std::min<uint64_t>(len,b.size()); ssize_t k=pread(fd,b.data(),n,(off_t)off); if(k<=0) break; h=mono_fnv1a(b.data(),(size_t)k,h); off+=(uint64_t)k; len-=(uint64_t)k; }
    close(fd); return h;
}
struct QuietHdf5 {   // HDF5 error printing off for expected failures
    H5E_auto2_t fn; void* data;
    QuietHdf5(){ H5Eget_auto2(H5E_DEFAULT,&fn,&data); H5Eset_auto2(H5E_DEFAULT,NULL,NULL); }
    ~QuietHdf5(){ H5Eset_auto2(H5E_DEFAULT,fn,data); }
};

// ---------------- the project's chunk codec ----------------
static void wbe(uint8_t* d,uint64_t v,int nb){ for(int i=0;i<nb;i++) d[nb-1-i]=(uint8_t)((v>>(8*i))&0xff); }
static uint64_t rbe(const uint8_t* d,int nb){ uint64_t v=0; for(int i=0;i<nb;i++) v=(v<<8)|d[i]; return v; }
static std::vector<uint8_t> encodeChunk(const uint8_t* vox,size_t nb,unsigned elem){
    std::vector<uint8_t> sh(nb);
    if(elem==2){ size_t h=nb/2; for(size_t i=0;i<h;i++){ sh[i]=vox[2*i]; sh[h+i]=vox[2*i+1]; } }
    else memcpy(sh.data(),vox,nb);
    int bd=LZ4_compressBound((int)nb); std::vector<uint8_t> out(16+(size_t)bd);
    int c=LZ4_compress_default((const char*)sh.data(),(char*)out.data()+16,(int)nb,bd);
    wbe(out.data(),nb,8); wbe(out.data()+8,nb,4); wbe(out.data()+12,(uint64_t)c,4); out.resize(16+(size_t)c);
    return out;
}
// A real decoder for HDF5 filter 32004 (ImarisWriter's LZ4 format: u64 size, u32 block size, then blocks of
// u32 compressed size + data; a block whose compressed size equals its size is stored raw).
static size_t lz4Filter(unsigned flags,size_t,const unsigned*,size_t nbytes,size_t* bufSize,void** buf){
    if(!(flags&H5Z_FLAG_REVERSE)||nbytes<12) return 0;
    const uint8_t* in=(const uint8_t*)*buf; uint64_t total=rbe(in,8); uint64_t block=rbe(in+8,4);
    if(!block) return 0;
    uint8_t* out=(uint8_t*)H5allocate_memory(total?(size_t)total:1,false); size_t pos=12; uint64_t done=0;
    while(done<total){
        if(pos+4>nbytes){ H5free_memory(out); return 0; }
        uint64_t cs=rbe(in+pos,4), bl=std::min(block,total-done); pos+=4;
        if(pos+cs>nbytes){ H5free_memory(out); return 0; }
        if(cs==bl) memcpy(out+done,in+pos,(size_t)bl);
        else if(LZ4_decompress_safe((const char*)in+pos,(char*)out+done,(int)cs,(int)bl)!=(int)bl){ H5free_memory(out); return 0; }
        pos+=(size_t)cs; done+=bl;
    }
    H5free_memory(*buf); *buf=out; *bufSize=(size_t)total; return (size_t)total;
}
static const H5Z_class2_t REAL_LZ4={H5Z_CLASS_T_VERS,(H5Z_filter_t)32004,1,1,"lz4 decoder (test)",NULL,NULL,(H5Z_func_t)lz4Filter};

// ---------------- synthetic volumes ----------------
struct Vol {
    std::string group; unsigned dtype, elem; uint16_t level; int t, c; MonoGeom g;
    std::vector<uint8_t> data;                 // x fastest
    std::vector<std::vector<uint8_t>> comp;    // compressed chunk k = cx + gx*(cy + gy*cz)
};
static Vol makeVol(const char* group,unsigned dtype,uint16_t level,int t,int c,const uint64_t size[3],const uint32_t chunk[3],int seed){
    Vol v; v.group=group; v.dtype=dtype; v.elem=mono_dtype_bytes(dtype); v.level=level; v.t=t; v.c=c;
    for(int a=0;a<3;a++){ v.g.size[a]=size[a]; v.g.chunk[a]=chunk[a]; v.g.grid[a]=(uint32_t)((size[a]+chunk[a]-1)/chunk[a]); }
    v.g.dtype=dtype;
    uint64_t X=size[0],Y=size[1],Z=size[2]; v.data.resize((size_t)(X*Y*Z*v.elem));
    for(uint64_t z=0;z<Z;z++) for(uint64_t y=0;y<Y;y++) for(uint64_t x=0;x<X;x++){
        size_t i=(size_t)(x+X*(y+Y*z));
        if(v.elem==2){ uint16_t val=(uint16_t)((x*7+y*13+z*17+(uint64_t)seed*137)%4096+1); memcpy(&v.data[2*i],&val,2); }
        else v.data[i]=(uint8_t)((x+3*y+5*z+(uint64_t)seed)%251); }
    const uint32_t* cd=chunk; size_t cb=(size_t)cd[0]*cd[1]*cd[2]*v.elem;
    for(uint32_t gz=0;gz<v.g.grid[2];gz++) for(uint32_t gy=0;gy<v.g.grid[1];gy++) for(uint32_t gx=0;gx<v.g.grid[0];gx++){
        std::vector<uint8_t> b(cb,0);                           // zero beyond the image, like the writer
        for(uint32_t z=0;z<cd[2];z++) for(uint32_t y=0;y<cd[1];y++){
            uint64_t X0=(uint64_t)gx*cd[0], Yg=(uint64_t)gy*cd[1]+y, Zg=(uint64_t)gz*cd[2]+z;
            if(Yg>=Y||Zg>=Z||X0>=X) continue;
            uint64_t nx=std::min<uint64_t>(cd[0],X-X0);
            memcpy(&b[((size_t)z*cd[1]+y)*cd[0]*v.elem],&v.data[(size_t)(X0+X*(Yg+Y*Zg))*v.elem],(size_t)nx*v.elem); }
        v.comp.push_back(encodeChunk(b.data(),b.size(),v.elem)); }
    return v;
}
static MonoChunk chunkOf(const Vol& v,size_t k,uint64_t offset){
    MonoChunk m=MonoChunk(); uint32_t gx=v.g.grid[0],gy=v.g.grid[1];
    m.cx=(uint32_t)(k%gx); m.cy=(uint32_t)((k/gx)%gy); m.cz=(uint32_t)(k/((size_t)gx*gy));
    m.offset=offset; m.size=(uint32_t)v.comp[k].size(); m.level=v.level; return m;
}
static MonoIdxLevel levelOf(const Vol& v,uint64_t n){
    MonoIdxLevel l; l.level=v.level; l.dtype=v.dtype;
    for(int a=0;a<3;a++){ l.size[a]=v.g.size[a]; l.chunk[a]=v.g.chunk[a]; l.grid[a]=v.g.grid[a]; }
    l.nChunks=n; return l;
}

// Builds the HDF5 structure over the raw region: per volume a group with attributes, a small contiguous
// "Histogram" dataset (an un-armed raw write that must pass through) and the indexed Data dataset.
static bool finishFile(const std::string& path,uint64_t rawEnd,const std::vector<Vol>& vols,std::vector<std::vector<MonoChunk>>& chunks,
                       MonoVfdStats* st,std::string* err){
    MonoVfd vfd(rawEnd);
    hid_t fapl=mono_vfd_fapl(vfd,err);
    if(fapl<0) return false;
    hid_t f=H5Fcreate(path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl);
    if(f<0){ *err="H5Fcreate through the placement driver failed: "+vfd.err; return false; }
    bool ok=true;
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    for(size_t i=0;i<vols.size()&&ok;i++){ const Vol& v=vols[i];
        hid_t g=H5Gcreate2(f,v.group.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
        std::string sx=std::to_string(v.g.size[0]); hsize_t d=sx.size();
        hid_t sp=H5Screate_simple(1,&d,NULL), ty=H5Tcopy(H5T_C_S1), a=H5Acreate2(g,"ImageSizeX",ty,sp,H5P_DEFAULT,H5P_DEFAULT);
        H5Awrite(a,ty,sx.data()); H5Aclose(a); H5Tclose(ty); H5Sclose(sp);
        std::vector<uint64_t> hist(256); for(size_t b=0;b<hist.size();b++) hist[b]=b*1000+i;
        hsize_t hn=hist.size(); hid_t hs=H5Screate_simple(1,&hn,NULL);
        hid_t hd=H5Dcreate2(g,"Histogram",H5T_STD_U64LE,hs,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT);
        ok=hd>=0&&H5Dwrite(hd,H5T_NATIVE_UINT64,H5S_ALL,H5S_ALL,H5P_DEFAULT,hist.data())>=0;
        if(hd>=0) H5Dclose(hd);
        H5Sclose(hs);
        if(!ok) *err="cannot write the histogram dataset";
        if(ok) ok=mono_index_dataset(vfd,g,"Data",v.g,chunks[i],err);
        H5Gclose(g); }
    H5Pclose(lcpl);
    if(H5Fclose(f)<0&&ok){ ok=false; *err="H5Fclose failed: "+vfd.err; }
    std::string e;
    if(ok&&!mono_vfd_ok(vfd,&e)){ ok=false; *err=e; }
    if(st) *st=mono_vfd_stats(vfd);
    return ok;
}
// Stock-driver check: chunk index, voxels through the real decoder, histograms.
static void verifyFile(const std::string& path,const std::vector<Vol>& vols,std::vector<std::vector<MonoChunk>>& chunks){
    H5Zregister(&REAL_LZ4);
    hid_t f=H5Fopen(path.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT);
    CHECK(f>=0,"stock driver cannot open %s",path.c_str());
    if(f<0) return;
    for(size_t i=0;i<vols.size();i++){ const Vol& v=vols[i];
        hid_t d=H5Dopen2(f,(v.group+"/Data").c_str(),H5P_DEFAULT);
        CHECK(d>=0,"%s/Data missing",v.group.c_str());
        if(d<0) continue;
        uint64_t bad=0; std::string e;
        CHECK(mono_verify_dataset(d,v.g,chunks[i],&bad,&e),"%s: chunk index differs from the placed chunks: %s",v.group.c_str(),e.c_str());
        size_t pad=(size_t)v.g.chunk[0]*v.g.grid[0], pady=(size_t)v.g.chunk[1]*v.g.grid[1], padz=(size_t)v.g.chunk[2]*v.g.grid[2];
        std::vector<uint8_t> got(pad*pady*padz*v.elem,0xEE);
        bool rd=H5Dread(d,v.elem==2?H5T_NATIVE_UINT16:H5T_NATIVE_UINT8,H5S_ALL,H5S_ALL,H5P_DEFAULT,got.data())>=0;
        CHECK(rd,"%s: H5Dread through the LZ4 decoder failed",v.group.c_str());
        uint64_t wrong=0,nonzeroPad=0;
        for(size_t z=0;rd&&z<padz;z++) for(size_t y=0;y<pady;y++) for(size_t x=0;x<pad;x++){
            const uint8_t* q=&got[((z*pady+y)*pad+x)*v.elem];
            if(x<v.g.size[0]&&y<v.g.size[1]&&z<v.g.size[2]){ if(memcmp(q,&v.data[(x+v.g.size[0]*(y+v.g.size[1]*z))*v.elem],v.elem)) wrong++; }
            else for(unsigned b=0;b<v.elem;b++) if(q[b]) { nonzeroPad++; break; } }
        CHECK(wrong==0&&nonzeroPad==0,"%s: %llu wrong voxels, %llu non-zero padding voxels",v.group.c_str(),(unsigned long long)wrong,(unsigned long long)nonzeroPad);
        H5Dclose(d);
        hid_t hd=H5Dopen2(f,(v.group+"/Histogram").c_str(),H5P_DEFAULT); std::vector<uint64_t> h(256,7);
        bool hok=hd>=0&&H5Dread(hd,H5T_NATIVE_UINT64,H5S_ALL,H5S_ALL,H5P_DEFAULT,h.data())>=0;
        for(size_t b=0;hok&&b<h.size();b++) hok=h[b]==b*1000+i;
        CHECK(hok,"%s: histogram dataset differs",v.group.c_str());
        if(hd>=0) H5Dclose(hd); }
    H5Fclose(f);
}

// ---------------- placement end to end ----------------
static void testPlacement(const std::string& work){
    std::printf("[placement] synthetic volumes -> claims -> blobs -> sidecars -> placement VFD -> stock driver\n");
    std::string dir=work+"/place", mono=dir+"/mono", file=dir+"/output.ims.partial"; mkdirp(mono);
    const uint64_t sA[3]={149,129,101}, sB[3]={75,65,51}, sC[3]={37,29,23};
    const uint32_t cA[3]={64,64,16}, cB[3]={32,32,32}, cC[3]={16,16,8};
    std::vector<Vol> vols;
    vols.push_back(makeVol("/DataSet/ResolutionLevel 0/TimePoint 0/Channel 0",MONO_U16,0,0,0,sA,cA,0));
    vols.push_back(makeVol("/DataSet/ResolutionLevel 1/TimePoint 0/Channel 0",MONO_U16,1,0,0,sB,cB,1));
    vols.push_back(makeVol("/DataSet/ResolutionLevel 0/TimePoint 0/Channel 1",MONO_U8,0,0,1,sC,cC,2));
    // four owners, claimed in this order: A's even chunks reversed, B shuffled, A's odd chunks reversed, C shuffled
    struct Own { std::string name; size_t vol; std::vector<size_t> order; };
    std::vector<Own> owners(4); std::mt19937 rng(12345);
    owners[0].name="P_A1"; owners[0].vol=0; owners[1].name="P_B"; owners[1].vol=1; owners[2].name="P_A2"; owners[2].vol=0; owners[3].name="P_C"; owners[3].vol=2;
    for(size_t k=vols[0].comp.size();k-->0;){ (k%2?owners[2]:owners[0]).order.push_back(k); }
    for(size_t k=0;k<vols[1].comp.size();k++) owners[1].order.push_back(k);
    for(size_t k=0;k<vols[2].comp.size();k++) owners[3].order.push_back(k);
    std::shuffle(owners[1].order.begin(),owners[1].order.end(),rng); std::shuffle(owners[3].order.begin(),owners[3].order.end(),rng);
    MonoClaims cl; std::string e;
    CHECK(cl.open(mono,"link",&e),"claims open: %s",e.c_str());
    std::vector<std::vector<MonoChunk>> perVol(vols.size());
    size_t total=0; uint64_t placedBytes=0;
    for(const Own& o:owners){ const Vol& v=vols[o.vol];
        uint64_t blob=0; for(size_t k:o.order) blob+=v.comp[k].size();
        MonoClaim c; bool ok=cl.claim(o.name,blob,c,&e);
        CHECK(ok,"claim %s: %s",o.name.c_str(),e.c_str());
        if(!ok) return;
        MonoBlobWriter w;                                   // 12 KiB pieces from 3 threads: many pieces per blob
        ok=w.open(file,c.offset,c.size,3,&e,12288);
        CHECK(ok,"blob writer open: %s",e.c_str());
        MonoIdx x; x.owner=o.name; x.timepoint=v.t; x.channel=v.c; x.claimOffset=c.offset; x.claimSize=c.size;
        uint64_t pos=0;
        for(size_t k:o.order){ ok=ok&&w.append(v.comp[k].data(),v.comp[k].size(),&e); x.chunks.push_back(chunkOf(v,k,c.offset+pos)); pos+=v.comp[k].size(); }
        ok=ok&&w.finish(&e);
        CHECK(ok&&w.bytes()==blob,"placing %s: %s",o.name.c_str(),e.c_str());
        x.blobBytes=pos; x.levels.push_back(levelOf(v,x.chunks.size()));
        CHECK(mono_idx_write(mono+"/"+o.name+".idx",x,&e),"idx write %s: %s",o.name.c_str(),e.c_str());
        total+=x.chunks.size(); placedBytes+=blob;
        std::printf("  %-5s %3zu chunks, %7llu bytes at %llu (%s)\n",o.name.c_str(),x.chunks.size(),(unsigned long long)blob,(unsigned long long)c.offset,w.direct()?"direct":"buffered"); }
    // the finisher's side: sidecars back, checked against the journal
    std::vector<MonoClaim> journal; bool sealed=true;
    CHECK(cl.list(journal,&sealed,&e)&&journal.size()==owners.size()&&!sealed,"journal: %s",e.c_str());
    for(size_t i=0;i<owners.size();i++){ MonoIdx x;
        bool ok=mono_idx_read(mono+"/"+owners[i].name+".idx",x,&e);
        CHECK(ok,"idx read %s: %s",owners[i].name.c_str(),e.c_str());
        if(!ok) continue;
        CHECK(i<journal.size()&&journal[i].owner==x.owner&&journal[i].offset==x.claimOffset&&journal[i].size==x.claimSize,"sidecar %s does not match its claim",x.owner.c_str());
        CHECK(mono_idx_validate(x,owners[i].name!="P_A1"&&owners[i].name!="P_A2",&e),"idx validate %s: %s",x.owner.c_str(),e.c_str());
        for(const MonoChunk& c:x.chunks) perVol[owners[i].vol].push_back(c); }
    MonoClaim s;
    CHECK(cl.seal(s,&e)&&s.seal,"seal: %s",e.c_str());
    uint64_t rawEnd=s.offset;
    CHECK(journal.size()==4&&rawEnd==journal[3].offset+journal[3].size,"seal offset %llu is not the end of the last claim",(unsigned long long)rawEnd);
    CHECK(fileSize(file)==rawEnd,"after placement the file is %llu bytes, raw region ends at %llu",(unsigned long long)fileSize(file),(unsigned long long)rawEnd);
    uint64_t rawHash=hashRange(file,MONO_RAW_BEGIN,rawEnd-MONO_RAW_BEGIN);
    // finish twice: the second run rebuilds the structure over the same raw region
    for(int run=0;run<2;run++){
        MonoVfdStats st; double t0=secs(); bool ok=finishFile(file,rawEnd,vols,perVol,&st,&e);
        CHECK(ok,"finish %d: %s",run,e.c_str());
        std::printf("  finish %d: %.3f s, %llu chunks placed/%llu discarded, %llu pass-through writes (%llu raw), %llu failures, file %llu bytes = EOA %llu\n",
            run,secs()-t0,(unsigned long long)st.placed,(unsigned long long)st.discarded,(unsigned long long)st.writes,(unsigned long long)st.rawWrites,
            (unsigned long long)st.failures,(unsigned long long)fileSize(file),(unsigned long long)st.eoa);
        CHECK(st.placed==total&&st.discarded==total&&st.armed==total&&st.failures==0,"driver counts (placed %llu, discarded %llu, expected %zu)",(unsigned long long)st.placed,(unsigned long long)st.discarded,total);
        CHECK(st.rawWrites>=vols.size(),"the histogram writes did not pass through");
        CHECK(fileSize(file)==st.eoa&&st.eoa>rawEnd,"file size %llu != EOA %llu",(unsigned long long)fileSize(file),(unsigned long long)st.eoa);
        CHECK(hashRange(file,MONO_RAW_BEGIN,rawEnd-MONO_RAW_BEGIN)==rawHash,"the raw region changed");
        uint8_t sig[8]={0}; int fd=open(file.c_str(),O_RDONLY); if(fd>=0){ ssize_t r=pread(fd,sig,8,0); (void)r; close(fd); }
        CHECK(!memcmp(sig,"\x89HDF\r\n\x1a\n",8),"no HDF5 signature at offset 0");
        verifyFile(file,vols,perVol); }
    std::printf("  %zu chunks, %llu bytes placed in 4 blobs\n",total,(unsigned long long)placedBytes);
}

// ---------------- driver and sidecar failures ----------------
static void testNegative(const std::string& work){
    std::printf("[negative] driver rule violations and bad sidecars\n");
    std::string dir=work+"/neg"; mkdirp(dir);
    const uint64_t sz[3]={37,29,23}; const uint32_t ch[3]={16,16,8};
    Vol v=makeVol("/D",MONO_U8,0,0,0,sz,ch,5);
    std::string file=dir+"/neg.bin";
    uint64_t a0=MONO_RAW_BEGIN, a1=a0+mono_round_up(v.comp[0].size()), rawEnd=a1+mono_round_up(v.comp[1].size());
    { int fd=open(file.c_str(),O_RDWR|O_CREAT|O_TRUNC,0666); bool ok=fd>=0&&pwrite(fd,v.comp[0].data(),v.comp[0].size(),(off_t)a0)==(ssize_t)v.comp[0].size()
          &&pwrite(fd,v.comp[1].data(),v.comp[1].size(),(off_t)a1)==(ssize_t)v.comp[1].size()&&ftruncate(fd,(off_t)rawEnd)==0; if(fd>=0) close(fd);
      CHECK(ok,"cannot write %s",file.c_str()); }
    QuietHdf5 quiet; std::string e;
    {   MonoVfd s(rawEnd); hid_t fapl=mono_vfd_fapl(s,&e), f=H5Fcreate(file.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
        hid_t d=mono_create_dataset(f,"D",v.g,&e); std::vector<uint8_t> z(v.comp[0].size()+1,0); hsize_t off[3]={0,0,0};
        mono_vfd_arm(s,a0,v.comp[0].size()+1);
        CHECK(f>=0&&d>=0&&H5Dwrite_chunk(d,H5P_DEFAULT,0,off,v.comp[0].size(),z.data())<0&&!mono_vfd_ok(s,&e)&&e.find("allocates")!=std::string::npos,
              "a wrong armed size was not refused (%s)",e.c_str());
        mono_vfd_disarm(s);
        if(d>=0) H5Dclose(d);
        if(f>=0) H5Fclose(f);
        H5Pclose(fapl); }
    {   MonoVfd s(rawEnd); hid_t fapl=mono_vfd_fapl(s,&e), f=H5Fcreate(file.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
        mono_vfd_arm(s,a0,v.comp[0].size()); mono_vfd_arm(s,a1,v.comp[1].size());
        CHECK(!mono_vfd_ok(s,&e)&&e.find("never written")!=std::string::npos,"an armed chunk that was never written went unnoticed");
        mono_vfd_disarm(s); if(f>=0) H5Fclose(f); H5Pclose(fapl); }
    {   MonoVfd s(rawEnd); hid_t fapl=mono_vfd_fapl(s,&e), f=H5Fcreate(file.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
        std::vector<MonoChunk> c0(1,chunkOf(v,0,4096));                    // in the header region
        CHECK(!mono_index_dataset(s,f,"D0",v.g,c0,&e)&&e.find("raw region")!=std::string::npos,"a chunk in the header region was accepted");
        std::vector<MonoChunk> c1(1,chunkOf(v,0,rawEnd-8));                // runs past rawEnd
        CHECK(!mono_index_dataset(s,f,"D1",v.g,c1,&e)&&e.find("raw region")!=std::string::npos,"a chunk beyond the raw region was accepted");
        MonoChunk ab[2]={chunkOf(v,1,a1),chunkOf(v,0,a0)};
        CHECK(!mono_index_dataset(s,f,"D2",v.g,2,[&](size_t k,MonoChunk& c){ c=ab[k]; },&e)&&e.find("sorted")!=std::string::npos,"unsorted chunks were accepted");
        MonoChunk dup[2]={chunkOf(v,0,a0),chunkOf(v,0,a1)};
        CHECK(!mono_index_dataset(s,f,"D3",v.g,2,[&](size_t k,MonoChunk& c){ c=dup[k]; },&e),"a chunk listed twice was accepted");
        MonoChunk out=chunkOf(v,0,a0); out.cx=v.g.grid[0];
        CHECK(!mono_index_dataset(s,f,"D4",v.g,1,[&](size_t,MonoChunk& c){ c=out; },&e)&&e.find("grid")!=std::string::npos,"a chunk outside the grid was accepted");
        std::vector<MonoChunk> good; good.push_back(chunkOf(v,1,a1)); good.push_back(chunkOf(v,0,a0));
        CHECK(mono_index_dataset(s,f,"D5",v.g,good,&e),"two valid chunks were refused: %s",e.c_str());
        CHECK(f>=0&&H5Fclose(f)>=0&&mono_vfd_ok(s,&e),"close after refused inputs: %s",e.c_str());
        H5Pclose(fapl);
        hid_t r=H5Fopen(file.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); hid_t d=r>=0?H5Dopen2(r,"D5",H5P_DEFAULT):-1; uint64_t bad=1;
        CHECK(d>=0&&mono_verify_dataset(d,v.g,good,&bad,&e)&&bad==0,"the valid dataset next to the refused ones is wrong: %s",e.c_str());
        if(d>=0) H5Dclose(d);
        if(r>=0) H5Fclose(r); }
    {   MonoVfd s(rawEnd); hid_t fapl=mono_vfd_fapl(s,&e); H5Pset_meta_block_size(fapl,2u<<20);
        hid_t f=H5Fcreate(file.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
        CHECK(f<0&&!mono_vfd_ok(s,&e)&&e.find("header region")!=std::string::npos,"a first metadata block larger than the header region was accepted");
        if(f>=0) H5Fclose(f);
        H5Pclose(fapl); }
    {   MonoVfd s(rawEnd); hid_t fapl=mono_vfd_fapl(s,&e);
        hid_t f=H5Fopen(file.c_str(),H5F_ACC_RDWR,fapl);
        CHECK(f<0,"the placement driver opened an existing file (it may only create)");
        if(f>=0) H5Fclose(f);
        H5Pclose(fapl); }
    // blob writer: bounds, contents, zero padding; /dev/shm (tmpfs) exercises the buffered fallback on Linux
    {   MonoBlobWriter w;
        CHECK(!w.open(dir+"/blob.bin",MONO_RAW_BEGIN+100,8192,2,&e),"an unaligned blob offset was accepted");
        MonoBlobWriter w2; std::vector<uint8_t> big(8193,1);
        CHECK(w2.open(dir+"/blob.bin",MONO_RAW_BEGIN,8192,2,&e)&&!w2.append(big.data(),big.size(),&e)&&e.find("larger")!=std::string::npos,"a blob larger than its claim was accepted");
        std::vector<std::string> targets(1,dir+"/blob2.bin"); struct stat sb;
        if(stat("/dev/shm",&sb)==0&&S_ISDIR(sb.st_mode)) targets.push_back("/dev/shm/monocoretest_"+std::to_string((long long)getpid())+".bin");
        for(const std::string& t:targets){
            std::vector<uint8_t> a(5000),b(7000); for(size_t i=0;i<a.size();i++) a[i]=(uint8_t)(i*7+1); for(size_t i=0;i<b.size();i++) b[i]=(uint8_t)(i*13+5);
            std::vector<MonoPiece> pieces; MonoPiece p1={a.data(),a.size()}, p2={b.data(),b.size()}; pieces.push_back(p1); pieces.push_back(p2);
            MonoPlaceStats ps; uint64_t off=MONO_RAW_BEGIN+3*MONO_ALIGN;
            bool ok=mono_place(t,off,mono_round_up(a.size()+b.size())+MONO_ALIGN,pieces,2,&ps,&e);
            std::vector<uint8_t> got(12288,0xEE); int fd=open(t.c_str(),O_RDONLY); bool rd=fd>=0&&pread(fd,got.data(),got.size(),(off_t)off)==(ssize_t)got.size(); if(fd>=0) close(fd);
            bool same=rd&&!memcmp(got.data(),a.data(),a.size())&&!memcmp(got.data()+a.size(),b.data(),b.size());
            for(size_t i=a.size()+b.size();same&&i<got.size();i++) same=got[i]==0;
            CHECK(ok&&same&&ps.bytes==a.size()+b.size()&&fileSize(t)==off+12288,"mono_place into %s: %s",t.c_str(),e.c_str());
            std::printf("  mono_place %s: %s\n",t.compare(0,9,"/dev/shm/")?"workdir":"/dev/shm",ps.direct?"O_DIRECT":"buffered (O_DIRECT refused)");
            if(!t.compare(0,9,"/dev/shm/")) unlink(t.c_str()); } }
    // sidecars
    MonoIdx x; x.owner="P_C0_T0"; x.timepoint=0; x.channel=0; x.claimOffset=MONO_RAW_BEGIN; uint64_t pos=0;
    for(size_t k=0;k<v.comp.size();k++){ x.chunks.push_back(chunkOf(v,k,x.claimOffset+pos)); pos+=v.comp[k].size(); }
    x.blobBytes=pos; x.claimSize=mono_round_up(pos); x.levels.push_back(levelOf(v,x.chunks.size()));
    std::string p=dir+"/x.idx"; MonoIdx y;
    CHECK(mono_idx_write(p,x,&e)&&mono_idx_read(p,y,&e),"sidecar round trip: %s",e.c_str());
    CHECK(y.owner==x.owner&&y.chunks.size()==x.chunks.size()&&y.claimOffset==x.claimOffset&&y.blobBytes==x.blobBytes&&y.levels.size()==1
          &&y.levels[0].grid[1]==x.levels[0].grid[1]&&!memcmp(&y.chunks.back(),&x.chunks.back(),sizeof(MonoChunk)),"sidecar read back differs");
    CHECK(mono_idx_validate(y,true,&e),"complete sidecar flagged incomplete: %s",e.c_str());
    std::vector<uint8_t> raw; mono_read_file(p,raw,&e);
    { std::vector<uint8_t> b(raw); b[b.size()/2]^=0x40; mono_write_atomic(p,b.data(),b.size(),&e);
      CHECK(!mono_idx_read(p,y,&e)&&e.find("checksum")!=std::string::npos,"a flipped byte was not detected (%s)",e.c_str()); }
    { std::vector<uint8_t> b(raw.begin(),raw.end()-1); mono_write_atomic(p,b.data(),b.size(),&e);
      CHECK(!mono_idx_read(p,y,&e)&&e.find("truncated")!=std::string::npos,"a truncated sidecar was not detected (%s)",e.c_str()); }
    { std::vector<uint8_t> b(raw); b[0]='X'; mono_write_atomic(p,b.data(),b.size(),&e);
      CHECK(!mono_idx_read(p,y,&e),"a sidecar with a bad magic was accepted"); }
    CHECK(!mono_idx_read(dir+"/missing.idx",y,&e),"a missing sidecar was read");
    { MonoIdx b=x; b.chunks[3].offset=x.claimOffset+x.blobBytes; CHECK(!mono_idx_write(p,b,&e)&&e.find("blob")!=std::string::npos,"a chunk outside the blob was accepted"); }
    { MonoIdx b=x; b.chunks[2]=b.chunks[1]; CHECK(!mono_idx_write(p,b,&e),"a chunk listed twice was accepted"); }
    { MonoIdx b=x; b.chunks[2].offset=b.chunks[1].offset+1; CHECK(!mono_idx_write(p,b,&e)&&e.find("overlap")!=std::string::npos,"overlapping chunks were accepted"); }
    { MonoIdx b=x; b.chunks.pop_back(); b.levels[0].nChunks--; CHECK(mono_idx_write(p,b,&e)&&!mono_idx_validate(b,true,&e)&&e.find("incomplete")!=std::string::npos,"an incomplete level passed requireComplete"); }
    { MonoIdx b=x; b.levels[0].nChunks++; CHECK(!mono_idx_write(p,b,&e),"a wrong chunk count was accepted"); }
    { MonoIdx b=x; b.levels[0].grid[2]++; CHECK(!mono_idx_write(p,b,&e),"an inconsistent grid was accepted"); }
    { MonoIdx b=x; b.chunks[0].level=3; CHECK(!mono_idx_write(p,b,&e),"a chunk of an unknown level was accepted"); }
    { MonoIdx b=x; b.claimOffset+=1; CHECK(!mono_idx_write(p,b,&e),"an unaligned claim was accepted"); }
}

// ---------------- claims ----------------
static uint64_t claimBytes(uint64_t gid,uint64_t i){ return 1000+((gid*7919ull+i*104729ull)%100000ull); }
static std::string ownerName(uint64_t gid,uint64_t i){ return "g"+std::to_string(gid)+"_"+std::to_string(i); }
// One process: nclaims claims (every 10th re-claimed: same range), results to resdir/<gid>.txt.
static int claimsProc(const std::string& mode,const std::string& dir,const std::string& resdir,uint64_t gid,uint64_t nclaims,double startEpoch){
    MonoClaims cl; std::string e;
    if(!cl.open(dir,mode,&e)){ std::printf("PROC gid=%llu: open: %s\n",(unsigned long long)gid,e.c_str()); return 1; }
    while(wall()<startEpoch) usleep(1000);
    double t0=secs(); std::string out; int err=0;
    for(uint64_t i=0;i<nclaims&&!err;i++){ MonoClaim c;
        if(!cl.claim(ownerName(gid,i),claimBytes(gid,i),c,&e)){ std::printf("PROC gid=%llu claim %llu: %s\n",(unsigned long long)gid,(unsigned long long)i,e.c_str()); err=1; break; }
        if(i%10==0){ MonoClaim d;
            if(!cl.claim(ownerName(gid,i),claimBytes(gid,i),d,&e)||d.offset!=c.offset||d.size!=c.size||d.slot!=c.slot){ std::printf("PROC gid=%llu: re-claim %llu differs: %s\n",(unsigned long long)gid,(unsigned long long)i,e.c_str()); err=1; } }
        char b[160]; snprintf(b,sizeof b,"%s %llu %llu %llu\n",c.owner.c_str(),(unsigned long long)c.slot,(unsigned long long)c.offset,(unsigned long long)c.size); out+=b; }
    double dt=secs()-t0;
    std::string rp=resdir+"/"+std::to_string(gid)+".txt";
    if(!mono_write_atomic(rp,out.data(),out.size(),&e)){ std::printf("PROC gid=%llu: %s\n",(unsigned long long)gid,e.c_str()); err=1; }
    char host[128]={0}; gethostname(host,sizeof host-1);
    std::printf("PROC %s gid=%llu mode=%s claims=%llu seconds=%.2f claims/s=%.1f err=%d\n",host,(unsigned long long)gid,cl.mode().c_str(),(unsigned long long)nclaims,dt,nclaims/(dt>0?dt:1),err);
    std::fflush(stdout);
    return err;
}
static int claimsRun(const std::string& mode,const std::string& dir,const std::string& resdir,int nproc,uint64_t nclaims,uint64_t gidBase,double startEpoch){
    mkdirp(resdir); std::fflush(stdout);
    std::vector<pid_t> kids;
    for(int p=0;p<nproc;p++){ pid_t k=fork(); if(k==0) _exit(claimsProc(mode,dir,resdir,gidBase+(uint64_t)p,nclaims,startEpoch)); if(k>0) kids.push_back(k); }
    int bad=(int)(nproc-(int)kids.size());
    for(pid_t k:kids){ int st=0; if(waitpid(k,&st,0)<0||!WIFEXITED(st)||WEXITSTATUS(st)) bad++; }
    return bad?1:0;
}
static int claimsCheck(const std::string& dir,const std::string& resdir,uint64_t expected){
    MonoClaims cl; std::string e; std::vector<MonoClaim> all; bool sealed=false;
    if(!cl.open(dir,"",&e)||!cl.list(all,&sealed,&e)){ std::printf("CHECK: %s\n",e.c_str()); return 1; }   // list() checks contiguity
    std::map<std::string,const MonoClaim*> byOwner; uint64_t dups=0,claims=0;
    for(const MonoClaim& c:all){ if(c.seal) continue; claims++; if(byOwner.count(c.owner)) dups++; byOwner[c.owner]=&c; }
    uint64_t lines=0,missing=0; DIR* d=opendir(resdir.c_str()); struct dirent* de;
    while(d&&(de=readdir(d))){ std::string n=de->d_name; if(n.size()<5||n.compare(n.size()-4,4,".txt")) continue;
        FILE* f=fopen((resdir+"/"+n).c_str(),"r"); char ow[128]; unsigned long long sl,of,sz;
        while(f&&fscanf(f,"%127s %llu %llu %llu",ow,&sl,&of,&sz)==4){ lines++; std::map<std::string,const MonoClaim*>::iterator it=byOwner.find(ow);
            if(it==byOwner.end()||it->second->slot!=sl||it->second->offset!=of||it->second->size!=sz) missing++; }
        if(f) fclose(f); }
    if(d) closedir(d);
    uint64_t endOff=all.empty()?MONO_RAW_BEGIN:all.back().offset+all.back().size;
    bool ok=claims==expected&&dups==0&&lines==expected&&missing==0;
    std::printf("CHECK %s: records=%llu (expected %llu) duplicates=%llu results=%llu not-in-journal=%llu end=%llu sealed=%d (gap-free: checked by list) -> %s\n",
        cl.mode().c_str(),(unsigned long long)claims,(unsigned long long)expected,(unsigned long long)dups,(unsigned long long)lines,(unsigned long long)missing,
        (unsigned long long)endOff,(int)sealed,ok?"OK":"FAILED");
    return ok?0:1;
}
static void testClaims(const std::string& work,const std::string& mode){
    const int NPROC=8; const uint64_t NCLAIMS=200;
    std::printf("[claims %s] %d processes x %llu claims\n",mode.c_str(),NPROC,(unsigned long long)NCLAIMS);
    std::string base=work+"/claims_"+mode, dir=base+"/mono", res=base+"/res";
    std::string e; MonoClaims cl;
    CHECK(cl.open(dir,mode,&e)&&cl.mode()==mode,"open %s: %s",mode.c_str(),e.c_str());
    double t0=secs();
    CHECK(claimsRun(mode,dir,res,NPROC,NCLAIMS,0,wall()+0.5)==0,"a claiming process failed");
    std::printf("  %d x %llu claims in %.1f s\n",NPROC,(unsigned long long)NCLAIMS,secs()-t0);
    CHECK(claimsCheck(dir,res,(uint64_t)NPROC*NCLAIMS)==0,"journal check failed");
    MonoClaim a,b; uint64_t end0=0,end1=0; bool sealed=true;
    CHECK(cl.claim("g3_17",claimBytes(3,17),a,&e)&&cl.claim("g3_17",claimBytes(3,17),b,&e)&&a.offset==b.offset&&a.slot==b.slot,"re-claim: %s",e.c_str());
    CHECK(!cl.claim("g3_17",claimBytes(3,17)+8192,b,&e)&&e.find("already claimed")!=std::string::npos,"a re-claim with another size was accepted");
    CHECK(cl.end(&end0,&sealed,&e)&&!sealed,"end: %s",e.c_str());
    CHECK(cl.rewindReceipt("g5_9",&e)&&cl.claim("g5_9",claimBytes(5,9),b,&e),"claim after a lost receipt: %s",e.c_str());
    bool found=false;
    CHECK(cl.find("g5_9",a,&found,&e)&&found&&a.offset==b.offset,"the lost-receipt claim did not find its record");
    CHECK(cl.end(&end1,&sealed,&e)&&end1==end0,"the lost-receipt claim appended a new record");
    found=false; unlink((dir+"/owners/g2_4").c_str());
    CHECK(cl.find("g2_4",a,&found,&e)&&found&&a.owner=="g2_4","find without a receipt: %s",e.c_str());
    CHECK(!cl.claim("bad/name",10,a,&e)&&!cl.claim(".hidden",10,a,&e)&&!cl.claim("",10,a,&e),"an invalid owner name was accepted");
    MonoClaim late,s,s2;
    CHECK(cl.claim("late_1",5000,late,&e)&&late.offset==end0&&late.size==8192,"late claim: %s",e.c_str());
    CHECK(cl.seal(s,&e)&&s.seal&&s.offset==late.offset+late.size,"seal: %s",e.c_str());
    CHECK(!cl.claim("late_2",5000,a,&e)&&e.find("sealed")!=std::string::npos,"a claim after the seal was accepted");
    CHECK(!cl.claim("g3_17",claimBytes(3,17),a,&e)&&e.find("sealed")!=std::string::npos,"a re-claim after the seal was accepted");
    MonoClaims other; std::string otherMode=mode=="link"?"fcntl":"link";
    CHECK(other.open(dir,otherMode,&e)&&other.mode()==mode,"a second opener did not follow the recorded mode (%s)",e.c_str());
    CHECK(other.seal(s2,&e)&&s2.offset==s.offset&&s2.slot==s.slot,"seal is not idempotent: %s",e.c_str());
    std::vector<MonoClaim> all;
    CHECK(other.list(all,&sealed,&e)&&sealed&&all.size()==(size_t)NPROC*NCLAIMS+2&&all.back().seal,"final journal: %s (%zu records)",e.c_str(),all.size());
    CHECK(other.end(&end1,&sealed,&e)&&sealed&&end1==s.offset,"end after the seal");
}

// ---------------- index-pass benchmark ----------------
static inline uint64_t mix(uint64_t x){ x+=0x9E3779B97F4A7C15ull; x=(x^(x>>30))*0xBF58476D1CE4E5B9ull; x=(x^(x>>27))*0x94D049BB133111EBull; return x^(x>>31); }
static inline uint32_t benchChunkSize(uint64_t i){ return 150000u+(uint32_t)(mix(i)%300001u); }   // ~300 KB: a 1 MiB chunk at ~3.5x
static double rssGB(){ struct rusage r; getrusage(RUSAGE_SELF,&r);
#ifdef __APPLE__
    return r.ru_maxrss/1073741824.0;
#else
    return r.ru_maxrss/1048576.0;
#endif
}
static int bench(int argc,char** argv){
    if(argc<5){ std::fprintf(stderr,"usage: %s --bench <file> <cz,cy,cx> [-v] <count:nz,ny,nx>...\n",argv[0]); return 2; }
    std::string path=argv[2]; unsigned long long cz=0,cy=0,cx=0;
    if(sscanf(argv[3],"%llu,%llu,%llu",&cz,&cy,&cx)!=3||!cz||!cy||!cx){ std::fprintf(stderr,"bad chunk dims %s\n",argv[3]); return 2; }
    struct Spec { unsigned long long count,nz,ny,nx; }; std::vector<Spec> specs; bool verify=false;
    for(int a=4;a<argc;a++){ if(!strcmp(argv[a],"-v")){ verify=true; continue; } Spec s;
        if(sscanf(argv[a],"%llu:%llu,%llu,%llu",&s.count,&s.nz,&s.ny,&s.nx)!=4){ std::fprintf(stderr,"bad spec %s\n",argv[a]); return 2; } specs.push_back(s); }
    uint64_t nTot=0,nDs=0,raw=0; for(const Spec& s:specs){ nTot+=s.count*s.nz*s.ny*s.nx; nDs+=s.count; }
    for(uint64_t i=0;i<nTot;i++) raw+=benchChunkSize(i);
    uint64_t rawEnd=MONO_RAW_BEGIN+raw;
    std::printf("bench: %llu datasets, %llu chunks of %llux%llux%llu (z,y,x), %.2f TB raw region, chunks sorted, addresses ascending\n",
        (unsigned long long)nDs,(unsigned long long)nTot,cz,cy,cx,raw/1e12);
    std::fflush(stdout);
    MonoVfd vfd(rawEnd); std::string e; hid_t fapl=mono_vfd_fapl(vfd,&e);
    if(fapl<0){ std::printf("fapl: %s\n",e.c_str()); return 1; }
    double t0=secs(); hid_t f=H5Fcreate(path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl);
    if(f<0){ std::printf("H5Fcreate failed: %s\n",vfd.err.c_str()); return 1; }
    std::printf("H5Fcreate took %.2f s\n",secs()-t0); std::fflush(stdout);
    std::vector<uint64_t> off; std::vector<uint32_t> sz; uint64_t gi=0,cum=0; int level=0; bool ok=true;
    // same generator in both passes: chunk k of a dataset -> (z,y,x) in sorted order, address = running sum
    auto fill=[&](const Spec& s,uint64_t& gidx,uint64_t& c){ uint64_t per=s.nz*s.ny*s.nx; off.resize(per); sz.resize(per);
        for(uint64_t k=0;k<per;k++){ sz[k]=benchChunkSize(gidx+k); off[k]=MONO_RAW_BEGIN+c; c+=sz[k]; } gidx+=per; };
    auto geom=[&](const Spec& s){ MonoGeom g; g.chunk[0]=(uint32_t)cx; g.chunk[1]=(uint32_t)cy; g.chunk[2]=(uint32_t)cz;
        g.grid[0]=(uint32_t)s.nx; g.grid[1]=(uint32_t)s.ny; g.grid[2]=(uint32_t)s.nz; for(int a=0;a<3;a++) g.size[a]=(uint64_t)g.chunk[a]*g.grid[a]; g.dtype=MONO_U16; return g; };
    auto name=[](int r,uint64_t g){ char b[160]; snprintf(b,sizeof b,"/DataSet/ResolutionLevel %d/TimePoint %llu/Channel %llu/Data",r,(unsigned long long)(g/2),(unsigned long long)(g%2)); return std::string(b); };
    for(const Spec& s:specs){ double ts=secs(); MonoGeom g=geom(s); uint64_t per=s.nz*s.ny*s.nx;
        for(uint64_t d=0;d<s.count&&ok;d++){ fill(s,gi,cum);
            ok=mono_index_dataset(vfd,f,name(level,d).c_str(),g,(size_t)per,[&](size_t k,MonoChunk& c){
                c.offset=off[k]; c.size=sz[k]; c.mask=0; c.level=(uint16_t)level; c.reserved=0;
                c.cz=(uint32_t)(k/(s.ny*s.nx)); uint64_t r=k%(s.ny*s.nx); c.cy=(uint32_t)(r/s.nx); c.cx=(uint32_t)(r%s.nx); },&e);
            if(!ok) std::printf("index: %s\n",e.c_str()); }
        double te=secs()-ts; std::printf("level %d: %llu datasets x %llu chunks in %.2f s (%.0f chunks/s)\n",level,s.count,(unsigned long long)per,te,s.count*per/(te>0?te:1));
        std::fflush(stdout); level++; }
    double t1=secs(); herr_t rc=H5Fclose(f); double t2=secs();
    const MonoVfdStats& st=mono_vfd_stats(vfd); struct stat sb; stat(path.c_str(),&sb);
    std::printf("RESULT insert %.2f s  close %.2f s  total %.2f s  => %.0f chunks/s\n",t1-t0,t2-t1,t2-t0,nTot/(t2-t0));
    std::printf("RESULT chunks=%llu placed=%llu discarded=%llu failures=%llu close=%s metaWrites=%llu metaBytes=%.3f GB (%.1f B/chunk) file=%.3f GB apparent, %.3f GB allocated  maxrss=%.2f GB\n",
        (unsigned long long)nTot,(unsigned long long)st.placed,(unsigned long long)st.discarded,(unsigned long long)st.failures,rc<0?"FAILED":"ok",
        (unsigned long long)st.writes,st.writeBytes/1e9,nTot?(double)st.writeBytes/nTot:0.0,sb.st_size/1e9,sb.st_blocks*512.0/1e9,rssGB());
    ok=ok&&rc>=0&&st.failures==0&&st.discarded==nTot;
    if(verify&&ok){
        double tv=secs(); hid_t r=H5Fopen(path.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); uint64_t bad=0,nd=0; gi=0; cum=0; level=0;
        if(r<0){ std::printf("VERIFY: stock driver cannot open the file\n"); return 1; }
        for(const Spec& s:specs){ MonoGeom g=geom(s); uint64_t per=s.nz*s.ny*s.nx;
            for(uint64_t d=0;d<s.count;d++){ fill(s,gi,cum); hid_t ds=H5Dopen2(r,name(level,d).c_str(),H5P_DEFAULT); uint64_t m=0;
                if(ds<0){ bad+=per; continue; }
                mono_verify_dataset(ds,g,(size_t)per,[&](size_t k,MonoChunk& c){
                    c.offset=off[k]; c.size=sz[k]; c.mask=0; c.level=(uint16_t)level; c.reserved=0;
                    c.cz=(uint32_t)(k/(s.ny*s.nx)); uint64_t q=k%(s.ny*s.nx); c.cy=(uint32_t)(q/s.nx); c.cx=(uint32_t)(q%s.nx); },&m,&e);
                bad+=m; nd++; H5Dclose(ds); }
            level++; }
        H5Fclose(r); double tv2=secs()-tv;
        std::printf("VERIFY (stock driver, H5Dchunk_iter): %llu datasets, %llu chunks, mismatches=%llu, %.2f s (%.0f chunks/s) -> %s  maxrss=%.2f GB\n",
            (unsigned long long)nd,(unsigned long long)nTot,(unsigned long long)bad,tv2,nTot/(tv2>0?tv2:1),bad?"MISMATCH":"MATCH",rssGB());
        ok=bad==0; }
    return ok?0:1;
}

// ---------------- blob-writer throughput ----------------
// Appends GB of incompressible data in chunk-sized pieces at offset 1 MiB of file and syncs.
static int placeBench(int argc,char** argv){
    if(argc<5){ std::fprintf(stderr,"usage: %s --place-bench <file> <GB> <threads> [pieceMiB=64] [chunkKB=397]\n",argv[0]); return 2; }
    std::string path=argv[2]; double gb=atof(argv[3]); int threads=atoi(argv[4]);
    size_t seg=(size_t)(argc>5?atoi(argv[5]):64)<<20, chunk=(size_t)(argc>6?atoi(argv[6]):397)<<10;
    uint64_t total=(uint64_t)(gb*1e9); std::vector<uint8_t> pat((size_t)256<<20); uint64_t x=88172645463325252ull;
    for(size_t i=0;i+8<=pat.size();i+=8){ x^=x<<13; x^=x>>7; x^=x<<17; memcpy(&pat[i],&x,8); }
    MonoBlobWriter w; std::string e; double t0=secs();
    if(!w.open(path,MONO_RAW_BEGIN,mono_round_up(total),threads,&e,seg)){ std::printf("open: %s\n",e.c_str()); return 1; }
    uint64_t done=0; size_t at=0;
    while(done<total){ size_t n=(size_t)std::min<uint64_t>(chunk,total-done); if(at+n>pat.size()) at=0;
        if(!w.append(&pat[at],n,&e)){ std::printf("append: %s\n",e.c_str()); return 1; } at+=n; done+=n; }
    double t1=secs();
    if(!w.finish(&e)){ std::printf("finish: %s\n",e.c_str()); return 1; }
    double t2=secs();
    std::printf("PLACE %.2f GB in %.2f s (appends %.2f s, sync+close %.2f s) = %.2f GB/s, %d threads, %zu MiB pieces, %zu KB chunks, %s\n",
        total/1e9,t2-t0,t1-t0,t2-t1,total/1e9/(t2-t0),threads,seg>>20,chunk>>10,w.direct()?"O_DIRECT":"buffered");
    return 0;
}

int main(int argc,char** argv){
    if(argc>=2&&!strcmp(argv[1],"--bench")) return bench(argc,argv);
    if(argc>=2&&!strcmp(argv[1],"--place-bench")) return placeBench(argc,argv);
    if(argc>=9&&!strcmp(argv[1],"--claims-run"))
        return claimsRun(argv[2],argv[3],argv[4],atoi(argv[5]),strtoull(argv[6],NULL,10),strtoull(argv[7],NULL,10),atof(argv[8]));
    if(argc>=5&&!strcmp(argv[1],"--claims-check")) return claimsCheck(argv[2],argv[3],strtoull(argv[4],NULL,10));
    if(argc!=2||argv[1][0]=='-'){ std::fprintf(stderr,"usage: %s <workdir> | --bench ... | --claims-run ... | --claims-check ...\n",argv[0]); return 2; }
    std::string work=argv[1]; rmrf(work); mkdirp(work);
    double t0=secs();
    testPlacement(work);
    testNegative(work);
    testClaims(work,"link");
    testClaims(work,"fcntl");
    std::printf("monocoretest %s (%d failures, %.1f s)\n",gFail?"FAILED":"PASSED",gFail,secs()-t0);
    if(!gFail) rmrf(work);
    return gFail?1:0;
}
#endif
