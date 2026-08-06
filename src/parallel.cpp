// Parallel .ims tool (self-contained, multi-channel + multi-timepoint). Work is split by
// (channel, timepoint, spatial tile, pyramid level): many workers can share one volume.
// Each worker produces plain .chunk files for its tile at one level (level 0 from the zarr,
// coarser levels by downsampling the finer level's chunk files -- so LEVELS RUN IN ORDER),
// plus a partial histogram. A parallel merge (OpenMP read, single HDF5 writer) assembles the
// chunk files into ONE .ims, byte-identical to a monolithic conversion.
//   parallelimariswriter setup   <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz]  -- cx,cy,cz = per-worker tile size (voxels). Prints jobs per level.
//   parallelimariswriter worker  <workdir> <level> <index>                 -- build one tile at one level (submit level 0's array first, then level 1's, ...)
//   parallelimariswriter monitor <workdir> [out.ims]                       -- wait for all chunks, then parallel-merge them into the single file
//   parallelimariswriter layout  <X> <Y> <Z> [elemSize]                    -- debug: print the pyramid
#include "hdf5.h"
#include <lz4.h>
#include "zarr.h"
#include "parallelreadzarr.h"
#include "bpDeriche.h"
#include <vector>
#include <array>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <functional>
#include <dirent.h>
#include <unistd.h>
#include <omp.h>
#include "parallel.h"

typedef uint64_t bpSize;
typedef std::array<bpSize,3> Vec3;

// ===================== ported ImarisWriter block layout =====================
struct Cost { bpSize x,y,z,t; float slice, geom, mem; };
static void GetOptimalBlockSize(bpSize aB, bpSize sx,bpSize sy,bpSize sz,bpSize st,
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
static std::vector<Vec3> GetOptimalBlockSizes(bpSize aB, const std::vector<Vec3>& s, bpSize st){
    std::vector<Vec3> o; for(size_t i=0;i<s.size();++i){ bpSize mnx=1,mny=1,mnz=1,mnt=1;
        if(i>0){ bool rx=(s[i-1][0]/2)==s[i][0],ry=(s[i-1][1]/2)==s[i][1],rz=(s[i-1][2]/2)==s[i][2];
            mnx=rx?o[i-1][0]/2:o[i-1][0]; mny=ry?o[i-1][1]/2:o[i-1][1]; mnz=rz?o[i-1][2]/2:o[i-1][2]; }
        bpSize bx,by,bz,bt; GetOptimalBlockSize(aB,s[i][0],s[i][1],s[i][2],st,mnx,mny,mnz,mnt,bx,by,bz,bt); o.push_back({bx,by,bz}); }
    return o;
}
static std::vector<Vec3> GetOptimalImagePyramid(bpSize aMax, Vec3 img, bool rZ){
    std::vector<Vec3> r{img}; Vec3 s=img;
    while((unsigned long long)s[0]*s[1]*s[2]>aMax){ unsigned long long lx=s[0],ly=s[1],lz=rZ?s[2]:1;
        bool rx=lx>1&&(10*lx)*(10*lx)>ly*lz, ry=ly>1&&(10*ly)*(10*ly)>lx*lz, rz=lz>1&&(10*lz)*(10*lz)>lx*ly;
        if(!rx&&!ry&&!rz)break;
        if(rx)s[0]/=2; if(ry)s[1]/=2; if(rz)s[2]/=2;
        r.push_back(s);
    }
    return r;
}

// ===================== chunk codec (bpShuffle 2-byte + bpLZ4) =====================
static void shuffle2(const uint8_t* s,size_t n,uint8_t* d){ size_t h=n/2; for(size_t i=0;i<h;i++){d[i]=s[2*i];d[h+i]=s[2*i+1];} }
static void unshuffle2(const uint8_t* s,size_t n,uint8_t* d){ size_t h=n/2; for(size_t i=0;i<h;i++){d[2*i]=s[i];d[2*i+1]=s[h+i];} }
static void wbe(uint8_t* d,uint64_t v,int nb){ for(int i=0;i<nb;i++) d[nb-1-i]=(uint8_t)((v>>(8*i))&0xff); }
static uint64_t rbe(const uint8_t* d,int nb){ uint64_t v=0; for(int i=0;i<nb;i++)v=(v<<8)|d[i]; return v; }
static void encodeChunk(const uint8_t* vox,size_t nb,std::vector<uint8_t>& out){
    std::vector<uint8_t> sh(nb); shuffle2(vox,nb,sh.data()); int bd=LZ4_compressBound((int)nb); out.resize(16+bd);
    int c=LZ4_compress_default((const char*)sh.data(),(char*)out.data()+16,(int)nb,bd);
    wbe(out.data(),nb,8); wbe(out.data()+8,nb,4); wbe(out.data()+12,(uint64_t)c,4); out.resize(16+c);
}
static void decodeChunk(const uint8_t* c,size_t cs,std::vector<uint8_t>& vox){
    uint64_t t=rbe(c,8); std::vector<uint8_t> sh(t); LZ4_decompress_safe((const char*)c+16,(char*)sh.data(),(int)(cs-16),(int)t); vox.resize(t); unshuffle2(sh.data(),t,vox.data());
}
// Box-average one pyramid step; dims/strides [x,y,z] (x fastest), di a multiple of st.
static void boxDown(const std::vector<uint16_t>& in,const bpSize di[3],const bpSize st[3],std::vector<uint16_t>& out,bpSize dOut[3]){
    for(int a=0;a<3;a++) dOut[a]=di[a]/st[a];
    out.assign((size_t)dOut[0]*dOut[1]*dOut[2],0); const float dn=(float)(st[0]*st[1]*st[2]);
    for(bpSize oz=0;oz<dOut[2];oz++)for(bpSize oy=0;oy<dOut[1];oy++)for(bpSize ox=0;ox<dOut[0];ox++){ float s=0;
        for(bpSize dz=0;dz<st[2];dz++)for(bpSize dy=0;dy<st[1];dy++)for(bpSize dx=0;dx<st[0];dx++)
            s+=in[(ox*st[0]+dx)+(oy*st[1]+dy)*di[0]+(oz*st[2]+dz)*(size_t)di[0]*di[1]];
        out[ox+oy*dOut[0]+oz*(size_t)dOut[0]*dOut[1]]=(uint16_t)(s/dn); }
}

// ===================== chunk + partial-histogram files (atomic) =====================
static void writeFileAtomic(const std::string& p,const void* d,size_t n){ std::string t=p+".tmp"; FILE* f=fopen(t.c_str(),"wb"); if(!f)return; fwrite(d,1,n,f); fclose(f); rename(t.c_str(),p.c_str()); }
static std::vector<uint8_t> readFileBytes(const std::string& p){ FILE* f=fopen(p.c_str(),"rb"); if(!f)return{}; fseek(f,0,SEEK_END); long n=ftell(f); fseek(f,0,SEEK_SET); std::vector<uint8_t> b(n>0?n:0); if(n>0){size_t r=fread(b.data(),1,n,f);(void)r;} fclose(f); return b; }
// One .pack file per tile holds ALL that tile's block chunks (avoids millions of tiny files on the
// shared FS). Layout: [chunk0 bytes][chunk1 bytes]...[N*(u64 cz,cy,cx,size)][u64 N] -- the index sits
// at the END so the writer streams the body and only buffers the small index.
static std::string packName(const std::string& d,int r,int c,int t,long tile){ char b[128]; snprintf(b,sizeof b,"/L%d_C%d_T%d_%ld.pack",r,c,t,tile); return d+b; }
static std::string histName(const std::string& d,int r,int c,int t,long tile){ char b[128]; snprintf(b,sizeof b,"/h_L%d_C%d_T%d_%ld.hist",r,c,t,tile); return d+b; }
static void forEachPackedChunk(const std::string& path,const std::function<void(bpSize,bpSize,bpSize,const uint8_t*,size_t)>& cb){
    FILE* f=fopen(path.c_str(),"rb"); if(!f)return;
    if(fseek(f,-8,SEEK_END)!=0){fclose(f);return;} uint64_t N=0; if(fread(&N,8,1,f)!=1||N==0){fclose(f);return;}
    if(fseek(f,-(long)(8+N*32),SEEK_END)!=0){fclose(f);return;} std::vector<uint64_t> idx(N*4); if(fread(idx.data(),8,N*4,f)!=N*4){fclose(f);return;}
    fseek(f,0,SEEK_SET); std::vector<uint8_t> buf;
    for(uint64_t i=0;i<N;i++){ uint64_t sz=idx[i*4+3]; buf.resize(sz); if(sz&&fread(buf.data(),1,sz,f)!=sz)break; cb(idx[i*4],idx[i*4+1],idx[i*4+2],buf.data(),sz); }
    fclose(f);
}

// ===================== manifest (JSON) =====================
struct Lev { Vec3 size, block, nch, tb, nt; };   // tb = tile size in blocks; nt = tile grid
struct Manifest { std::vector<std::string> zarrs; int nC=1,nT=1; bpSize X,Y,Z,elem; float vox[3]; bpSize cap[3]={0,0,0}; std::vector<Lev> lev; };
static const std::string& zarrAt(const Manifest& m,int c,int t){ return m.zarrs[(size_t)c*m.nT+t]; }
static bpSize levStride(const Manifest& m,int r,int a){ bpSize s=m.lev[r-1].size[a]/m.lev[r].size[a]; return s?s:1; }
static long jobsAtLevel(const Manifest& m,int r){ const Lev& L=m.lev[r]; return (long)m.nC*m.nT*L.nt[0]*L.nt[1]*L.nt[2]; }
static double voxClean(float f){ char b[32]; snprintf(b,sizeof b,"%.7g",(double)f); return atof(b); }
static void writeManifest(const std::string& wd,const Manifest& m){
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
    std::ofstream out(wd+"/manifest.json"); if(out) out<<j.dump(2)<<"\n";
}
static Manifest readManifest(const std::string& wd){
    Manifest m{}; std::ifstream in(wd+"/manifest.json");
    if(!in){ fprintf(stderr,"cannot open %s/manifest.json (run setup?)\n",wd.c_str()); return m; }
    nlohmann::json j; try{ in>>j; }catch(const std::exception& e){ fprintf(stderr,"invalid manifest.json: %s\n",e.what()); return m; }
    m.nC=j.value("channels",1); m.nT=j.value("timepoints",1); m.elem=j.value("elem",(bpSize)2);
    if(j.contains("size")){ auto&s=j["size"]; m.X=s[0].get<bpSize>(); m.Y=s[1].get<bpSize>(); m.Z=s[2].get<bpSize>(); }
    if(j.contains("voxel")){ auto&v=j["voxel"]; for(int a=0;a<3;a++) m.vox[a]=v[a].get<float>(); }
    if(j.contains("tile")){ auto&c=j["tile"]; for(int a=0;a<3;a++) m.cap[a]=c[a].get<bpSize>(); }
    if(j.contains("level")) for(auto& e:j["level"]){ Lev L; for(int a=0;a<3;a++){ L.size[a]=e["size"][a].get<bpSize>(); L.block[a]=e["block"][a].get<bpSize>(); L.nch[a]=e["nchunks"][a].get<bpSize>(); L.tb[a]=e["tileBlocks"][a].get<bpSize>(); L.nt[a]=e["ntiles"][a].get<bpSize>(); } m.lev.push_back(L); }
    m.zarrs.assign((size_t)m.nC*m.nT,std::string());
    if(j.contains("zarrs")){ auto&z=j["zarrs"]; for(int c=0;c<m.nC&&c<(int)z.size();c++) for(int t=0;t<m.nT&&t<(int)z[c].size();t++) m.zarrs[(size_t)c*m.nT+t]=z[c][t].get<std::string>(); }
    return m;
}

// ===================== HDF5 helpers =====================
static const int FID=32004; static size_t lz4_noop(unsigned,size_t,const unsigned int*,size_t,size_t*,void**){return 0;}
static const H5Z_class2_t H5Z_LZ4[1]={{H5Z_CLASS_T_VERS,(H5Z_filter_t)FID,1,1,"HDF5 lz4 filter; see http://www.hdfgroup.org/services/contributions.html",NULL,NULL,(H5Z_func_t)lz4_noop}};
static void registerLZ4(){ if(H5Zfilter_avail(FID)<=0) H5Zregister(H5Z_LZ4); }
static void setLZ4(hid_t d){ unsigned bs=1u<<30; H5Pset_filter(d,FID,H5Z_FLAG_MANDATORY,1,&bs); }
static void setStrAttr(hid_t o,const char* n,const std::string& v){ hsize_t dim=v.size()?v.size():1; hid_t sp=H5Screate_simple(1,&dim,NULL),t=H5Tcopy(H5T_C_S1); hid_t a=H5Acreate2(o,n,t,sp,H5P_DEFAULT,H5P_DEFAULT); H5Awrite(a,t,v.size()?v.data():""); H5Aclose(a);H5Tclose(t);H5Sclose(sp); }
static std::string fstr(float v,int p){ std::ostringstream o; o.setf(std::ios::fixed); o<<std::setprecision(p)<<v; return o.str(); }
static void writeU64(hid_t g,const char* n,const std::vector<uint64_t>& v){ hsize_t d=v.size(); hid_t sp=H5Screate_simple(1,&d,NULL); hid_t ds=H5Dcreate2(g,n,H5T_STD_U64LE,sp,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT); H5Dwrite(ds,H5T_NATIVE_UINT64,H5S_ALL,H5S_ALL,H5P_DEFAULT,v.data()); H5Dclose(ds);H5Sclose(sp); }
static std::string fmtTime(unsigned long long s){ char b[16]; snprintf(b,sizeof b,"%02llu:%02llu:%02llu",s/3600,(s/60)%60,s%60); return std::string("2020-02-05 ")+b; }

// trim a 65536-bin histogram like bpHistogramBuilder<uint16>::GetHistogram (min 0, keep >=257 span)
static void writeHistogramFromBins(hid_t g,const std::vector<uint64_t>& bins){
    bool any=false; for(uint64_t v:bins){ if(v){any=true;break;} } if(!any)return;
    uint64_t vMax=65535; while(256<vMax&&bins[vMax]==0)--vMax; uint64_t nb=vMax+1; std::vector<uint64_t> tr(bins.begin(),bins.begin()+nb);
    auto rs=[](const std::vector<uint64_t>& in,uint64_t nN){ std::vector<uint64_t> o(nN,0); uint64_t on=in.size(); for(uint64_t i=0;i<on;i++) o[i*nN/on]+=in[i]; return o; };
    std::vector<uint64_t> h1024=nb>1024?rs(tr,1024):tr, h256=rs(h1024,256); std::string mn="0.000",mx=fstr((float)vMax,3);
    writeU64(g,"Histogram",h256); setStrAttr(g,"HistogramMin",mn); setStrAttr(g,"HistogramMax",mx);
    if(h1024.size()!=256){ writeU64(g,"Histogram1024",h1024); setStrAttr(g,"HistogramMin1024",mn); setStrAttr(g,"HistogramMax1024",mx); }
}
// per-channel auto ColorRange from the (exact, summed) level-0 histogram
struct ColorHist { std::vector<uint64_t> bins; float hmin,hmax; };
static void adjustColorRange(const ColorHist& h,float& rMin,float& rMax){
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
static ColorHist channelColorHistMulti(const std::vector<std::vector<uint64_t>>& perTp){
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

// ===================== worker: one (channel, timepoint, tile) at one level =====================
// Assemble a finer-level sub-region [fo, fo+fn) from its chunk files (zero beyond the finer size).
static void assembleFinerRegion(const Manifest& m,const std::string& cd,int fr,int c,int t,const bpSize fo[3],const bpSize fn[3],std::vector<uint16_t>& fine){
    const Lev& F=m.lev[fr]; fine.assign((size_t)fn[0]*fn[1]*fn[2],0);
    bpSize fb[3]={F.block[0],F.block[1],F.block[2]};
    // finer blocks covering the footprint, then the finer TILES holding those blocks
    bpSize cb0[3],cb1[3],ft0[3],ft1[3];
    for(int a=0;a<3;a++){ cb0[a]=fo[a]/fb[a]; cb1[a]=std::min((fo[a]+fn[a]-1)/fb[a],F.nch[a]-1); ft0[a]=cb0[a]/F.tb[a]; ft1[a]=std::min(cb1[a]/F.tb[a],(bpSize)F.nt[a]-1); }
    for(bpSize ftz=ft0[2];ftz<=ft1[2];ftz++)for(bpSize fty=ft0[1];fty<=ft1[1];fty++)for(bpSize ftx=ft0[0];ftx<=ft1[0];ftx++){
        long tileIdx=(long)ftx+(long)fty*F.nt[0]+(long)ftz*F.nt[0]*F.nt[1];
        forEachPackedChunk(packName(cd,fr,c,t,tileIdx),[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz){
            std::vector<uint8_t> vox; decodeChunk(by,sz,vox); const uint16_t* cv=(const uint16_t*)vox.data();
            bpSize bx0=cx*fb[0],by0=cy*fb[1],bz0=cz*fb[2];
            for(bpSize z=0;z<fb[2]&&bz0+z<F.size[2];z++)for(bpSize y=0;y<fb[1]&&by0+y<F.size[1];y++)for(bpSize x=0;x<fb[0]&&bx0+x<F.size[0];x++){
                bpSize GX=bx0+x,GY=by0+y,GZ=bz0+z;
                if(GX>=fo[0]&&GX<fo[0]+fn[0]&&GY>=fo[1]&&GY<fo[1]+fn[1]&&GZ>=fo[2]&&GZ<fo[2]+fn[2])
                    fine[(GX-fo[0])+(GY-fo[1])*fn[0]+(GZ-fo[2])*(size_t)fn[0]*fn[1]]=cv[x+y*fb[0]+z*(size_t)fb[0]*fb[1]];
            }
        });
    }
}
static int doWorker(const std::string& wd,int r,long index){
    Manifest m=readManifest(wd); std::string cd=wd+"/chunks"; int L=(int)m.lev.size();
    if(r<0||r>=L){ fprintf(stderr,"level %d out of range [0,%d)\n",r,L); return 1; }
    const Lev& Lr=m.lev[r]; long perCT=(long)Lr.nt[0]*Lr.nt[1]*Lr.nt[2], jobs=(long)m.nC*m.nT*perCT;
    if(index<0||index>=jobs){ fprintf(stderr,"index %ld out of range [0,%ld) for level %d\n",index,jobs,r); return 1; }
    int c=(int)(index/((long)m.nT*perCT)); long rem=index%((long)m.nT*perCT); int t=(int)(rem/perCT); long ti=rem%perCT;
    bpSize tx=(bpSize)(ti%Lr.nt[0]), ty=(bpSize)((ti/Lr.nt[0])%Lr.nt[1]), tz=(bpSize)(ti/(Lr.nt[0]*Lr.nt[1]));
    bpSize blk[3]={Lr.block[0],Lr.block[1],Lr.block[2]};
    // this tile's level-r block range and voxel extent (clipped to the level size)
    bpSize cb0[3]={tx*Lr.tb[0],ty*Lr.tb[1],tz*Lr.tb[2]};
    bpSize cb1[3]={std::min(cb0[0]+Lr.tb[0],Lr.nch[0]),std::min(cb0[1]+Lr.tb[1],Lr.nch[1]),std::min(cb0[2]+Lr.tb[2],Lr.nch[2])};
    if(cb0[0]>=Lr.nch[0]||cb0[1]>=Lr.nch[1]||cb0[2]>=Lr.nch[2]) return 0;   // empty tile
    bpSize to[3]={cb0[0]*blk[0],cb0[1]*blk[1],cb0[2]*blk[2]};                // tile voxel origin
    bpSize td[3]={(cb1[0]-cb0[0])*blk[0],(cb1[1]-cb0[1])*blk[1],(cb1[2]-cb0[2])*blk[2]};  // tile voxel extent (block-aligned)
    std::vector<uint16_t> tile((size_t)td[0]*td[1]*td[2],0);
    if(r==0){
        bpSize ex=std::min(to[0]+td[0],m.X),ey=std::min(to[1]+td[1],m.Y),ez=std::min(to[2]+td[2],m.Z);
        if(to[0]<m.X&&to[1]<m.Y&&to[2]<m.Z){ bpSize rx=ex-to[0],ry=ey-to[1],rz=ez-to[2];
            uint16_t* reg=(uint16_t*)readZarrParallelHelper(zarrAt(m,c,t).c_str(),to[0],to[1],to[2],ex,ey,ez,0);
            for(bpSize z=0;z<rz;z++)for(bpSize y=0;y<ry;y++)for(bpSize x=0;x<rx;x++) tile[x+y*td[0]+z*(size_t)td[0]*td[1]]=reg[x+y*rx+z*(size_t)rx*ry];
            free(reg); }
    } else {
        bpSize st[3]={levStride(m,r,0),levStride(m,r,1),levStride(m,r,2)};
        bpSize fo[3]={to[0]*st[0],to[1]*st[1],to[2]*st[2]}, fn[3]={td[0]*st[0],td[1]*st[1],td[2]*st[2]};
        std::vector<uint16_t> fine; assembleFinerRegion(m,cd,r-1,c,t,fo,fn,fine);
        bpSize dOut[3]; boxDown(fine,fn,st,tile,dOut);   // tile <- downsample(fine); dims td = fn/st
    }
    // zero voxels past the true level size (floor-halving tail; the writer zero-pads them)
    for(bpSize z=0;z<td[2];z++)for(bpSize y=0;y<td[1];y++)for(bpSize x=0;x<td[0];x++)
        if(to[0]+x>=Lr.size[0]||to[1]+y>=Lr.size[1]||to[2]+z>=Lr.size[2]) tile[x+y*td[0]+z*(size_t)td[0]*td[1]]=0;
    // pack every block chunk of this tile into ONE file (body streamed, index at the end);
    // accumulate the tile histogram over in-size voxels only.
    std::vector<uint64_t> hist(65536,0), pidx;
    std::string pp=packName(cd,r,c,t,ti), ptmp=pp+".tmp"; FILE* pf=fopen(ptmp.c_str(),"wb");
    for(bpSize bz=cb0[2];bz<cb1[2];bz++)for(bpSize by=cb0[1];by<cb1[1];by++)for(bpSize bx=cb0[0];bx<cb1[0];bx++){
        std::vector<uint16_t> cbk((size_t)blk[0]*blk[1]*blk[2]);
        bpSize lx=(bx-cb0[0])*blk[0],ly=(by-cb0[1])*blk[1],lz=(bz-cb0[2])*blk[2];
        for(bpSize z=0;z<blk[2];z++)for(bpSize y=0;y<blk[1];y++)for(bpSize x=0;x<blk[0];x++){
            uint16_t v=tile[(lx+x)+(ly+y)*td[0]+(lz+z)*(size_t)td[0]*td[1]];
            cbk[x+y*blk[0]+z*(size_t)blk[0]*blk[1]]=v;
            if(bx*blk[0]+x<Lr.size[0]&&by*blk[1]+y<Lr.size[1]&&bz*blk[2]+z<Lr.size[2]) hist[v]++;
        }
        std::vector<uint8_t> comp; encodeChunk((uint8_t*)cbk.data(),cbk.size()*2,comp);
        if(pf){ fwrite(comp.data(),1,comp.size(),pf); pidx.push_back(bz); pidx.push_back(by); pidx.push_back(bx); pidx.push_back(comp.size()); }
    }
    if(pf){ uint64_t N=pidx.size()/4; if(N) fwrite(pidx.data(),8,pidx.size(),pf); fwrite(&N,8,1,pf); fclose(pf); rename(ptmp.c_str(),pp.c_str()); }
    writeFileAtomic(histName(cd,r,c,t,ti),hist.data(),hist.size()*sizeof(uint64_t));
    return 0;
}

// ===================== gather: parallel-read chunk files, single HDF5 writer =====================
static std::vector<uint64_t> sumPartialHists(const Manifest& m,const std::string& cd,int r,int c,int t){
    const Lev& L=m.lev[r]; long perCT=(long)L.nt[0]*L.nt[1]*L.nt[2];
    std::vector<uint64_t> sum(65536,0);
    for(long ti=0;ti<perCT;ti++){ auto b=readFileBytes(histName(cd,r,c,t,ti)); if(b.size()<65536*sizeof(uint64_t))continue;
        const uint64_t* h=(const uint64_t*)b.data(); for(int i=0;i<65536;i++) sum[i]+=h[i]; }
    return sum;
}
// ---- shared .ims writers (used by the single-file combine, the per-(tp,channel) parts, and the link file) ----
static const char* IMS_CHNAME[3]={"First channel","Second channel","Third channel"};
static hid_t imsCreate(const std::string& path){ hid_t fapl=H5Pcreate(H5P_FILE_ACCESS); H5Pset_libver_bounds(fapl,H5F_LIBVER_V18,H5F_LIBVER_V18); hid_t f=H5Fcreate(path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl); H5Pclose(fapl); return f; }
static hid_t imsDtype(bpSize elem){ return elem==2?H5Tcopy(H5T_STD_U16LE):(elem==1?H5Tcopy(H5T_STD_U8LE):H5Tcopy(H5T_IEEE_F32LE)); }
static std::vector<int> iota0(int n){ std::vector<int> v(n); for(int i=0;i<n;i++) v[i]=i; return v; }
// root attrs + full DataSetInfo (Image, Channels w/ ColorRange, TimeInfo, ImarisDataSet, Log) + empty DataSet group.
// srcCh = source channel indices written as Channel 0,1,... (color/name from the SOURCE index); colorTps = source
// timepoints aggregated for each channel's ColorRange; nTime = number of TimeInfo entries.
static void writeImsMeta(hid_t out,const Manifest& m,const std::string& cd,hid_t lcpl,
                         const std::vector<int>& srcCh,const std::vector<int>& colorTps,int nTime){
    setStrAttr(out,"ImarisDataSet","ImarisDataSet"); setStrAttr(out,"ImarisVersion","5.5.0"); setStrAttr(out,"DataSetInfoDirectoryName","DataSetInfo"); setStrAttr(out,"ThumbnailDirectoryName","Thumbnail"); setStrAttr(out,"DataSetDirectoryName","DataSet");
    { uint32_t one=1; hsize_t d=1; hid_t sp=H5Screate_simple(1,&d,NULL); hid_t a=H5Acreate2(out,"NumberOfDataSets",H5T_STD_U32LE,sp,H5P_DEFAULT,H5P_DEFAULT); H5Awrite(a,H5T_NATIVE_UINT32,&one); H5Aclose(a);H5Sclose(sp); }
    bpSize X=m.X,Y=m.Y,Z=m.Z;
    hid_t img=H5Gcreate2(out,"DataSetInfo/Image",lcpl,H5P_DEFAULT,H5P_DEFAULT);
    setStrAttr(img,"Name","(name not specified)"); setStrAttr(img,"Description","(description not specified)");
    setStrAttr(img,"ExtMin0",fstr(0.f,9)); setStrAttr(img,"ExtMin1",fstr(0.f,9)); setStrAttr(img,"ExtMin2",fstr(0.f,9));
    setStrAttr(img,"ExtMax0",fstr((float)X*m.vox[0],9)); setStrAttr(img,"ExtMax1",fstr((float)Y*m.vox[1],9)); setStrAttr(img,"ExtMax2",fstr((float)Z*m.vox[2],9));
    setStrAttr(img,"X",std::to_string(X)); setStrAttr(img,"Y",std::to_string(Y)); setStrAttr(img,"Z",std::to_string(Z)); setStrAttr(img,"Unit","um");
    setStrAttr(img,"ResampleDimensionX","true"); setStrAttr(img,"ResampleDimensionY","true"); setStrAttr(img,"ResampleDimensionZ","true"); setStrAttr(img,"RecordingDate",fmtTime(55624)); H5Gclose(img);
    for(size_t oc=0;oc<srcCh.size();oc++){ int c=srcCh[oc];
        std::vector<std::vector<uint64_t>> perTp; for(int t:colorTps) perTp.push_back(sumPartialHists(m,cd,0,c,t));
        float rMin=0,rMax=0; adjustColorRange(channelColorHistMulti(perTp),rMin,rMax);
        std::string col=fstr(c%3==0?1.f:0.f,3)+" "+fstr(c%3==1?1.f:0.f,3)+" "+fstr(c%3==2?1.f:0.f,3);
        hid_t ch=H5Gcreate2(out,("DataSetInfo/Channel "+std::to_string(oc)).c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
        setStrAttr(ch,"Name",c<3?IMS_CHNAME[c]:"Other channel"); setStrAttr(ch,"Description","(description not specified)");
        setStrAttr(ch,"LSMEmissionWavelength","700"); setStrAttr(ch,"OtherChannelParameter","OtherChannelValue");
        setStrAttr(ch,"ColorMode","BaseColor"); setStrAttr(ch,"Color",col);
        setStrAttr(ch,"ColorRange",fstr(rMin,3)+" "+fstr(rMax,3)); setStrAttr(ch,"ColorOpacity","0.000"); setStrAttr(ch,"GammaCorrection","1.000"); H5Gclose(ch);
    }
    hid_t ti=H5Gcreate2(out,"DataSetInfo/TimeInfo",lcpl,H5P_DEFAULT,H5P_DEFAULT);
    setStrAttr(ti,"FileTimePoints",std::to_string(nTime)); setStrAttr(ti,"DatasetTimePoints",std::to_string(nTime));
    for(int t=0;t<nTime;t++) setStrAttr(ti,("TimePoint"+std::to_string(t+1)).c_str(), fmtTime(55624+t)); H5Gclose(ti);
    hid_t ids=H5Gcreate2(out,"DataSetInfo/ImarisDataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); setStrAttr(ids,"NumberOfImages","1"); setStrAttr(ids,"Creator","TestC"); setStrAttr(ids,"Version","1.0.0"); H5Gclose(ids);
    hid_t lg=H5Gcreate2(out,"DataSetInfo/Log",lcpl,H5P_DEFAULT,H5P_DEFAULT); setStrAttr(lg,"Entries","0"); H5Gclose(lg);
    { hid_t g=H5Gcreate2(out,"DataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); H5Gclose(g); }
}
// write /DataSet/ResolutionLevel r/TimePoint tOut/Channel cOut/{ImageSize,Histogram,Data} from packs (r,cSrc,tSrc).
static void writeOneDataset(hid_t out,const Manifest& m,const std::string& cd,hid_t lcpl,hid_t dtype,int r,int tSrc,int cSrc,int tOut,int cOut){
    const Lev& L=m.lev[r];
    std::string dp="DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(tOut)+"/Channel "+std::to_string(cOut);
    hid_t g=H5Gcreate2(out,dp.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
    setStrAttr(g,"ImageSizeX",std::to_string(L.size[0])); setStrAttr(g,"ImageSizeY",std::to_string(L.size[1])); setStrAttr(g,"ImageSizeZ",std::to_string(L.size[2]));
    writeHistogramFromBins(g,sumPartialHists(m,cd,r,cSrc,tSrc));
    hsize_t fd[3]={L.block[2]*L.nch[2],L.block[1]*L.nch[1],L.block[0]*L.nch[0]}, chk[3]={L.block[2],L.block[1],L.block[0]};
    hid_t dcpl=H5Pcreate(H5P_DATASET_CREATE); H5Pset_chunk(dcpl,3,chk); if(m.elem==2||m.elem==4)H5Pset_shuffle(dcpl); setLZ4(dcpl);
    hid_t sp=H5Screate_simple(3,fd,NULL); hid_t d=H5Dcreate2(g,"Data",dtype,sp,H5P_DEFAULT,dcpl,H5P_DEFAULT);
    long perCT=(long)L.nt[0]*L.nt[1]*L.nt[2];
    #pragma omp parallel for schedule(dynamic)
    for(long tile=0;tile<perCT;tile++){
        forEachPackedChunk(packName(cd,r,cSrc,tSrc,tile),[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz){
            hsize_t off[3]={cz*L.block[2],cy*L.block[1],cx*L.block[0]};
            #pragma omp critical
            { H5Dwrite_chunk(d,H5P_DEFAULT,0,off,sz,(void*)by); }
        });
    }
    H5Dclose(d);H5Sclose(sp);H5Pclose(dcpl);H5Gclose(g);
}

static int doCombine(const std::string& wd,const std::string& outPath){
    Manifest m=readManifest(wd); std::string cd=wd+"/chunks"; int nT=m.nT, nC=m.nC, nR=(int)m.lev.size();
    hid_t fapl=H5Pcreate(H5P_FILE_ACCESS); H5Pset_libver_bounds(fapl,H5F_LIBVER_V18,H5F_LIBVER_V18);
    hid_t out=H5Fcreate(outPath.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,fapl);
    setStrAttr(out,"ImarisDataSet","ImarisDataSet"); setStrAttr(out,"ImarisVersion","5.5.0"); setStrAttr(out,"DataSetInfoDirectoryName","DataSetInfo"); setStrAttr(out,"ThumbnailDirectoryName","Thumbnail"); setStrAttr(out,"DataSetDirectoryName","DataSet");
    { uint32_t one=1; hsize_t d=1; hid_t sp=H5Screate_simple(1,&d,NULL); hid_t a=H5Acreate2(out,"NumberOfDataSets",H5T_STD_U32LE,sp,H5P_DEFAULT,H5P_DEFAULT); H5Awrite(a,H5T_NATIVE_UINT32,&one); H5Aclose(a);H5Sclose(sp); }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    bpSize X=m.X,Y=m.Y,Z=m.Z;
    hid_t img=H5Gcreate2(out,"DataSetInfo/Image",lcpl,H5P_DEFAULT,H5P_DEFAULT);
    setStrAttr(img,"Name","(name not specified)"); setStrAttr(img,"Description","(description not specified)");
    setStrAttr(img,"ExtMin0",fstr(0.f,9)); setStrAttr(img,"ExtMin1",fstr(0.f,9)); setStrAttr(img,"ExtMin2",fstr(0.f,9));
    setStrAttr(img,"ExtMax0",fstr((float)X*m.vox[0],9)); setStrAttr(img,"ExtMax1",fstr((float)Y*m.vox[1],9)); setStrAttr(img,"ExtMax2",fstr((float)Z*m.vox[2],9));
    setStrAttr(img,"X",std::to_string(X)); setStrAttr(img,"Y",std::to_string(Y)); setStrAttr(img,"Z",std::to_string(Z)); setStrAttr(img,"Unit","um");
    setStrAttr(img,"ResampleDimensionX","true"); setStrAttr(img,"ResampleDimensionY","true"); setStrAttr(img,"ResampleDimensionZ","true"); setStrAttr(img,"RecordingDate",fmtTime(55624)); H5Gclose(img);
    static const char* CHNAME[3]={"First channel","Second channel","Third channel"};
    for(int c=0;c<nC;c++){
        std::vector<std::vector<uint64_t>> perTp; for(int t=0;t<nT;t++) perTp.push_back(sumPartialHists(m,cd,0,c,t));
        float rMin=0,rMax=0; adjustColorRange(channelColorHistMulti(perTp),rMin,rMax);
        std::string col=fstr(c%3==0?1.f:0.f,3)+" "+fstr(c%3==1?1.f:0.f,3)+" "+fstr(c%3==2?1.f:0.f,3);
        hid_t ch=H5Gcreate2(out,("DataSetInfo/Channel "+std::to_string(c)).c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
        setStrAttr(ch,"Name",c<3?CHNAME[c]:"Other channel"); setStrAttr(ch,"Description","(description not specified)");
        setStrAttr(ch,"LSMEmissionWavelength","700"); setStrAttr(ch,"OtherChannelParameter","OtherChannelValue");
        setStrAttr(ch,"ColorMode","BaseColor"); setStrAttr(ch,"Color",col);
        setStrAttr(ch,"ColorRange",fstr(rMin,3)+" "+fstr(rMax,3)); setStrAttr(ch,"ColorOpacity","0.000"); setStrAttr(ch,"GammaCorrection","1.000"); H5Gclose(ch);
    }
    hid_t ti=H5Gcreate2(out,"DataSetInfo/TimeInfo",lcpl,H5P_DEFAULT,H5P_DEFAULT);
    setStrAttr(ti,"FileTimePoints",std::to_string(nT)); setStrAttr(ti,"DatasetTimePoints",std::to_string(nT));
    for(int t=0;t<nT;t++) setStrAttr(ti,("TimePoint"+std::to_string(t+1)).c_str(), fmtTime(55624+t)); H5Gclose(ti);
    hid_t ids=H5Gcreate2(out,"DataSetInfo/ImarisDataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); setStrAttr(ids,"NumberOfImages","1"); setStrAttr(ids,"Creator","TestC"); setStrAttr(ids,"Version","1.0.0"); H5Gclose(ids);
    hid_t lg=H5Gcreate2(out,"DataSetInfo/Log",lcpl,H5P_DEFAULT,H5P_DEFAULT); setStrAttr(lg,"Entries","0"); H5Gclose(lg);
    { hid_t g=H5Gcreate2(out,"DataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); H5Gclose(g); }
    hid_t dtype=m.elem==2?H5Tcopy(H5T_STD_U16LE):(m.elem==1?H5Tcopy(H5T_STD_U8LE):H5Tcopy(H5T_IEEE_F32LE));
    for(int r=0;r<nR;r++){ const Lev& L=m.lev[r];
        for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){
            std::string dp="DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
            hid_t g=H5Gcreate2(out,dp.c_str(),lcpl,H5P_DEFAULT,H5P_DEFAULT);
            setStrAttr(g,"ImageSizeX",std::to_string(L.size[0])); setStrAttr(g,"ImageSizeY",std::to_string(L.size[1])); setStrAttr(g,"ImageSizeZ",std::to_string(L.size[2]));
            writeHistogramFromBins(g,sumPartialHists(m,cd,r,c,t));
            hsize_t fd[3]={L.block[2]*L.nch[2],L.block[1]*L.nch[1],L.block[0]*L.nch[0]}, chk[3]={L.block[2],L.block[1],L.block[0]};
            hid_t dcpl=H5Pcreate(H5P_DATASET_CREATE); H5Pset_chunk(dcpl,3,chk); if(m.elem==2||m.elem==4)H5Pset_shuffle(dcpl); setLZ4(dcpl);
            hid_t sp=H5Screate_simple(3,fd,NULL); hid_t d=H5Dcreate2(g,"Data",dtype,sp,H5P_DEFAULT,dcpl,H5P_DEFAULT);
            // parallel-read this (r,t,c)'s tile .pack files across threads; single writer does H5Dwrite_chunk
            long perCT=(long)L.nt[0]*L.nt[1]*L.nt[2];
            #pragma omp parallel for schedule(dynamic)
            for(long tile=0;tile<perCT;tile++){
                forEachPackedChunk(packName(cd,r,c,t,tile),[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz){
                    hsize_t off[3]={cz*L.block[2],cy*L.block[1],cx*L.block[0]};
                    #pragma omp critical
                    { H5Dwrite_chunk(d,H5P_DEFAULT,0,off,sz,(void*)by); }
                });
            }
            H5Dclose(d);H5Sclose(sp);H5Pclose(dcpl);H5Gclose(g);
        }
    }
    H5Tclose(dtype); H5Pclose(lcpl); H5Pclose(fapl); H5Fclose(out);
    printf("merged %d channels x %d timepoints x %d levels -> %s (thumbnail TODO)\n",nC,nT,nR,outPath.c_str()); return 0;
}

// one (timepoint, channel) -> a single-tp/channel .ims (renumbered to TimePoint 0 / Channel 0). Independent
// of every other (t,c), so all nT*nC of these can run as one parallel array -- no shared HDF5 file, no single
// writer. Stitched afterward by `link` (external links). This is the fast combine.
static int combineOne(const std::string& wd,int T,int C,const std::string& outPath){
    Manifest m=readManifest(wd); std::string cd=wd+"/chunks"; int nR=(int)m.lev.size();
    if(m.lev.empty()){ fprintf(stderr,"bad manifest (run setup?)\n"); return 1; }
    if(T<0||T>=m.nT||C<0||C>=m.nC){ fprintf(stderr,"(t=%d,c=%d) out of range (nT=%d,nC=%d)\n",T,C,m.nT,m.nC); return 1; }
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    writeImsMeta(out,m,cd,lcpl,std::vector<int>{C},std::vector<int>{T},1);
    hid_t dtype=imsDtype(m.elem);
    for(int r=0;r<nR;r++) writeOneDataset(out,m,cd,lcpl,dtype,r,T,C,0,0);
    H5Tclose(dtype); H5Pclose(lcpl); H5Fclose(out);
    printf("combined (t=%d,c=%d) x %d levels -> %s\n",T,C,nR,outPath.c_str()); return 0;
}
static std::string partName(int t,int c){ char b[64]; snprintf(b,sizeof b,"part_t%d_c%d.ims",t,c); return b; }
// link .ims: a tiny file of external links into the per-(tp,channel) parts. Imaris follows the links and
// presents them as one dataset. `partRel` is the parts' folder relative to this file (they must live together).
static int buildLink(const std::string& wd,const std::string& outPath,const std::string& partRel){
    Manifest m=readManifest(wd); std::string cd=wd+"/chunks"; int nT=m.nT,nC=m.nC,nR=(int)m.lev.size();
    if(m.lev.empty()){ fprintf(stderr,"bad manifest (run setup?)\n"); return 1; }
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    writeImsMeta(out,m,cd,lcpl,iota0(nC),iota0(nT),nT);
    int made=0,failed=0;
    for(int r=0;r<nR;r++)for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){
        std::string linkPath="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
        std::string target="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint 0/Channel 0";
        std::string pf=(partRel.empty()?std::string():partRel+"/")+partName(t,c);
        if(H5Lcreate_external(pf.c_str(),target.c_str(),out,linkPath.c_str(),lcpl,H5P_DEFAULT)<0) failed++; else made++;
    }
    H5Pclose(lcpl); H5Fclose(out);
    printf("link -> %s: %d external links (%d failed) into %s/part_t{t}_c{c}.ims\n",outPath.c_str(),made,failed,partRel.empty()?".":partRel.c_str());
    return failed?1:0;
}

// ---- MONOLITHIC mode: parts come from the single-shot converter (one .ims per (tp,channel), NO packs).
// The link master then sources its level count + DataSetInfo from the parts themselves. ----
static std::string readStrAttr(hid_t o,const char* n){ if(H5Aexists(o,n)<=0) return std::string();
    hid_t a=H5Aopen(o,n,H5P_DEFAULT),t=H5Aget_type(a),s=H5Aget_space(a); hssize_t np=H5Sget_simple_extent_npoints(s);
    std::vector<char> b((size_t)(np>0?np:1)+1,0); H5Aread(a,t,b.data()); H5Aclose(a);H5Tclose(t);H5Sclose(s); return std::string(b.data()); }
static void setStrAttrOW(hid_t o,const char* n,const std::string& v){ if(H5Aexists(o,n)>0) H5Adelete(o,n); setStrAttr(o,n,v); }
static std::string partColorRange(const std::string& partDir,int c){          // channel c's ColorRange from part_t0_c{c}
    std::string p=partDir+"/"+partName(0,c); hid_t f=H5Fopen(p.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT); if(f<0) return std::string();
    std::string cr; if(H5Lexists(f,"DataSetInfo/Channel 0",H5P_DEFAULT)>0){ hid_t g=H5Gopen2(f,"DataSetInfo/Channel 0",H5P_DEFAULT); cr=readStrAttr(g,"ColorRange"); H5Gclose(g); }
    H5Fclose(f); return cr; }

static int linkParts(const std::string& wd,const std::string& outPath,const std::string& partRel){
    Manifest m=readManifest(wd); int nT=m.nT,nC=m.nC;
    if(nT<1||nC<1){ fprintf(stderr,"bad manifest (run setup?)\n"); return 1; }
    std::string outDir="."; { size_t s=outPath.rfind('/'); if(s!=std::string::npos) outDir=outPath.substr(0,s); }
    std::string partDir=partRel.empty()?outDir:outDir+"/"+partRel;
    std::string p00=partDir+"/"+partName(0,0);
    hid_t ref=H5Fopen(p00.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT);
    if(ref<0){ fprintf(stderr,"cannot open part %s (run the per-(tp,channel) convert jobs first)\n",p00.c_str()); return 1; }
    int nR=0; while(H5Lexists(ref,("/DataSet/ResolutionLevel "+std::to_string(nR)).c_str(),H5P_DEFAULT)>0) nR++;
    if(nR==0){ fprintf(stderr,"part %s has no ResolutionLevels\n",p00.c_str()); H5Fclose(ref); return 1; }
    hid_t out=imsCreate(outPath); if(out<0){ fprintf(stderr,"cannot create %s\n",outPath.c_str()); H5Fclose(ref); return 1; }
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    for(const char* a : {"ImarisDataSet","ImarisVersion","DataSetInfoDirectoryName","ThumbnailDirectoryName","DataSetDirectoryName"}){
        if(H5Aexists(ref,a)>0){ hid_t sa=H5Aopen(ref,a,H5P_DEFAULT),t=H5Aget_type(sa),s=H5Aget_space(sa);
            hssize_t n=H5Sget_simple_extent_npoints(s); std::vector<char> b((size_t)(n>0?n:1)+1,0); H5Aread(sa,t,b.data());
            hid_t da=H5Acreate2(out,a,t,s,H5P_DEFAULT,H5P_DEFAULT); H5Awrite(da,t,b.data()); H5Aclose(da);H5Tclose(t);H5Sclose(s);H5Aclose(sa); } }
    { uint32_t one=1; hsize_t d=1; hid_t sp=H5Screate_simple(1,&d,NULL); hid_t a=H5Acreate2(out,"NumberOfDataSets",H5T_STD_U32LE,sp,H5P_DEFAULT,H5P_DEFAULT); H5Awrite(a,H5T_NATIVE_UINT32,&one); H5Aclose(a);H5Sclose(sp); }
    H5Ocopy(ref,"DataSetInfo",out,"DataSetInfo",H5P_DEFAULT,lcpl);           // Image, Channel 0, TimeInfo, ... verbatim from part_t0_c0
    H5Fclose(ref);
    int failed=0;
    for(int c=1;c<nC;c++){ std::string dst="DataSetInfo/Channel "+std::to_string(c);   // duplicate the Channel-0 template for the other channels
        if(H5Lexists(out,dst.c_str(),H5P_DEFAULT)>0) H5Ldelete(out,dst.c_str(),H5P_DEFAULT);
        if(H5Ocopy(out,"DataSetInfo/Channel 0",out,dst.c_str(),H5P_DEFAULT,lcpl)<0){ fprintf(stderr,"channel %d template copy failed\n",c); failed++; } }
    for(int c=0;c<nC;c++){ std::string gp="DataSetInfo/Channel "+std::to_string(c); if(H5Lexists(out,gp.c_str(),H5P_DEFAULT)<=0) continue;
        hid_t g=H5Gopen2(out,gp.c_str(),H5P_DEFAULT);                        // per-channel color/name/range
        std::string col=fstr(c%3==0?1.f:0.f,3)+" "+fstr(c%3==1?1.f:0.f,3)+" "+fstr(c%3==2?1.f:0.f,3);
        setStrAttrOW(g,"Color",col); setStrAttrOW(g,"Name",c<3?IMS_CHNAME[c]:"Other channel");
        std::string cr=partColorRange(partDir,c); if(!cr.empty()) setStrAttrOW(g,"ColorRange",cr);
        H5Gclose(g); }
    if(H5Lexists(out,"DataSetInfo/TimeInfo",H5P_DEFAULT)>0) H5Ldelete(out,"DataSetInfo/TimeInfo",H5P_DEFAULT);  // rebuild TimeInfo for nT (part had 1)
    { hid_t ti=H5Gcreate2(out,"DataSetInfo/TimeInfo",lcpl,H5P_DEFAULT,H5P_DEFAULT);
      setStrAttr(ti,"FileTimePoints",std::to_string(nT)); setStrAttr(ti,"DatasetTimePoints",std::to_string(nT));
      for(int t=0;t<nT;t++) setStrAttr(ti,("TimePoint"+std::to_string(t+1)).c_str(),fmtTime(55624+t)); H5Gclose(ti); }
    { hid_t g=H5Gcreate2(out,"DataSet",lcpl,H5P_DEFAULT,H5P_DEFAULT); H5Gclose(g); }
    int made=0;
    for(int r=0;r<nR;r++)for(int t=0;t<nT;t++)for(int c=0;c<nC;c++){
        std::string linkPath="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint "+std::to_string(t)+"/Channel "+std::to_string(c);
        std::string target="/DataSet/ResolutionLevel "+std::to_string(r)+"/TimePoint 0/Channel 0";
        std::string pf=(partRel.empty()?std::string():partRel+"/")+partName(t,c);
        if(H5Lcreate_external(pf.c_str(),target.c_str(),out,linkPath.c_str(),lcpl,H5P_DEFAULT)<0) failed++; else made++; }
    H5Pclose(lcpl); H5Fclose(out);
    printf("linkparts -> %s: %d levels x %d tp x %d ch = %d external links (%d failed); DataSetInfo assembled from parts\n",
           outPath.c_str(),nR,nT,nC,made,failed);
    return failed?1:0;
}

// how many chunks are done vs expected (for the stateless monitor)
static bpSize expectedPacks(const Manifest& m){ bpSize n=0; for(size_t r=0;r<m.lev.size();++r) n+=(bpSize)jobsAtLevel(m,(int)r); return n; }  // one .pack per (level,channel,timepoint,tile) = per worker
static bpSize countPacks(const std::string& dir){ DIR* d=opendir(dir.c_str()); if(!d)return 0; struct dirent* e; bpSize n=0; while((e=readdir(d))){ size_t l=strlen(e->d_name); if(l>5&&!strcmp(e->d_name+l-5,".pack"))n++; } closedir(d); return n; }

static std::vector<std::string> listZarrs(const std::string& folder,const std::string& pattern){
    std::vector<std::string> out; DIR* d=opendir(folder.c_str()); if(!d)return out; struct dirent* e;
    while((e=readdir(d))){ std::string n=e->d_name; if(n.size()>5&&n.substr(n.size()-5)==".zarr"&&n.find(pattern)!=std::string::npos) out.push_back(folder+"/"+n); }
    closedir(d); std::sort(out.begin(),out.end()); return out;
}

bool isParallelCommand(const char* c){ return c && (!strcmp(c,"setup")||!strcmp(c,"worker")||!strcmp(c,"monitor")||!strcmp(c,"combineone")||!strcmp(c,"link")||!strcmp(c,"linkparts")||!strcmp(c,"layout")); }

// Chunk-size budget in BYTES. Bigger chunks = far fewer chunks = much faster combine (the combine cost
// is dominated by per-chunk overhead: ~27M tiny chunks on the real data). Trade-off: NOT byte-identical
// to the monolithic writer, and it re-tiles so the conversion must be re-run. Default 1 MiB (the
// ImarisWriter choice); override with env PIW_CHUNK_BYTES (e.g. 8388608 for ~8 MiB chunks).
static bpSize chunkBudgetBytes(){ bpSize b=1u<<20; const char* e=getenv("PIW_CHUNK_BYTES"); if(e){ char* end=NULL; unsigned long long v=strtoull(e,&end,10); if(end!=e&&v>=4096) b=(bpSize)v; } return b; }

int runParallel(int argc,char** argv){
    H5Eset_auto2(H5E_DEFAULT,NULL,NULL); registerLZ4();
    std::string cmd=argc>=2?argv[1]:"";
    if(cmd=="layout"&&argc>=5){ Vec3 img{(bpSize)atoll(argv[2]),(bpSize)atoll(argv[3]),(bpSize)atoll(argv[4])}; bpSize e=argc>5?atoll(argv[5]):2; bpSize cb=chunkBudgetBytes(); auto ls=GetOptimalImagePyramid((1024*1024)/e,img,true),bs=GetOptimalBlockSizes(cb/e,ls,1); printf("chunk budget %llu bytes\n",(unsigned long long)cb); printf("%zu levels\n",ls.size()); for(size_t i=0;i<ls.size();++i) printf("  L%zu size=%llu,%llu,%llu block=%llu,%llu,%llu\n",i,(unsigned long long)ls[i][0],(unsigned long long)ls[i][1],(unsigned long long)ls[i][2],(unsigned long long)bs[i][0],(unsigned long long)bs[i][1],(unsigned long long)bs[i][2]); return 0; }
    if(cmd=="setup"&&argc>=6){
        std::string folder=argv[2],patterns=argv[3],wd=argv[4];
        if(wd.find('/')==std::string::npos){ std::string f=folder; while(f.size()>1&&f.back()=='/')f.pop_back(); wd=f+"/"+wd; }
        bpSize cap[3]={0,0,0}; sscanf(argv[5],"%llu,%llu,%llu",(unsigned long long*)&cap[0],(unsigned long long*)&cap[1],(unsigned long long*)&cap[2]);
        if(!cap[0]||!cap[1]||!cap[2]){ fprintf(stderr,"bad tile size '%s' (expected X,Y,Z voxels)\n",argv[5]); return 1; }
        std::vector<std::string> pats; { std::stringstream ss(patterns); std::string p; while(std::getline(ss,p,',')) if(!p.empty()) pats.push_back(p); }
        if(pats.empty()){ fprintf(stderr,"no channel patterns given\n"); return 1; }
        Manifest m; m.nC=(int)pats.size(); for(int a=0;a<3;a++) m.cap[a]=cap[a];
        std::vector<std::vector<std::string>> perCh;
        for(auto&p:pats){ auto zs=listZarrs(folder,p); if(zs.empty()){ fprintf(stderr,"no .zarr matching '%s' in %s\n",p.c_str(),folder.c_str()); return 1; } perCh.push_back(zs); }
        m.nT=(int)perCh[0].size();
        for(int c=0;c<m.nC;c++) if((int)perCh[c].size()!=m.nT){ fprintf(stderr,"channel '%s' has %zu timepoints, expected %d\n",pats[c].c_str(),perCh[c].size(),m.nT); return 1; }
        m.zarrs.resize((size_t)m.nC*m.nT); for(int c=0;c<m.nC;c++)for(int t=0;t<m.nT;t++) m.zarrs[(size_t)c*m.nT+t]=perCh[c][t];
        zarr Z(zarrAt(m,0,0).c_str()); m.X=Z.get_shape(0); m.Y=Z.get_shape(1); m.Z=Z.get_shape(2); m.elem=Z.dtypeBytes();
        m.vox[0]=m.vox[1]=m.vox[2]=1.f; if(argc>6) sscanf(argv[6],"%g,%g,%g",&m.vox[0],&m.vox[1],&m.vox[2]);
        bpSize chunkBytes=chunkBudgetBytes();   // only the BLOCK budget changes; the pyramid (level count) is unchanged
        auto ls=GetOptimalImagePyramid((1024*1024)/m.elem,{m.X,m.Y,m.Z},true),bs=GetOptimalBlockSizes(chunkBytes/m.elem,ls,1);
        for(size_t r=0;r<ls.size();++r){ Lev L; L.size=ls[r]; L.block=bs[r];
            for(int a=0;a<3;a++){ L.nch[a]=(ls[r][a]+bs[r][a]-1)/bs[r][a]; L.tb[a]=std::max((bpSize)1,(cap[a]+bs[r][a]-1)/bs[r][a]); L.nt[a]=(L.nch[a]+L.tb[a]-1)/L.tb[a]; }
            m.lev.push_back(L); }
        std::string mk="mkdir -p \""+wd+"/chunks\""; int rc=system(mk.c_str());(void)rc; writeManifest(wd,m);
        long total=0; printf("setup: %llux%llux%llu elem=%llu, %d channels, %d timepoints, %zu levels\n",(unsigned long long)m.X,(unsigned long long)m.Y,(unsigned long long)m.Z,(unsigned long long)m.elem,m.nC,m.nT,m.lev.size());
        printf("  workdir: %s\n  tile: %llu,%llu,%llu voxels\n  chunk budget %llu bytes -> L0 block %llu,%llu,%llu voxels\n",wd.c_str(),(unsigned long long)cap[0],(unsigned long long)cap[1],(unsigned long long)cap[2],(unsigned long long)chunkBytes,(unsigned long long)m.lev[0].block[0],(unsigned long long)m.lev[0].block[1],(unsigned long long)m.lev[0].block[2]);
        printf("  run one worker array PER LEVEL, in order (level r+1 after level r):\n");
        for(int r=0;r<(int)m.lev.size();++r){ long jr=jobsAtLevel(m,r); total+=jr; printf("    level %d: worker <workdir> %d <index>   index 0..%ld  (%lld tiles/(ch*tp))\n",r,r,jr-1,(long long)((long long)m.lev[r].nt[0]*m.lev[r].nt[1]*m.lev[r].nt[2])); }
        printf("  total worker jobs across all levels: %ld\n",total);
        return 0;
    }
    if(cmd=="worker"&&argc>=5) return doWorker(argv[2],atoi(argv[3]),atol(argv[4]));
    if(cmd=="monitor"&&argc>=3){ std::string wd=argv[2],out=argc>3?argv[3]:wd+"/output.ims"; Manifest m=readManifest(wd);
        bpSize total=expectedPacks(m);
        for(;;){ bpSize done=countPacks(wd+"/chunks"); printf("monitor: %llu/%llu tiles\n",(unsigned long long)done,(unsigned long long)total); fflush(stdout);
            if(done>=total){ printf("all tiles present -> merging %s\n",out.c_str()); return doCombine(wd,out); } sleep(5); }
    }
    if(cmd=="combineone"&&argc>=6) return combineOne(argv[2],atoi(argv[3]),atoi(argv[4]),argv[5]);
    if(cmd=="link"&&argc>=5) return buildLink(argv[2],argv[3],argv[4]);
    if(cmd=="linkparts"&&argc>=5) return linkParts(argv[2],argv[3],argv[4]);
    fprintf(stderr,"usage: parallelimariswriter setup|worker|monitor|combineone|link|linkparts|layout ...\n"); return 2;
}
