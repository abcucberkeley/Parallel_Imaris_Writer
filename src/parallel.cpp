// Parallel .ims tool (self-contained, multi-channel + multi-timepoint). Work is split by
// (channel, timepoint, spatial tile, pyramid level): many workers can share one volume.
// Each worker produces one .pack file (all its block chunks) for its tile at one level (level 0
// from the zarr, coarser levels by downsampling the finer level's packs -- so LEVELS RUN IN ORDER),
// plus a partial histogram. A parallel merge (OpenMP read, single HDF5 writer) assembles the
// packs into ONE .ims, byte-identical to a monolithic conversion. The subcommands are listed in
// parallel.h; the shared .ims building blocks are in imscore.h. Every failure prints a message on
// stderr and gives a non-zero exit code.
#include "imscore.h"
#include "imsthumb.h"
#include "zarr.h"
#include "parallelreadzarr.h"
#include <atomic>
#include <cerrno>
#include <ctime>
#include <dirent.h>
#include <unistd.h>
#include <omp.h>
#include "parallel.h"

using namespace imscore;

static int fail(const std::string& msg){ fprintf(stderr,"ERROR: %s\n",msg.c_str()); return 1; }
static int fail(const Status& s){ return fail(s.message); }
static const char* UINT16_ONLY="the tile workers and the pack-based combine (worker/monitor/combineone/link) take uint16 data only";
static bool parseLong(const char* s,long& v){ char* e=NULL; errno=0; long r=strtol(s,&e,10); if(e==s||*e||errno) return false; v=r; return true; }
static std::string dirOf(const std::string& p){ size_t s=p.find_last_of("/"
#ifdef _WIN32
    "\\"
#endif
    ); return s==std::string::npos?std::string("."):(s==0?std::string("/"):p.substr(0,s)); }

// ===================== worker: one (channel, timepoint, tile) at one level =====================
// Assemble a finer-level sub-region [fo, fo+fn) from its packs (zero beyond the finer size).
static Status assembleFinerRegion(const Manifest& m,const std::string& cd,int fr,int c,int t,const bpSize fo[3],const bpSize fn[3],std::vector<uint16_t>& fine){
    const Lev& F=m.lev[fr]; fine.assign((size_t)fn[0]*fn[1]*fn[2],0);
    bpSize fb[3]={F.block[0],F.block[1],F.block[2]}; const size_t rawBytes=(size_t)fb[0]*fb[1]*fb[2]*sizeof(uint16_t);
    // finer blocks covering the footprint, then the finer TILES holding those blocks
    bpSize cb0[3],cb1[3],ft0[3],ft1[3];
    for(int a=0;a<3;a++){ cb0[a]=fo[a]/fb[a]; cb1[a]=std::min((fo[a]+fn[a]-1)/fb[a],F.nch[a]-1); ft0[a]=cb0[a]/F.tb[a]; ft1[a]=std::min(cb1[a]/F.tb[a],(bpSize)F.nt[a]-1); }
    for(bpSize ftz=ft0[2];ftz<=ft1[2];ftz++)for(bpSize fty=ft0[1];fty<=ft1[1];fty++)for(bpSize ftx=ft0[0];ftx<=ft1[0];ftx++){
        long tileIdx=(long)ftx+(long)fty*F.nt[0]+(long)ftz*F.nt[0]*F.nt[1];
        std::string pp=packName(cd,fr,c,t,tileIdx);
        Status s=forEachPackedChunk(pp,[&](bpSize cz,bpSize cy,bpSize cx,const uint8_t* by,size_t sz)->Status{
            std::vector<uint8_t> vox; Status d=decodeChunk(by,sz,vox,rawBytes); if(!d.ok()) return Status::fail(pp+": "+d.message);
            const uint16_t* cv=(const uint16_t*)vox.data();
            bpSize bx0=cx*fb[0],by0=cy*fb[1],bz0=cz*fb[2];
            for(bpSize z=0;z<fb[2]&&bz0+z<F.size[2];z++)for(bpSize y=0;y<fb[1]&&by0+y<F.size[1];y++)for(bpSize x=0;x<fb[0]&&bx0+x<F.size[0];x++){
                bpSize GX=bx0+x,GY=by0+y,GZ=bz0+z;
                if(GX>=fo[0]&&GX<fo[0]+fn[0]&&GY>=fo[1]&&GY<fo[1]+fn[1]&&GZ>=fo[2]&&GZ<fo[2]+fn[2])
                    fine[(GX-fo[0])+(GY-fo[1])*fn[0]+(GZ-fo[2])*(size_t)fn[0]*fn[1]]=cv[x+y*fb[0]+z*(size_t)fb[0]*fb[1]];
            }
            return Status();
        },&F,tileIdx);
        if(!s.ok()) return Status::fail("level "+std::to_string(fr+1)+" needs level "+std::to_string(fr)+"'s packs (run its workers first): "+s.message);
    }
    return Status();
}
static int doWorker(const std::string& wd,int r,long index){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    if(m.elem!=2) return fail(std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
    std::string cd=wd+"/chunks"; int L=(int)m.lev.size();
    if(r<0||r>=L){ fprintf(stderr,"level %d out of range [0,%d)\n",r,L); return 1; }
    const Lev& Lr=m.lev[r]; long perCT=tilesPerCT(Lr), jobs=(long)m.nC*m.nT*perCT;
    if(index<0||index>=jobs){ fprintf(stderr,"index %ld out of range [0,%ld) for level %d\n",index,jobs,r); return 1; }
    int c=(int)(index/((long)m.nT*perCT)); long rem=index%((long)m.nT*perCT); int t=(int)(rem/perCT); long ti=rem%perCT;
    bpSize blk[3]={Lr.block[0],Lr.block[1],Lr.block[2]};
    // this tile's level-r block range and voxel extent (clipped to the level size)
    bpSize cb0[3],cb1[3]; tileBlockRange(Lr,ti,cb0,cb1);
    if(cb0[0]>=Lr.nch[0]||cb0[1]>=Lr.nch[1]||cb0[2]>=Lr.nch[2]) return 0;   // empty tile
    bpSize to[3]={cb0[0]*blk[0],cb0[1]*blk[1],cb0[2]*blk[2]};                // tile voxel origin
    bpSize td[3]={(cb1[0]-cb0[0])*blk[0],(cb1[1]-cb0[1])*blk[1],(cb1[2]-cb0[2])*blk[2]};  // tile voxel extent (block-aligned)
    std::vector<uint16_t> tile((size_t)td[0]*td[1]*td[2],0);
    if(r==0){
        bpSize ex=std::min(to[0]+td[0],m.X),ey=std::min(to[1]+td[1],m.Y),ez=std::min(to[2]+td[2],m.Z);
        if(to[0]<m.X&&to[1]<m.Y&&to[2]<m.Z){ bpSize rx=ex-to[0],ry=ey-to[1],rz=ez-to[2];
            uint16_t* reg=(uint16_t*)readZarrParallelHelper(zarrAt(m,c,t).c_str(),to[0],to[1],to[2],ex,ey,ez,0);
            if(!reg){ fprintf(stderr,"cannot read %s\n",zarrAt(m,c,t).c_str()); return 1; }
            for(bpSize z=0;z<rz;z++)for(bpSize y=0;y<ry;y++)for(bpSize x=0;x<rx;x++) tile[x+y*td[0]+z*(size_t)td[0]*td[1]]=reg[x+y*rx+z*(size_t)rx*ry];
            free(reg); }
    } else {
        bpSize st[3]={levStride(m,r,0),levStride(m,r,1),levStride(m,r,2)};
        bpSize fo[3]={to[0]*st[0],to[1]*st[1],to[2]*st[2]}, fn[3]={td[0]*st[0],td[1]*st[1],td[2]*st[2]};
        std::vector<uint16_t> fine; s=assembleFinerRegion(m,cd,r-1,c,t,fo,fn,fine); if(!s.ok()) return fail(s);
        bpSize dOut[3]; boxDown(fine,fn,st,tile,dOut);   // tile <- downsample(fine); dims td = fn/st
    }
    // zero voxels past the true level size (floor-halving tail; the writer zero-pads them)
    for(bpSize z=0;z<td[2];z++)for(bpSize y=0;y<td[1];y++)for(bpSize x=0;x<td[0];x++)
        if(to[0]+x>=Lr.size[0]||to[1]+y>=Lr.size[1]||to[2]+z>=Lr.size[2]) tile[x+y*td[0]+z*(size_t)td[0]*td[1]]=0;
    // pack every block chunk of this tile into ONE file (body streamed, index at the end);
    // accumulate the tile histogram over in-size voxels only.
    std::vector<uint64_t> hist(65536,0), pidx;
    std::string pp=packName(cd,r,c,t,ti), ptmp=pp+".tmp"; FILE* pf=fopen(ptmp.c_str(),"wb");
    if(!pf) return fail("cannot create "+ptmp+": "+strerror(errno));
    bool wok=true, eok=true;
    for(bpSize bz=cb0[2];wok&&eok&&bz<cb1[2];bz++)for(bpSize by=cb0[1];wok&&eok&&by<cb1[1];by++)for(bpSize bx=cb0[0];wok&&eok&&bx<cb1[0];bx++){
        std::vector<uint16_t> cbk((size_t)blk[0]*blk[1]*blk[2]);
        bpSize lx=(bx-cb0[0])*blk[0],ly=(by-cb0[1])*blk[1],lz=(bz-cb0[2])*blk[2];
        for(bpSize z=0;z<blk[2];z++)for(bpSize y=0;y<blk[1];y++)for(bpSize x=0;x<blk[0];x++){
            uint16_t v=tile[(lx+x)+(ly+y)*td[0]+(lz+z)*(size_t)td[0]*td[1]];
            cbk[x+y*blk[0]+z*(size_t)blk[0]*blk[1]]=v;
            if(bx*blk[0]+x<Lr.size[0]&&by*blk[1]+y<Lr.size[1]&&bz*blk[2]+z<Lr.size[2]) hist[v]++;
        }
        std::vector<uint8_t> comp; eok=encodeChunk((uint8_t*)cbk.data(),cbk.size()*2,comp);
        if(eok){ wok=fwrite(comp.data(),1,comp.size(),pf)==comp.size(); pidx.push_back(bz); pidx.push_back(by); pidx.push_back(bx); pidx.push_back(comp.size()); }
    }
    uint64_t N=pidx.size()/4;
    if(wok&&eok&&N) wok=fwrite(pidx.data(),8,pidx.size(),pf)==pidx.size();
    if(wok&&eok) wok=fwrite(&N,8,1,pf)==1;
    int e=errno; wok=(fclose(pf)==0)&&wok;
    if(!eok){ remove(ptmp.c_str()); return fail("LZ4 could not compress a chunk of "+pp); }
    if(!wok){ remove(ptmp.c_str()); return fail("cannot write "+ptmp+": "+strerror(e?e:errno)); }
    // the histogram first, then the pack: the pack's name is the commit point the monitor waits for
    s=writeFileAtomic(histName(cd,r,c,t,ti),hist.data(),hist.size()*sizeof(uint64_t));
    if(!s.ok()){ remove(ptmp.c_str()); return fail(s); }
    if(!renameReplace(ptmp,pp)){ e=errno; remove(ptmp.c_str()); return fail("cannot rename "+ptmp+" to "+pp+": "+strerror(e)); }
    return 0;
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
static int finishOutput(hid_t out,const std::string& outPath,Status s){
    if(H5Fclose(out)<0&&s.ok()) s=Status::fail("cannot close "+outPath+" (HDF5 error above)");
    if(!s.ok()){ remove(outPath.c_str()); return fail(s.message+"; removed "+outPath); }
    return 0;
}

static int doCombine(const Manifest& m,const std::string& wd,const std::string& outPath){
    std::string cd=wd+"/chunks"; int nT=m.nT, nC=m.nC, nR=(int)m.lev.size();
    hid_t out=imsCreate(outPath); if(out<0) return fail("cannot create "+outPath+" (HDF5 error above)");
    hid_t lcpl=H5Pcreate(H5P_LINK_CREATE); H5Pset_create_intermediate_group(lcpl,1);
    std::vector<ChannelDisplay> display;
    Status s=writeImsMeta(out,m,cd,lcpl,iota0(nC),iota0(nT),&display);
    hid_t dtype=imsDtype(m.elem);
    // parallel-read each (r,t,c)'s tile .pack files across threads; single writer does H5Dwrite_chunk
    for(int r=0;r<nR&&s.ok();r++) for(int t=0;t<nT&&s.ok();t++) for(int c=0;c<nC&&s.ok();c++) s=writeOneDataset(out,m,cd,lcpl,dtype,r,t,c,t,c);
    if(s.ok()) s=writePackThumbnail(out,m,cd,iota0(nC),0,display);
    H5Tclose(dtype); H5Pclose(lcpl);
    if(finishOutput(out,outPath,s)) return 1;
    printf("merged %d channels x %d timepoints x %d levels -> %s\n",nC,nT,nR,outPath.c_str()); return 0;
}

// one (timepoint, channel) -> a single-tp/channel .ims (renumbered to TimePoint 0 / Channel 0). Independent
// of every other (t,c), so all nT*nC of these can run as one parallel array -- no shared HDF5 file, no single
// writer. Stitched afterward by `link` (external links). This is the fast combine.
static int combineOne(const std::string& wd,int T,int C,const std::string& outPath){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    if(m.elem!=2) return fail(std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
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

// ===================== monitor: wait for every expected pack, check them, then combine =====================
// A tile is done when its pack and histogram exist (the worker renames the pack last). Only the expected
// names count, so stale or foreign packs in chunks/ are ignored. PIW_MONITOR_TIMEOUT=<seconds> gives up
// (exit 1, listing missing tiles) instead of waiting forever for a worker that failed.
static int doMonitor(const std::string& wd,const std::string& out){
    Manifest m; Status s=readManifest(wd,m); if(!s.ok()) return fail(s);
    if(m.elem!=2) return fail(std::string(UINT16_ONLY)+"; "+wd+" has "+std::to_string(m.elem)+"-byte voxels");
    std::string cd=wd+"/chunks";
    struct Want { int r,c,t; long tile; };
    std::vector<Want> want;   // one .pack per (level,channel,timepoint,tile) = per worker
    for(int r=0;r<(int)m.lev.size();r++) for(int c=0;c<m.nC;c++) for(int t=0;t<m.nT;t++) for(long ti=0;ti<tilesPerCT(m.lev[r]);ti++) want.push_back({r,c,t,ti});
    double timeout=0; { const char* e=getenv("PIW_MONITOR_TIMEOUT"); if(e&&*e){ char* end=NULL; timeout=strtod(e,&end); if(end==e||*end||timeout<0) return fail(std::string("bad PIW_MONITOR_TIMEOUT '")+e+"' (seconds)"); } }
    std::vector<char> done(want.size(),0); size_t nDone=0; time_t t0=time(NULL);
    for(;;){
        for(size_t i=0;i<want.size();i++){ const Want& w=want[i];
            if(!done[i]&&fileExists(packName(cd,w.r,w.c,w.t,w.tile))&&fileExists(histName(cd,w.r,w.c,w.t,w.tile))){ done[i]=1; nDone++; } }
        printf("monitor: %llu/%llu tiles\n",(unsigned long long)nDone,(unsigned long long)want.size()); fflush(stdout);
        if(nDone>=want.size()) break;
        double el=difftime(time(NULL),t0);
        if(timeout>0&&el>=timeout){ int shown=0;
            for(size_t i=0;i<want.size()&&shown<10;i++) if(!done[i]){ const Want& w=want[i]; fprintf(stderr,"missing: %s\n",packName(cd,w.r,w.c,w.t,w.tile).c_str()); shown++; }
            return fail("monitor gave up after "+std::to_string((long)el)+" s (PIW_MONITOR_TIMEOUT): "+std::to_string(want.size()-nDone)+" tiles missing"); }
        double nap=timeout>0?std::min(5.0,timeout-el):5.0; sleep((unsigned)std::max(1.0,nap));
    }
    // every pack must hold exactly its tile's chunks and every histogram must be whole before anything is written
    for(const Want& w:want){ std::string pp=packName(cd,w.r,w.c,w.t,w.tile); std::vector<uint64_t> idx;
        s=readPackIndex(pp,idx); if(s.ok()) s=checkPackTile(idx,m.lev[w.r],w.tile,pp);
        if(!s.ok()) return fail(s.message+" (re-run its worker)");
        std::string hp=histName(cd,w.r,w.c,w.t,w.tile); struct stat st;
        if(stat(hp.c_str(),&st)!=0||(size_t)st.st_size!=HIST_BYTES) return fail(hp+" is missing or not "+std::to_string(HIST_BYTES)+" bytes (re-run its worker)"); }
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
    // [vx,vy,vz] then the converter's metadata options (--name value or --name=value)
    int argi=6; const char* voxArg=NULL; if(argc>6&&strncmp(argv[6],"--",2)!=0){ voxArg=argv[6]; argi=7; }
    piw::ConvertOptions mo;
    for(;argi<argc;argi++){ std::string a=argv[argi], name, value;
        if(a.compare(0,2,"--")!=0) return fail("unexpected argument '"+a+"' (setup <folder> <patterns> <workdir> <cx,cy,cz> [vx,vy,vz] [--option value ...])");
        size_t eq=a.find('='); name=a.substr(2,eq==std::string::npos?std::string::npos:eq-2);
        if(!imsmeta::isMetadataOption(name)) return fail("unknown setup option --"+name);
        if(eq!=std::string::npos) value=a.substr(eq+1); else if(argi+1<argc) value=argv[++argi]; else return fail("--"+name+" needs a value");
        std::string e=imsmeta::parseMetadataOption(name,value,mo); if(!e.empty()) return fail(e); }
    if(wd.find_first_of("/"
#ifdef _WIN32
        "\\:"
#endif
        )==std::string::npos){ std::string f=folder; while(f.size()>1&&f.back()=='/')f.pop_back(); wd=f+"/"+wd; }   // a bare name: inside the input folder
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
    try{ zarr Z(zarrAt(m,0,0).c_str()); m.X=Z.get_shape(0); m.Y=Z.get_shape(1); m.Z=Z.get_shape(2); m.elem=Z.dtypeBytes(); }
    catch(const std::string& e){ return fail("cannot open "+zarrAt(m,0,0)+": "+e); }
    catch(const std::exception& e){ return fail("cannot open "+zarrAt(m,0,0)+": "+e.what()); }
    if(!m.X||!m.Y||!m.Z) return fail(zarrAt(m,0,0)+" has an empty shape");
    if(m.elem!=1&&m.elem!=2&&m.elem!=4) return fail(zarrAt(m,0,0)+" has "+std::to_string(m.elem)+"-byte voxels; only 8, 16 and 32-bit data are supported");
    m.vox[0]=m.vox[1]=m.vox[2]=0.108f;   // the single-node converter's default
    if(voxArg&&(sscanf(voxArg,"%g,%g,%g",&m.vox[0],&m.vox[1],&m.vox[2])!=3||!(m.vox[0]>0&&m.vox[1]>0&&m.vox[2]>0&&std::isfinite(m.vox[0])&&std::isfinite(m.vox[1])&&std::isfinite(m.vox[2]))))
        return fail(std::string("bad voxel size '")+voxArg+"' (expected vx,vy,vz > 0)");
    // the metadata, resolved as the single-node converter resolves it (channel names default to the patterns)
    mo.files=perCh; if(mo.channelNames.empty()) mo.channelNames=pats;
    { std::string e=imsmeta::resolveMetadata(mo,m.meta); if(!e.empty()) return fail(e); }
    bpSize chunkBytes=chunkBudgetBytes();   // only the BLOCK budget changes; the pyramid (level count) is unchanged
    m.lev=makeLevels(m.X,m.Y,m.Z,m.elem,cap,chunkBytes);
    Status s=makeDirs(wd+"/chunks"); if(!s.ok()) return fail(s);
    s=writeManifest(wd,m); if(!s.ok()) return fail(s);
    long total=0; printf("setup: %llux%llux%llu elem=%llu, %d channels, %d timepoints, %zu levels\n",(unsigned long long)m.X,(unsigned long long)m.Y,(unsigned long long)m.Z,(unsigned long long)m.elem,m.nC,m.nT,m.lev.size());
    printf("  workdir: %s\n  tile: %llu,%llu,%llu voxels\n  chunk budget %llu bytes -> L0 block %llu,%llu,%llu voxels\n",wd.c_str(),(unsigned long long)cap[0],(unsigned long long)cap[1],(unsigned long long)cap[2],(unsigned long long)chunkBytes,(unsigned long long)m.lev[0].block[0],(unsigned long long)m.lev[0].block[1],(unsigned long long)m.lev[0].block[2]);
    printf("  run one worker array PER LEVEL, in order (level r+1 after level r):\n");
    for(int r=0;r<(int)m.lev.size();++r){ long jr=jobsAtLevel(m,r); total+=jr; printf("    level %d: worker <workdir> %d <index>   index 0..%ld  (%lld tiles/(ch*tp))\n",r,r,jr-1,(long long)((long long)m.lev[r].nt[0]*m.lev[r].nt[1]*m.lev[r].nt[2])); }
    printf("  total worker jobs across all levels: %ld\n",total);
    if(m.elem!=2) fprintf(stderr,"WARNING: %s; this %llu-byte input can only go through per-(timepoint,channel) converter jobs + linkparts\n",UINT16_ONLY,(unsigned long long)m.elem);
    return 0;
}

bool isParallelCommand(const char* c){ return c && (!strcmp(c,"setup")||!strcmp(c,"worker")||!strcmp(c,"monitor")||!strcmp(c,"combineone")||!strcmp(c,"link")||!strcmp(c,"linkparts")||!strcmp(c,"layout")); }

int runParallel(int argc,char** argv){
    if(!registerLZ4()) return fail("cannot register the LZ4 filter id with HDF5");
    std::string cmd=argc>=2?argv[1]:"";
    if(cmd=="layout"&&argc>=5){ Vec3 img{(bpSize)atoll(argv[2]),(bpSize)atoll(argv[3]),(bpSize)atoll(argv[4])}; bpSize e=argc>5?atoll(argv[5]):2;
        if(!img[0]||!img[1]||!img[2]||(e!=1&&e!=2&&e!=4)) return fail("layout needs X Y Z > 0 and elemSize 1, 2 or 4");
        bpSize cb=chunkBudgetBytes(); auto ls=GetOptimalImagePyramid((1024*1024)/e,img,true),bs=GetOptimalBlockSizes(cb/e,ls,1); printf("chunk budget %llu bytes\n",(unsigned long long)cb); printf("%zu levels\n",ls.size()); for(size_t i=0;i<ls.size();++i) printf("  L%zu size=%llu,%llu,%llu block=%llu,%llu,%llu\n",i,(unsigned long long)ls[i][0],(unsigned long long)ls[i][1],(unsigned long long)ls[i][2],(unsigned long long)bs[i][0],(unsigned long long)bs[i][1],(unsigned long long)bs[i][2]); return 0; }
    if(cmd=="setup"&&argc>=6) return doSetup(argc,argv);
    if(cmd=="worker"&&argc>=5){ long r,i; if(!parseLong(argv[3],r)||!parseLong(argv[4],i)) return fail("worker needs a numeric level and index");
        return doWorker(argv[2],(int)r,i); }
    if(cmd=="monitor"&&argc>=3){ std::string wd=argv[2]; return doMonitor(wd,argc>3?argv[3]:wd+"/output.ims"); }
    if(cmd=="combineone"&&argc>=6){ long t,c; if(!parseLong(argv[3],t)||!parseLong(argv[4],c)) return fail("combineone needs a numeric timepoint and channel");
        return combineOne(argv[2],(int)t,(int)c,argv[5]); }
    if(cmd=="link"&&argc>=5) return buildLink(argv[2],argv[3],argv[4]);
    if(cmd=="linkparts"&&argc>=5) return linkParts(argv[2],argv[3],argv[4]);
    fprintf(stderr,"usage: parallelimariswriter setup|worker|monitor|combineone|link|linkparts|layout ...  (see parallel.h)\n"); return 2;
}
