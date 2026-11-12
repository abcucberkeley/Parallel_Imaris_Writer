// .idx sidecars of the single-file cluster output (format in monoidx.h) and small file helpers.
#include "monoidx.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <cctype>
#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
const char IDX_MAGIC[8]={'P','I','W','M','I','D','X','1'};
const uint32_t IDX_VERSION=1;
const size_t HDR_BYTES=64, LEVEL_BYTES=64, CHUNK_BYTES=32;
const size_t MAX_LEVELS=64, MAX_OWNER=255;

std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }
bool bad(std::string* err,const std::string& m){ if(err){ *err=m; } return false; }
void put16(std::vector<uint8_t>& b,uint16_t v){ for(int i=0;i<2;i++) b.push_back((uint8_t)(v>>(8*i))); }
void put32(std::vector<uint8_t>& b,uint32_t v){ for(int i=0;i<4;i++) b.push_back((uint8_t)(v>>(8*i))); }
void put64(std::vector<uint8_t>& b,uint64_t v){ for(int i=0;i<8;i++) b.push_back((uint8_t)(v>>(8*i))); }
uint16_t get16(const uint8_t* p){ return (uint16_t)(p[0]|(p[1]<<8)); }
uint32_t get32(const uint8_t* p){ uint32_t v=0; for(int i=3;i>=0;i--) v=(v<<8)|p[i]; return v; }
uint64_t get64(const uint8_t* p){ uint64_t v=0; for(int i=7;i>=0;i--) v=(v<<8)|p[i]; return v; }
size_t pad8(size_t n){ return (n+7)&~(size_t)7; }
bool lessLZYX(const MonoChunk& a,const MonoChunk& b){
    if(a.level!=b.level) return a.level<b.level;
    if(a.cz!=b.cz) return a.cz<b.cz;
    if(a.cy!=b.cy) return a.cy<b.cy;
    return a.cx<b.cx;
}
}  // namespace

uint64_t mono_fnv1a(const void* data,size_t n,uint64_t h){
    const uint8_t* p=(const uint8_t*)data;
    for(size_t i=0;i<n;i++){ h^=p[i]; h*=1099511628211ull; }
    return h;
}

MonoGeom mono_idx_geom(const MonoIdxLevel& l){
    MonoGeom g;
    for(int a=0;a<3;a++){ g.size[a]=l.size[a]; g.chunk[a]=l.chunk[a]; g.grid[a]=l.grid[a]; }
    g.dtype=l.dtype; return g;
}

bool mono_idx_validate(const MonoIdx& x,bool requireComplete,std::string* err){
    const std::string who="sidecar of '"+x.owner+"'";
    if(x.owner.empty()||x.owner.size()>MAX_OWNER) return bad(err,"sidecar: bad owner name");
    if(x.claimOffset<MONO_RAW_BEGIN||x.claimOffset%MONO_ALIGN||x.claimSize%MONO_ALIGN||x.blobBytes>x.claimSize
       ||x.claimSize>UINT64_MAX-x.claimOffset)
        return bad(err,who+": bad claim (offset "+u64s(x.claimOffset)+", size "+u64s(x.claimSize)+", blob "+u64s(x.blobBytes)+")");
    if(x.levels.empty()||x.levels.size()>MAX_LEVELS) return bad(err,who+": bad level count "+u64s(x.levels.size()));
    int slot[MAX_LEVELS];
    for(size_t i=0;i<MAX_LEVELS;i++){ slot[i]=-1; }
    for(size_t i=0;i<x.levels.size();i++){ const MonoIdxLevel& l=x.levels[i];
        if(l.level>=MAX_LEVELS||slot[l.level]>=0) return bad(err,who+": level "+u64s(l.level)+" invalid or listed twice");
        if(!mono_dtype_bytes(l.dtype)) return bad(err,who+": level "+u64s(l.level)+" has an unknown sample type");
        for(int a=0;a<3;a++) if(!l.size[a]||!l.chunk[a]||l.grid[a]!=(l.size[a]+l.chunk[a]-1)/l.chunk[a])
            return bad(err,who+": level "+u64s(l.level)+" has an inconsistent size/chunk/grid");
        slot[l.level]=(int)i; }
    std::vector<uint64_t> count(x.levels.size(),0);
    uint64_t blobEnd=x.claimOffset+x.blobBytes;
    for(size_t k=0;k<x.chunks.size();k++){ const MonoChunk& c=x.chunks[k];
        int li=c.level<MAX_LEVELS?slot[c.level]:-1;
        if(li<0) return bad(err,who+": chunk "+u64s(k)+" names unknown level "+u64s(c.level));
        const MonoIdxLevel& l=x.levels[(size_t)li];
        if(c.cx>=l.grid[0]||c.cy>=l.grid[1]||c.cz>=l.grid[2]) return bad(err,who+": chunk "+u64s(k)+" lies outside its level's grid");
        if(c.size==0||c.offset<x.claimOffset||c.offset>blobEnd||c.size>blobEnd-c.offset)
            return bad(err,who+": chunk "+u64s(k)+" ("+u64s(c.size)+" bytes at "+u64s(c.offset)+") is not inside the written blob");
        count[(size_t)li]++; }
    for(size_t i=0;i<x.levels.size();i++){ const MonoIdxLevel& l=x.levels[i];
        if(count[i]!=l.nChunks) return bad(err,who+": level "+u64s(l.level)+" lists "+u64s(l.nChunks)+" chunks but has "+u64s(count[i]));
        if(requireComplete&&l.nChunks!=(uint64_t)l.grid[0]*l.grid[1]*l.grid[2])
            return bad(err,who+": level "+u64s(l.level)+" is incomplete ("+u64s(l.nChunks)+" of "+u64s((uint64_t)l.grid[0]*l.grid[1]*l.grid[2])+" chunks)"); }
    std::vector<MonoChunk> v(x.chunks);
    std::sort(v.begin(),v.end(),lessLZYX);
    for(size_t k=1;k<v.size();k++) if(!lessLZYX(v[k-1],v[k]))
        return bad(err,who+": chunk ("+u64s(v[k].cx)+","+u64s(v[k].cy)+","+u64s(v[k].cz)+") of level "+u64s(v[k].level)+" is listed twice");
    std::sort(v.begin(),v.end(),[](const MonoChunk& a,const MonoChunk& b){ return a.offset<b.offset; });
    for(size_t k=1;k<v.size();k++) if(v[k-1].offset+v[k-1].size>v[k].offset)
        return bad(err,who+": chunks at "+u64s(v[k-1].offset)+" and "+u64s(v[k].offset)+" overlap");
    return true;
}

bool mono_idx_write(const std::string& path,const MonoIdx& x,std::string* err){
    if(!mono_idx_validate(x,false,err)) return false;
    std::vector<uint8_t> b; b.reserve(HDR_BYTES+pad8(x.owner.size())+LEVEL_BYTES*x.levels.size()+CHUNK_BYTES*x.chunks.size()+8);
    b.insert(b.end(),IDX_MAGIC,IDX_MAGIC+8);
    put32(b,IDX_VERSION); put32(b,(uint32_t)x.levels.size()); put64(b,x.chunks.size());
    put64(b,x.claimOffset); put64(b,x.claimSize); put64(b,x.blobBytes);
    put32(b,(uint32_t)x.timepoint); put32(b,(uint32_t)x.channel); put32(b,(uint32_t)x.owner.size()); put32(b,0);
    b.insert(b.end(),x.owner.begin(),x.owner.end()); b.resize(pad8(b.size()),0);
    for(const MonoIdxLevel& l:x.levels){
        put32(b,l.level); put32(b,l.dtype);
        for(int a=0;a<3;a++){ put64(b,l.size[a]); }
        for(int a=0;a<3;a++){ put32(b,l.chunk[a]); }
        for(int a=0;a<3;a++){ put32(b,l.grid[a]); }
        put64(b,l.nChunks); }
    for(const MonoChunk& c:x.chunks){
        put32(b,c.cx); put32(b,c.cy); put32(b,c.cz); put16(b,c.level); put16(b,0); put64(b,c.offset); put32(b,c.size); put32(b,c.mask); }
    put64(b,mono_fnv1a(b.data(),b.size()));
    return mono_write_atomic(path,b.data(),b.size(),err);
}

bool mono_idx_read(const std::string& path,MonoIdx& x,std::string* err){
    std::vector<uint8_t> b;
    if(!mono_read_file(path,b,err)) return false;
    const std::string who="sidecar "+path;
    if(b.size()<HDR_BYTES+8||memcmp(b.data(),IDX_MAGIC,8)) return bad(err,who+": not a sidecar (bad magic or too short)");
    const uint8_t* p=b.data();
    if(get32(p+8)!=IDX_VERSION) return bad(err,who+": unsupported version "+u64s(get32(p+8)));
    uint64_t nL=get32(p+12), nC=get64(p+16), own=get32(p+56);
    if(nL>MAX_LEVELS||own>MAX_OWNER||nC>b.size()/CHUNK_BYTES) return bad(err,who+": corrupt header");
    uint64_t want=HDR_BYTES+pad8((size_t)own)+LEVEL_BYTES*nL+CHUNK_BYTES*nC+8;
    if(want!=b.size()) return bad(err,who+": size "+u64s(b.size())+" does not match its header ("+u64s(want)+"); truncated?");
    if(get64(p+b.size()-8)!=mono_fnv1a(p,b.size()-8)) return bad(err,who+": checksum mismatch (corrupt)");
    MonoIdx r;
    r.claimOffset=get64(p+24); r.claimSize=get64(p+32); r.blobBytes=get64(p+40);
    r.timepoint=(int32_t)get32(p+48); r.channel=(int32_t)get32(p+52);
    r.owner.assign((const char*)p+HDR_BYTES,(size_t)own);
    const uint8_t* q=p+HDR_BYTES+pad8((size_t)own);
    r.levels.resize((size_t)nL);
    for(MonoIdxLevel& l:r.levels){
        l.level=get32(q); l.dtype=get32(q+4);
        for(int a=0;a<3;a++){ l.size[a]=get64(q+8+8*a); l.chunk[a]=get32(q+32+4*a); l.grid[a]=get32(q+44+4*a); }
        l.nChunks=get64(q+56); q+=LEVEL_BYTES; }
    r.chunks.resize((size_t)nC);
    for(MonoChunk& c:r.chunks){
        c.cx=get32(q); c.cy=get32(q+4); c.cz=get32(q+8); c.level=get16(q+12); c.reserved=0;
        c.offset=get64(q+16); c.size=get32(q+24); c.mask=get32(q+28); q+=CHUNK_BYTES; }
    if(!mono_idx_validate(r,false,err)) return false;
    x.owner.swap(r.owner); x.timepoint=r.timepoint; x.channel=r.channel; x.claimOffset=r.claimOffset;
    x.claimSize=r.claimSize; x.blobBytes=r.blobBytes; x.levels.swap(r.levels); x.chunks.swap(r.chunks);
    return true;
}

bool mono_read_file(const std::string& path,std::vector<uint8_t>& out,std::string* err){
    FILE* f=fopen(path.c_str(),"rb");
    if(!f) return bad(err,"cannot open "+path+": "+strerror(errno));
    out.clear(); uint8_t buf[1<<16]; size_t n; bool ok=true;
    while((n=fread(buf,1,sizeof buf,f))>0) out.insert(out.end(),buf,buf+n);
    if(ferror(f)) ok=false;
    fclose(f);
    return ok?true:bad(err,"cannot read "+path);
}

#ifdef _WIN32
std::string mono_process_tag(){ return "local"; }
bool mono_write_atomic(const std::string& path,const void*,size_t,std::string* err){
    return bad(err,"cannot write "+path+": the single-file cluster output is not supported on this platform");
}
#else
std::string mono_process_tag(){
    char h[256]={0}; if(gethostname(h,sizeof h-1)!=0) strcpy(h,"host");
    std::string t;
    for(const char* c=h;*c&&t.size()<64;c++) t+=(isalnum((unsigned char)*c)||*c=='-'||*c=='_')?*c:'_';
    if(t.empty()) t="host";
    return t+"."+u64s((uint64_t)getpid());
}
bool mono_write_atomic(const std::string& path,const void* data,size_t n,std::string* err){
    std::string tmp=path+".tmp."+mono_process_tag();
    int fd=open(tmp.c_str(),O_WRONLY|O_CREAT|O_TRUNC,0666);
    if(fd<0) return bad(err,"cannot create "+tmp+": "+strerror(errno));
    const char* p=(const char*)data; size_t put=0; bool ok=true;
    while(put<n){ ssize_t k=write(fd,p+put,n-put); if(k<0&&errno==EINTR) continue; if(k<=0){ ok=false; break; } put+=(size_t)k; }
    std::string why=ok?std::string():strerror(errno);
    if(ok&&fsync(fd)<0){ ok=false; why=strerror(errno); }
    if(close(fd)<0&&ok){ ok=false; why=strerror(errno); }
    if(ok&&rename(tmp.c_str(),path.c_str())<0){ ok=false; why=strerror(errno); }
    if(!ok){ unlink(tmp.c_str()); return bad(err,"cannot write "+path+": "+why); }
    return true;
}
#endif
