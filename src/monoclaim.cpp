// Claims journal and blob placement for the single-file cluster output (see monoclaim.h).
#include "monoclaim.h"
#include "monoidx.h"
#include "monovfd.h"
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#ifndef _WIN32
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

uint64_t mono_round_up(uint64_t n){ return (n+MONO_ALIGN-1)/MONO_ALIGN*MONO_ALIGN; }

namespace {
bool bad(std::string* err,const std::string& m){ if(err) *err=m; return false; }
}  // namespace

#ifdef _WIN32
// ---------------- Windows: not supported ----------------
namespace { const char* NOSUP="the single-file cluster output is not supported on this platform"; }
MonoClaims::MonoClaims():jfd_(-1),tailHint_(0),seq_(0){}
MonoClaims::~MonoClaims(){}
bool MonoClaims::open(const std::string&,const std::string&,std::string* err){ return bad(err,NOSUP); }
bool MonoClaims::claim(const std::string&,uint64_t,MonoClaim&,std::string* err){ return bad(err,NOSUP); }
bool MonoClaims::find(const std::string&,MonoClaim&,bool* found,std::string* err){ if(found) *found=false; return bad(err,NOSUP); }
bool MonoClaims::seal(MonoClaim&,std::string* err){ return bad(err,NOSUP); }
bool MonoClaims::end(uint64_t*,bool*,std::string* err){ return bad(err,NOSUP); }
bool MonoClaims::list(std::vector<MonoClaim>&,bool*,std::string* err){ return bad(err,NOSUP); }
bool MonoClaims::rewindReceipt(const std::string&,std::string* err){ return bad(err,NOSUP); }
MonoBlobWriter::MonoBlobWriter():fd_(-1),direct_(false),open_(false),off_(0),cap_(0),bytes_(0),seg_(0),stop_(false),busy_(0){ cur_.buf=NULL; cur_.len=0; cur_.off=0; }
MonoBlobWriter::~MonoBlobWriter(){}
bool MonoBlobWriter::open(const std::string&,uint64_t,uint64_t,int,std::string* err,size_t){ return bad(err,NOSUP); }
bool MonoBlobWriter::append(const void*,size_t,std::string* err){ return bad(err,NOSUP); }
bool MonoBlobWriter::finish(std::string* err){ return bad(err,NOSUP); }
#else
namespace {
std::string u64s(uint64_t v){ char b[32]; snprintf(b,sizeof b,"%llu",(unsigned long long)v); return b; }

// ---------------- records ----------------
// 128 bytes, little-endian: "PIWCLM01", u64 slot, u64 offset, u64 size, u64 nonce, u32 kind, u32 0,
// owner[64] (NUL padded), u64 0, u64 FNV-1a of bytes [0,120). Journal records (claims/<n> or the fcntl
// journal) and receipts (owners/<owner>) share the format.
const size_t REC=128;
const char REC_MAGIC[8]={'P','I','W','C','L','M','0','1'};
const char JRN_MAGIC[8]={'P','I','W','J','R','N','0','1'};
enum { KIND_CLAIM=1, KIND_SEAL=2, KIND_PENDING=3 };
const char* SEAL_OWNER="#SEAL";

void put64(uint8_t* p,uint64_t v){ for(int i=0;i<8;i++) p[i]=(uint8_t)(v>>(8*i)); }
void put32(uint8_t* p,uint32_t v){ for(int i=0;i<4;i++) p[i]=(uint8_t)(v>>(8*i)); }
uint64_t get64(const uint8_t* p){ uint64_t v=0; for(int i=7;i>=0;i--) v=(v<<8)|p[i]; return v; }
uint32_t get32(const uint8_t* p){ uint32_t v=0; for(int i=3;i>=0;i--) v=(v<<8)|p[i]; return v; }

void encodeRec(const MonoClaim& c,uint32_t kind,uint8_t* b){
    memset(b,0,REC); memcpy(b,REC_MAGIC,8);
    put64(b+8,c.slot); put64(b+16,c.offset); put64(b+24,c.size); put64(b+32,c.nonce); put32(b+40,kind);
    memcpy(b+48,c.owner.data(),std::min<size_t>(c.owner.size(),63));
    put64(b+120,mono_fnv1a(b,120));
}
bool decodeRec(const uint8_t* b,MonoClaim& c,uint32_t* kind){
    if(memcmp(b,REC_MAGIC,8)||get64(b+120)!=mono_fnv1a(b,120)) return false;
    uint32_t k=get32(b+40); if(k!=KIND_CLAIM&&k!=KIND_SEAL&&k!=KIND_PENDING) return false;
    if(memchr(b+48,0,64)==NULL) return false;
    c.slot=get64(b+8); c.offset=get64(b+16); c.size=get64(b+24); c.nonce=get64(b+32);
    c.owner.assign((const char*)b+48); c.seal=k==KIND_SEAL; if(kind) *kind=k;
    return true;
}
bool validOwner(const std::string& o){
    if(o.empty()||o.size()>63||o[0]=='.') return false;
    for(char ch:o) if(!(isalnum((unsigned char)ch)||ch=='_'||ch=='-'||ch=='.')) return false;
    return true;
}

// ---------------- small POSIX helpers ----------------
int readExact(const std::string& p,uint8_t* b,size_t n){   // 0, or an errno (EIO for a short file)
    int fd=::open(p.c_str(),O_RDONLY);
    if(fd<0) return errno;
    size_t got=0; int rc=0;
    while(got<n){ ssize_t k=pread(fd,b+got,n-got,(off_t)got); if(k<0&&errno==EINTR) continue; if(k<0){ rc=errno; break; } if(k==0){ rc=EIO; break; } got+=(size_t)k; }
    close(fd); return rc;
}
bool readText(const std::string& p,std::string& out){
    FILE* f=fopen(p.c_str(),"rb"); if(!f) return false;
    char b[64]; size_t n=fread(b,1,sizeof b,f); fclose(f);
    out.assign(b,n); while(!out.empty()&&(out.back()=='\n'||out.back()=='\r'||out.back()==' ')) out.pop_back();
    return true;
}
bool mkdirs(const std::string& p,std::string* err){
    for(size_t i=1;i<=p.size();i++){
        if(i<p.size()&&p[i]!='/') continue;
        std::string d=p.substr(0,i);
        if(mkdir(d.c_str(),0777)<0&&errno!=EEXIST) return bad(err,"cannot create directory "+d+": "+strerror(errno));
    }
    struct stat st;
    if(stat(p.c_str(),&st)<0||!S_ISDIR(st.st_mode)) return bad(err,p+" is not a directory");
    return true;
}
int syncFd(int fd){
#ifdef __APPLE__
    return fsync(fd);
#else
    return fdatasync(fd);
#endif
}
uint64_t randomNonce(){
    std::random_device rd;
    uint64_t x=((uint64_t)rd()<<32)^rd()^((uint64_t)getpid()<<17)
               ^(uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
    x+=0x9E3779B97F4A7C15ull; x=(x^(x>>30))*0xBF58476D1CE4E5B9ull; x=(x^(x>>27))*0x94D049BB133111EBull; x^=x>>31;
    return x?x:1;
}
void sleepMs(double ms){ if(ms>0) std::this_thread::sleep_for(std::chrono::microseconds((long long)(ms*1000))); }
// Random back-off after losing a slot: up to 1, 2, 4 ... 64 ms.
void backoff(std::mt19937_64& rng,unsigned losses){
    double cap=(double)(1u<<std::min(losses,6u));
    sleepMs(std::uniform_real_distribution<double>(0.0,cap)(rng));
}

#ifdef __linux__
std::string unescapeMount(const char* s){
    std::string o;
    for(const char* p=s;*p;p++){
        if(p[0]=='\\'&&p[1]>='0'&&p[1]<='3'&&p[2]>='0'&&p[2]<='7'&&p[3]>='0'&&p[3]<='7'){ o+=(char)((p[1]-'0')*64+(p[2]-'0')*8+(p[3]-'0')); p+=3; }
        else o+=*p;
    }
    return o;
}
// fcntl locks on an NFS mount with nolock or local_lock=posix|all only exclude processes of the same node.
bool nodeLocalLocks(const std::string& dir,std::string& why){
    char rp[PATH_MAX];
    if(!realpath(dir.c_str(),rp)) return false;
    FILE* f=fopen("/proc/self/mounts","r");
    if(!f) return false;
    std::string best,bestType,bestOpts; char line[8192];
    while(fgets(line,sizeof line,f)){
        char src[4096],mnt[4096],type[256],opts[4096];
        if(sscanf(line,"%4095s %4095s %255s %4095s",src,mnt,type,opts)!=4) continue;
        std::string m=unescapeMount(mnt); size_t n=m.size();
        bool under=m=="/"||(strncmp(rp,m.c_str(),n)==0&&(rp[n]=='/'||rp[n]==0));
        if(under&&n>=best.size()){ best=m; bestType=type; bestOpts=opts; }
    }
    fclose(f);
    if(bestType.compare(0,3,"nfs")!=0) return false;
    std::string o=","+bestOpts+",";
    const char* local[]={",nolock,",",local_lock=posix,",",local_lock=all,"};
    for(const char* l:local) if(o.find(l)!=std::string::npos){
        why=best+" is mounted with "+std::string(l+1,strlen(l)-2)+": fcntl locks there only exclude processes on the same node"; return true; }
    return false;
}
#endif
}  // namespace


MonoClaims::MonoClaims():jfd_(-1),tailHint_(0),seq_(0){}
MonoClaims::~MonoClaims(){ if(jfd_>=0) close(jfd_); }

bool MonoClaims::open(const std::string& dir,const std::string& mode,std::string* err){
    if(!mode_.empty()) return bad(err,"claims: already open");
    std::string d=dir; while(d.size()>1&&d.back()=='/') d.pop_back();
    const char* env=getenv("PIW_CLAIM");
    std::string req=!mode.empty()?mode:(env&&*env?std::string(env):std::string("link"));
    bool explicitReq=!mode.empty()||(env&&*env);
    if(req!="link"&&req!="fcntl") return bad(err,"claims: unknown mode '"+req+"' (use link or fcntl)");
    if(!mkdirs(d,err)||!mkdirs(d+"/owners",err)||!mkdirs(d+"/tmp",err)) return false;
    // The run's mechanism is recorded once; publishing it with link() means nobody reads a partial file.
    std::string mf=d+"/claims.mode", got;
    if(!readText(mf,got)){
        std::string tmp=d+"/tmp/mode."+mono_process_tag();
        FILE* f=fopen(tmp.c_str(),"wb");
        bool wrote=f&&fprintf(f,"%s\n",req.c_str())>0;
        if(f&&fclose(f)!=0) wrote=false;
        if(!wrote){ unlink(tmp.c_str()); return bad(err,"claims: cannot write "+tmp); }
        int lr=link(tmp.c_str(),mf.c_str()); struct stat st;
        bool mine=lr==0||(stat(tmp.c_str(),&st)==0&&st.st_nlink>=2);
        unlink(tmp.c_str());
        if(mine) got=req;
        else for(int i=0;i<100&&!readText(mf,got);i++) sleepMs(20);
    }
    if(got!="link"&&got!="fcntl") return bad(err,"claims: "+mf+" is missing or corrupt ('"+got+"')");
    if(explicitReq&&got!=req) fprintf(stderr,"claims: this run uses '%s' claims (recorded in %s); '%s' was requested\n",got.c_str(),mf.c_str(),req.c_str());
    if(got=="link"){ if(!mkdirs(d+"/claims",err)) return false; }
    else {
#ifdef __linux__
        std::string why; if(nodeLocalLocks(d,why)) return bad(err,"claims: cannot use fcntl claims: "+why+" (use the default link claims)");
#endif
        jfd_=::open((d+"/claims.journal").c_str(),O_RDWR|O_CREAT,0666);
        if(jfd_<0) return bad(err,"claims: cannot open "+d+"/claims.journal: "+strerror(errno));
    }
    dir_=d; mode_=got; return true;
}

std::string MonoClaims::slotPath(uint64_t n) const { char b[32]; snprintf(b,sizeof b,"/claims/%012llu",(unsigned long long)n); return dir_+b; }
bool MonoClaims::slotExists(uint64_t n){ struct stat st; return stat(slotPath(n).c_str(),&st)==0; }
// First missing slot at or after hint. Records are dense (slot n exists => every slot below n exists), so
// gallop then bisect. A stale "missing" answer from an NFS client cache only makes the result too small,
// and the following link() then fails with EEXIST: the server decides.
uint64_t MonoClaims::findTail(uint64_t hint){
    if(hint>0&&!slotExists(hint-1)) hint=0;
    if(!slotExists(hint)) return hint;
    uint64_t lo=hint,step=1;
    while(slotExists(lo+step)){ lo+=step; step<<=1; }
    uint64_t hi=lo+step;
    while(hi-lo>1){ uint64_t mid=lo+(hi-lo)/2; if(slotExists(mid)) lo=mid; else hi=mid; }
    return hi;
}

bool MonoClaims::lockJournal(bool write,std::string* err){
    struct flock fl; memset(&fl,0,sizeof fl); fl.l_type=write?F_WRLCK:F_RDLCK; fl.l_whence=SEEK_SET; fl.l_start=0; fl.l_len=0;
    while(fcntl(jfd_,F_SETLKW,&fl)<0){
        if(errno!=EINTR) return bad(err,"claims: cannot lock "+dir_+"/claims.journal: "+strerror(errno)+" (use the default link claims)"); }
    return true;
}
void MonoClaims::unlockJournal(){ struct flock fl; memset(&fl,0,sizeof fl); fl.l_type=F_UNLCK; fl.l_whence=SEEK_SET; fcntl(jfd_,F_SETLK,&fl); }

// fcntl journal: a 128-byte header {"PIWJRN01", count, end, sealed, ..., checksum}, then record n at
// 128*(n+1). Call with the lock held. A header that fails its checksum is rebuilt from the records.
bool MonoClaims::readHeader(uint64_t* count,uint64_t* endOffset,bool* sealed,std::string* err){
    uint8_t h[REC]; ssize_t k=pread(jfd_,h,REC,0);
    if(k<0) return bad(err,"claims: cannot read the journal: "+std::string(strerror(errno)));
    if(k==0){ *count=0; *endOffset=MONO_RAW_BEGIN; *sealed=false; return true; }
    if(k==(ssize_t)REC&&!memcmp(h,JRN_MAGIC,8)&&get64(h+120)==mono_fnv1a(h,120)){
        *count=get64(h+8); *endOffset=get64(h+16); *sealed=get64(h+24)!=0; return true; }
    uint64_t n=0,e=MONO_RAW_BEGIN; bool s=false; uint8_t b[REC];
    while(!s&&pread(jfd_,b,REC,(off_t)(REC*(n+1)))==(ssize_t)REC){
        MonoClaim c; uint32_t kind=0;
        if(!decodeRec(b,c,&kind)||kind==KIND_PENDING||c.slot!=n||c.offset!=e) break;
        e=c.offset+c.size; s=c.seal; n++; }
    fprintf(stderr,"claims: journal header was damaged; rebuilt from %llu records\n",(unsigned long long)n);
    memset(h,0,REC); memcpy(h,JRN_MAGIC,8); put64(h+8,n); put64(h+16,e); put64(h+24,s?1:0); put64(h+120,mono_fnv1a(h,120));
    if(pwrite(jfd_,h,REC,0)!=(ssize_t)REC) return bad(err,"claims: cannot repair the journal header");
    *count=n; *endOffset=e; *sealed=s; return true;
}

bool MonoClaims::record(uint64_t slot,MonoClaim& out,std::string* err){
    uint8_t b[REC]; uint32_t kind=0; int rc=0;
    if(mode_=="link"){
        for(int i=0;i<50;i++){ rc=readExact(slotPath(slot),b,REC); if(rc==0) break; sleepMs(10); }   // NFS visibility lag
        if(rc) return bad(err,"claims: cannot read record "+u64s(slot)+": "+strerror(rc));
    } else {
        if(!lockJournal(false,err)) return false;
        uint64_t count=0,e=0; bool s=false;
        bool ok=readHeader(&count,&e,&s,err)&&(slot<count||bad(err,"claims: record "+u64s(slot)+" is beyond the journal end"))
              &&(pread(jfd_,b,REC,(off_t)(REC*(slot+1)))==(ssize_t)REC||bad(err,"claims: cannot read record "+u64s(slot)));
        unlockJournal();
        if(!ok) return false;
    }
    if(!decodeRec(b,out,&kind)||kind==KIND_PENDING||out.slot!=slot) return bad(err,"claims: record "+u64s(slot)+" is corrupt");
    return true;
}

bool MonoClaims::lastRecord(MonoClaim* last,bool* any,std::string* err){
    *any=false;
    if(mode_=="link"){
        uint64_t t=findTail(tailHint_);
        if(t==0) return true;
        tailHint_=t;
        if(!record(t-1,*last,err)) return false;
        *any=true; return true;
    }
    if(!lockJournal(false,err)) return false;
    uint64_t count=0,e=0; bool s=false; uint8_t b[REC]; uint32_t kind=0;
    bool ok=readHeader(&count,&e,&s,err);
    if(ok&&count>0){
        ok=pread(jfd_,b,REC,(off_t)(REC*count))==(ssize_t)REC&&decodeRec(b,*last,&kind)&&last->slot==count-1;
        if(!ok) bad(err,"claims: the last journal record is corrupt");
        *any=ok; }
    unlockJournal();
    return ok;
}

bool MonoClaims::readReceipt(const std::string& owner,MonoClaim& out,bool* pending,bool* exists,std::string* err){
    uint8_t b[REC]; uint32_t kind=0;
    int rc=readExact(dir_+"/owners/"+owner,b,REC);
    *exists=false; *pending=false;
    if(rc==ENOENT) return true;
    if(rc) return bad(err,"claims: cannot read the receipt of '"+owner+"': "+strerror(rc));
    if(!decodeRec(b,out,&kind)||kind==KIND_SEAL||out.owner!=owner)
        return bad(err,"claims: the receipt "+dir_+"/owners/"+owner+" is corrupt");
    *exists=true; *pending=kind==KIND_PENDING; return true;
}
bool MonoClaims::writeReceipt(const MonoClaim& c,bool pending,std::string* err){
    uint8_t b[REC]; encodeRec(c,pending?KIND_PENDING:KIND_CLAIM,b);
    return mono_write_atomic(dir_+"/owners/"+c.owner,b,REC,err);
}

bool MonoClaims::claim(const std::string& owner,uint64_t bytes,MonoClaim& out,std::string* err){
    if(mode_.empty()) return bad(err,"claims: not open");
    if(!validOwner(owner)) return bad(err,"claims: invalid owner name '"+owner+"'");
    uint64_t size=mono_round_up(bytes);
    MonoClaim rc; bool pending=false,exists=false;
    if(!readReceipt(owner,rc,&pending,&exists,err)) return false;
    uint64_t nonce=0;
    if(exists&&!pending){                                    // re-claim: the existing range
        MonoClaim jr;
        if(!record(rc.slot,jr,err)) return false;
        if(jr.seal||jr.owner!=owner||jr.nonce!=rc.nonce||jr.offset!=rc.offset||jr.size!=rc.size)
            return bad(err,"claims: the receipt of '"+owner+"' does not match journal record "+u64s(rc.slot));
        tailHint_=std::max(tailHint_,rc.slot+1);
        uint64_t e=0; bool sealed=false;
        if(!end(&e,&sealed,err)) return false;
        if(sealed) return bad(err,"claims: the journal is sealed (the file was finished); '"+owner+"' cannot write again");
        if(jr.size!=size) return bad(err,"claims: '"+owner+"' already claimed "+u64s(jr.size)+" bytes and now asks for "+u64s(size));
        out=jr; return true;
    }
    if(exists){                                              // pending: an earlier attempt may have appended
        nonce=rc.nonce;
        std::vector<MonoClaim> all; bool sealed=false;
        if(!list(all,&sealed,err)) return false;
        for(const MonoClaim& c:all) if(!c.seal&&c.owner==owner&&c.nonce==nonce){
            if(sealed) return bad(err,"claims: the journal is sealed (the file was finished); '"+owner+"' cannot write again");
            if(c.size!=size) return bad(err,"claims: '"+owner+"' already claimed "+u64s(c.size)+" bytes and now asks for "+u64s(size));
            if(!writeReceipt(c,false,err)) return false;
            out=c; return true; }
        if(sealed) return bad(err,"claims: the journal is sealed (the file was finished); '"+owner+"' cannot claim");
    } else {
        nonce=randomNonce();
        MonoClaim p; p.owner=owner; p.nonce=nonce; p.size=size;
        if(!writeReceipt(p,true,err)) return false;
    }
    MonoClaim c;
    if(!append(owner,size,nonce,false,c,err)) return false;
    std::string e;
    if(!writeReceipt(c,false,&e)) return bad(err,"claims: '"+owner+"' claimed "+u64s(c.size)+" bytes at "+u64s(c.offset)+" but its receipt failed: "+e);
    out=c; return true;
}

bool MonoClaims::append(const std::string& owner,uint64_t size,uint64_t nonce,bool seal,MonoClaim& out,std::string* err){
    return mode_=="link"?appendLink(owner,size,nonce,seal,out,err):appendFcntl(owner,size,nonce,seal,out,err);
}

// Link chain: record n is published by link(tmp, claims/<n>); offset(n) = end of record n-1.
bool MonoClaims::appendLink(const std::string& owner,uint64_t size,uint64_t nonce,bool seal,MonoClaim& out,std::string* err){
    const std::string who=seal?std::string(SEAL_OWNER):owner;
    std::string tmp; int fd=-1;
    auto drop=[&](){ if(fd>=0){ close(fd); unlink(tmp.c_str()); fd=-1; } };
    uint64_t n=findTail(tailHint_);
    MonoClaim prev; bool havePrev=false;
    if(n>0){ if(!record(n-1,prev,err)) return false; havePrev=true; }
    std::mt19937_64 rng(nonce^((uint64_t)getpid()<<32));
    unsigned losses=0,unsure=0;
    for(;;){
        if(havePrev&&prev.seal){
            drop();
            if(seal){ out=prev; return true; }
            return bad(err,"claims: the journal is sealed (the file was finished); '"+owner+"' cannot claim");
        }
        MonoClaim r; r.owner=who; r.slot=n; r.offset=havePrev?prev.offset+prev.size:MONO_RAW_BEGIN; r.size=size; r.nonce=nonce; r.seal=seal;
        uint8_t b[REC]; encodeRec(r,seal?KIND_SEAL:KIND_CLAIM,b);
        if(fd<0){                    // one temporary file per claim, rewritten for each slot tried
            tmp=dir_+"/tmp/"+(seal?std::string("SEAL"):owner)+"."+mono_process_tag()+"."+u64s(++seq_);
            fd=::open(tmp.c_str(),O_RDWR|O_CREAT|O_EXCL|O_DSYNC,0666);
            if(fd<0) return bad(err,"claims: cannot create "+tmp+": "+strerror(errno));
        }
        // O_DSYNC: the record is on the server before link() publishes its name, so readers on other nodes
        // never see it empty or stale (one synchronous write per attempt instead of write + fsync).
        if(pwrite(fd,b,REC,0)!=(ssize_t)REC){ int e=errno; drop(); return bad(err,"claims: cannot write "+tmp+": "+strerror(e)); }
        int lr=link(tmp.c_str(),slotPath(n).c_str()), le=errno;
        struct stat st;
        if(lr==0||(fstat(fd,&st)==0&&st.st_nlink>=2)){ drop(); tailHint_=n+1; out=r; return true; }
        if(le!=EEXIST&&le!=EIO&&le!=ESTALE&&le!=EINTR&&le!=EAGAIN){
            drop();
            return bad(err,"claims: link() into "+dir_+"/claims failed: "+strerror(le)
                       +(le==EPERM||le==EOPNOTSUPP?" (no hard links on this filesystem? try PIW_CLAIM=fcntl)":""));
        }
        MonoClaim cur; std::string e2;
        if(!record(n,cur,&e2)){
            // Cannot tell whether this link went through: never rewrite that file; retry the slot with a new one.
            drop();
            if(++unsure>20) return bad(err,e2);
            backoff(rng,++losses); continue;
        }
        if(cur.nonce==nonce&&cur.owner==who&&cur.seal==seal){ drop(); tailHint_=n+1; out=cur; return true; }   // it did
        prev=cur; havePrev=true; n++;
        backoff(rng,++losses);
        if(slotExists(n)){ n=findTail(n); if(!record(n-1,prev,err)){ drop(); return false; } }
    }
}

bool MonoClaims::appendFcntl(const std::string& owner,uint64_t size,uint64_t nonce,bool seal,MonoClaim& out,std::string* err){
    if(!lockJournal(true,err)) return false;
    uint64_t count=0,e=0; bool sealed=false; uint8_t b[REC]; bool ok=readHeader(&count,&e,&sealed,err);
    if(ok&&sealed){
        uint32_t kind=0;
        ok=seal&&pread(jfd_,b,REC,(off_t)(REC*count))==(ssize_t)REC&&decodeRec(b,out,&kind)&&out.seal;
        unlockJournal();
        return ok||bad(err,seal?"claims: the journal's seal record is corrupt"
                              :"claims: the journal is sealed (the file was finished); '"+owner+"' cannot claim");
    }
    MonoClaim r; r.owner=seal?std::string(SEAL_OWNER):owner; r.slot=count; r.offset=e; r.size=size; r.nonce=nonce; r.seal=seal;
    if(ok){
        encodeRec(r,seal?KIND_SEAL:KIND_CLAIM,b);
        uint8_t h[REC]; memset(h,0,REC); memcpy(h,JRN_MAGIC,8); put64(h+8,count+1); put64(h+16,e+size); put64(h+24,seal?1:0); put64(h+120,mono_fnv1a(h,120));
        // record first, then the header that commits it, then sync before the lock is released
        ok=pwrite(jfd_,b,REC,(off_t)(REC*(count+1)))==(ssize_t)REC&&pwrite(jfd_,h,REC,0)==(ssize_t)REC&&syncFd(jfd_)==0;
        if(!ok) bad(err,"claims: cannot write the journal: "+std::string(strerror(errno)));
    }
    unlockJournal();
    if(ok) out=r;
    return ok;
}

bool MonoClaims::find(const std::string& owner,MonoClaim& out,bool* found,std::string* err){
    *found=false;
    if(mode_.empty()) return bad(err,"claims: not open");
    MonoClaim rc,jr; bool pending=false,exists=false;
    if(!readReceipt(owner,rc,&pending,&exists,err)) return false;
    if(exists&&!pending&&record(rc.slot,jr,NULL)&&!jr.seal&&jr.owner==owner&&jr.nonce==rc.nonce){ out=jr; *found=true; return true; }
    std::vector<MonoClaim> all; bool sealed=false;
    if(!list(all,&sealed,err)) return false;
    for(size_t i=all.size();i-->0;) if(!all[i].seal&&all[i].owner==owner){ out=all[i]; *found=true; break; }
    return true;
}

bool MonoClaims::seal(MonoClaim& out,std::string* err){
    if(mode_.empty()) return bad(err,"claims: not open");
    MonoClaim last; bool any=false;
    if(!lastRecord(&last,&any,err)) return false;
    if(any&&last.seal){ out=last; return true; }
    return append(SEAL_OWNER,0,randomNonce(),true,out,err);
}

bool MonoClaims::end(uint64_t* endOffset,bool* sealed,std::string* err){
    if(mode_.empty()) return bad(err,"claims: not open");
    MonoClaim last; bool any=false;
    if(!lastRecord(&last,&any,err)) return false;
    *endOffset=any?last.offset+last.size:MONO_RAW_BEGIN; *sealed=any&&last.seal; return true;
}

bool MonoClaims::list(std::vector<MonoClaim>& out,bool* sealed,std::string* err){
    out.clear(); *sealed=false;
    if(mode_.empty()) return bad(err,"claims: not open");
    if(mode_=="link"){
        uint64_t t=findTail(tailHint_); tailHint_=t;
        out.resize((size_t)t);
        // Thousands of small files: read them from a few threads (latency-bound on network filesystems).
        unsigned nt=t>256?16u:1u; std::vector<std::string> errs(nt); std::vector<std::thread> th;
        for(unsigned w=0;w<nt;w++) th.push_back(std::thread([&,w](){
            for(uint64_t i=w;i<t&&errs[w].empty();i+=nt) record(i,out[(size_t)i],&errs[w]); }));
        for(std::thread& x:th) x.join();
        for(const std::string& e:errs) if(!e.empty()) return bad(err,e);
    } else {
        if(!lockJournal(false,err)) return false;
        uint64_t count=0,e=0; bool s=false;
        bool ok=readHeader(&count,&e,&s,err);
        std::vector<uint8_t> b;
        if(ok){ b.resize((size_t)(count*REC)); ok=count==0||pread(jfd_,b.data(),b.size(),(off_t)REC)==(ssize_t)b.size(); if(!ok) bad(err,"claims: cannot read the journal"); }
        unlockJournal();
        if(!ok) return false;
        out.resize((size_t)count);
        for(uint64_t i=0;i<count;i++){ uint32_t kind=0;
            if(!decodeRec(&b[(size_t)(i*REC)],out[(size_t)i],&kind)||kind==KIND_PENDING) return bad(err,"claims: journal record "+u64s(i)+" is corrupt"); }
    }
    uint64_t at=MONO_RAW_BEGIN;
    for(size_t i=0;i<out.size();i++){ const MonoClaim& c=out[i];
        if(c.slot!=i||c.offset!=at||c.size%MONO_ALIGN||(c.seal&&i+1!=out.size()))
            return bad(err,"claims: the journal is inconsistent at record "+u64s(i));
        at+=c.size; }
    *sealed=!out.empty()&&out.back().seal;
    return true;
}

bool MonoClaims::rewindReceipt(const std::string& owner,std::string* err){
    MonoClaim rc; bool pending=false,exists=false;
    if(!readReceipt(owner,rc,&pending,&exists,err)) return false;
    if(!exists) return bad(err,"claims: '"+owner+"' has no receipt");
    return writeReceipt(rc,true,err);
}

// ---------------- blob writer ----------------
MonoBlobWriter::MonoBlobWriter():fd_(-1),direct_(false),open_(false),off_(0),cap_(0),bytes_(0),seg_(0),stop_(false),busy_(0){ cur_.buf=NULL; cur_.len=0; cur_.off=0; }
MonoBlobWriter::~MonoBlobWriter(){
    if(open_){ { std::lock_guard<std::mutex> g(mu_); if(err_.empty()) err_="aborted"; } shutdown(); }
    for(uint8_t* b:all_) free(b);
    if(fd_>=0) close(fd_);
}

bool MonoBlobWriter::open(const std::string& path,uint64_t offset,uint64_t capacity,int threads,std::string* err,size_t segBytes){
    if(open_) return bad(err,"blob writer: already open");
    if(offset<MONO_RAW_BEGIN||offset%MONO_ALIGN||capacity%MONO_ALIGN||segBytes==0||segBytes%MONO_ALIGN)
        return bad(err,"blob writer: offset, capacity and piece size must be multiples of 4096 inside the raw region");
    threads=std::max(1,std::min(threads,64));
    int fd=-1; bool direct=false;
#ifdef O_DIRECT
    fd=::open(path.c_str(),O_WRONLY|O_CREAT|O_DIRECT,0666);
    if(fd>=0) direct=true;
    else if(errno!=EINVAL) return bad(err,"cannot open "+path+": "+strerror(errno));
#endif
    if(fd<0&&(fd=::open(path.c_str(),O_WRONLY|O_CREAT,0666))<0) return bad(err,"cannot open "+path+": "+strerror(errno));
#ifdef __APPLE__
    if(fcntl(fd,F_NOCACHE,1)==0) direct=true;
#endif
#ifdef O_DIRECT
    if(direct&&capacity>0){              // some filesystems accept O_DIRECT at open but refuse the writes
        void* z=NULL;
        if(posix_memalign(&z,MONO_ALIGN,MONO_ALIGN)){ close(fd); return bad(err,"blob writer: out of memory"); }
        memset(z,0,MONO_ALIGN);
        ssize_t k=pwrite(fd,z,MONO_ALIGN,(off_t)offset); int e=errno; free(z);   // inside our own range
        if(k!=(ssize_t)MONO_ALIGN){
            close(fd);
            if(k>=0||e!=EINVAL) return bad(err,"cannot write "+path+": "+strerror(k<0?e:EIO));
            if((fd=::open(path.c_str(),O_WRONLY|O_CREAT,0666))<0) return bad(err,"cannot open "+path+": "+strerror(errno));
            direct=false;
        }
    }
#endif
    for(int i=0;i<=threads;i++){
        void* p=NULL;
        if(posix_memalign(&p,MONO_ALIGN,segBytes)){ close(fd); return bad(err,"blob writer: out of memory"); }
        all_.push_back((uint8_t*)p); free_.push_back((uint8_t*)p);
    }
    fd_=fd; direct_=direct; off_=offset; cap_=capacity; bytes_=0; seg_=segBytes; stop_=false; busy_=0; err_.clear();
    cur_.buf=free_.back(); free_.pop_back(); cur_.len=0; cur_.off=offset;
    for(int i=0;i<threads;i++) threads_.push_back(std::thread(&MonoBlobWriter::worker,this));
    open_=true; return true;
}

void MonoBlobWriter::worker(){
    std::unique_lock<std::mutex> lk(mu_);
    for(;;){
        cv_.wait(lk,[this]{ return !queue_.empty()||stop_; });
        if(queue_.empty()) return;
        Seg s=queue_.front(); queue_.pop_front(); busy_++;
        bool skip=!err_.empty();
        lk.unlock();
        std::string e; size_t put=0;
        while(!skip&&put<s.len){
            ssize_t k=pwrite(fd_,s.buf+put,s.len-put,(off_t)(s.off+put));
            if(k<0&&errno==EINTR) continue;
            if(k<=0){ e="write of "+u64s(s.len)+" bytes at "+u64s(s.off)+" failed: "+strerror(k<0?errno:EIO); break; }
            put+=(size_t)k;
        }
        lk.lock();
        busy_--;
        if(!e.empty()&&err_.empty()) err_=e;
        free_.push_back(s.buf);
        cv_.notify_all();
    }
}

bool MonoBlobWriter::submit(std::string* err){
    std::unique_lock<std::mutex> lk(mu_);
    queue_.push_back(cur_);
    uint64_t next=cur_.off+cur_.len;
    cur_.buf=NULL; cur_.len=0;
    cv_.notify_all();
    cv_.wait(lk,[this]{ return !free_.empty()||!err_.empty(); });
    if(!err_.empty()) return bad(err,err_);
    cur_.buf=free_.back(); free_.pop_back(); cur_.off=next;
    return true;
}

bool MonoBlobWriter::append(const void* data,size_t n,std::string* err){
    if(!open_) return bad(err,"blob writer: not open");
    { std::lock_guard<std::mutex> g(mu_); if(!err_.empty()) return bad(err,err_); }
    if(n>cap_-bytes_) return bad(err,"blob writer: the blob is larger than its claim ("+u64s(cap_)+" bytes)");
    const uint8_t* p=(const uint8_t*)data;
    while(n>0){
        size_t k=std::min(n,seg_-cur_.len);
        memcpy(cur_.buf+cur_.len,p,k); cur_.len+=k; p+=k; n-=k; bytes_+=k;
        if(cur_.len==seg_&&!submit(err)) return false;
    }
    return true;
}

void MonoBlobWriter::shutdown(){
    { std::lock_guard<std::mutex> g(mu_); stop_=true; }
    cv_.notify_all();
    for(std::thread& t:threads_) t.join();
    threads_.clear();
}

bool MonoBlobWriter::finish(std::string* err){
    if(!open_) return bad(err,"blob writer: not open");
    std::string e;
    if(cur_.buf&&cur_.len>0){
        size_t padded=(size_t)mono_round_up(cur_.len);
        memset(cur_.buf+cur_.len,0,padded-cur_.len); cur_.len=padded;
        submit(&e);
    }
    shutdown();
    if(e.empty()){ std::lock_guard<std::mutex> g(mu_); e=err_; }
    if(e.empty()&&syncFd(fd_)<0) e="sync failed: "+std::string(strerror(errno));
    if(close(fd_)<0&&e.empty()) e="close failed: "+std::string(strerror(errno));
    fd_=-1; open_=false;
    return e.empty()?true:bad(err,e);
}
#endif

bool mono_place(const std::string& path,uint64_t offset,uint64_t capacity,const std::vector<MonoPiece>& pieces,
                int threads,MonoPlaceStats* st,std::string* err){
    std::chrono::steady_clock::time_point t0=std::chrono::steady_clock::now();
    MonoBlobWriter w;
    if(!w.open(path,offset,capacity,threads,err)) return false;
    for(const MonoPiece& p:pieces) if(!w.append(p.data,p.size,err)) return false;
    if(!w.finish(err)) return false;
    if(st){ st->bytes=w.bytes(); st->direct=w.direct(); st->threads=threads;
            st->seconds=std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count(); }
    return true;
}
