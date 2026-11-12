#ifndef PARALLEL_IMARIS_MONOCLAIM_H
#define PARALLEL_IMARIS_MONOCLAIM_H

// Claims and placement for the single-file cluster output (writer side).
//
// Claims: jobs on any node reserve disjoint, gap-free byte ranges of the mono file's raw region through an
// append-only journal of immutable records {slot, owner, offset, size} in a shared directory (<workdir>/mono).
// The first claim starts at 1 MiB, record n starts where record n-1 ends, and sizes are rounded up to 4096.
// Two mechanisms, chosen once per run and recorded in claims.mode so every job uses the same one:
//   link  (default): a lock-free chain, claims/<n>. A record is written (O_DSYNC) to a temporary file and
//         published with link(tmp, claims/<n>), accepted if link succeeded, the temporary file now has two
//         links, or slot n holds this attempt's record (the NFS-safe recipe: link is atomic on the server,
//         even where fcntl locks are node-local). A loser reads the winner's record and tries the next slot
//         after a random back-off.
//   fcntl (PIW_CLAIM=fcntl): one journal file under a whole-file F_SETLKW lock; refused on NFS mounts whose
//         locks are node-local (nolock, local_lock=posix|all).
// A claim is idempotent per owner: the same owner asking for the same size gets its existing range back
// (through a per-owner receipt, owners/<owner>, so no journal scan is needed). A different size is an error.
// The receipt is written in two steps (pending before the append, final after), so a job killed between
// appending and writing its receipt finds its record again (the only case that scans the journal).
// A SEAL record (written by the finisher) ends the journal: every later claim fails, including re-claims.
// Concurrent duplicates of one owner may each claim a range: that wastes space but never overlaps; the
// finisher uses the range named by the owner's sidecar, which it checks against the journal.
//
// Placement: MonoBlobWriter writes one owner's blob into its claimed range of the mono file, with O_DIRECT in
// large aligned pieces from a few threads (macOS: F_NOCACHE), buffered + fdatasync where O_DIRECT is refused,
// zero padded to a multiple of 4096, checking every write.
//
// Linux and macOS only: on Windows every call fails with "not supported on this platform".
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

uint64_t mono_round_up(uint64_t n);   // to a multiple of 4096

struct MonoClaim {
    std::string owner;
    uint64_t slot = 0;      // journal position
    uint64_t offset = 0;    // first byte of the range
    uint64_t size = 0;      // bytes (a multiple of 4096; 0 for the seal)
    uint64_t nonce = 0;     // random id of the claiming attempt
    bool seal = false;      // the SEAL record: offset = end of the raw region
};

class MonoClaims {
public:
    MonoClaims();
    ~MonoClaims();
    // dir: shared directory of the run (created if missing). mode: "link", "fcntl", or "" for $PIW_CLAIM
    // (default link). If the run already recorded a mode, that one is used.
    bool open(const std::string& dir, const std::string& mode, std::string* err);
    const std::string& mode() const { return mode_; }
    // Claims bytes (rounded up to 4096) for owner ([A-Za-z0-9_.-], 1-63 chars, not starting with '.').
    bool claim(const std::string& owner, uint64_t bytes, MonoClaim& out, std::string* err);
    // The owner's claim (its receipt, or the latest record of that owner in the journal).
    bool find(const std::string& owner, MonoClaim& out, bool* found, std::string* err);
    // Appends the SEAL record (or returns the existing one). Its offset is the end of the raw region.
    bool seal(MonoClaim& out, std::string* err);
    // End of the last record (MONO_RAW_BEGIN if none) and whether the journal is sealed.
    bool end(uint64_t* endOffset, bool* sealed, std::string* err);
    // Every record in slot order, checked for contiguity (the finisher validates sidecars against it).
    bool list(std::vector<MonoClaim>& out, bool* sealed, std::string* err);
    // Test hook: turns the owner's final receipt back into a pending one, as if the claiming job had died
    // between appending its record and writing the receipt.
    bool rewindReceipt(const std::string& owner, std::string* err);
private:
    bool append(const std::string& owner, uint64_t size, uint64_t nonce, bool seal, MonoClaim& out, std::string* err);
    bool appendLink(const std::string& owner, uint64_t size, uint64_t nonce, bool seal, MonoClaim& out, std::string* err);
    bool appendFcntl(const std::string& owner, uint64_t size, uint64_t nonce, bool seal, MonoClaim& out, std::string* err);
    bool record(uint64_t slot, MonoClaim& out, std::string* err);
    bool lastRecord(MonoClaim* last, bool* any, std::string* err);
    uint64_t findTail(uint64_t hint);
    bool slotExists(uint64_t n);
    std::string slotPath(uint64_t n) const;
    bool readReceipt(const std::string& owner, MonoClaim& out, bool* pending, bool* exists, std::string* err);
    bool writeReceipt(const MonoClaim& c, bool pending, std::string* err);
    bool lockJournal(bool write, std::string* err);
    void unlockJournal();
    bool readHeader(uint64_t* count, uint64_t* endOffset, bool* sealed, std::string* err);
    std::string dir_, mode_;
    int jfd_;
    uint64_t tailHint_, seq_;
};

// Writes one blob (the bytes appended, in order) at offset into a claimed range of capacity bytes.
class MonoBlobWriter {
public:
    MonoBlobWriter();
    ~MonoBlobWriter();
    // path: the mono file (created if missing, never truncated). offset/capacity: multiples of 4096.
    // segBytes: bytes per write (a multiple of 4096); threads: concurrent writes.
    bool open(const std::string& path, uint64_t offset, uint64_t capacity, int threads, std::string* err,
              size_t segBytes = 64u << 20);
    bool append(const void* data, size_t n, std::string* err);
    // Zero pads the last piece to 4096, waits for every write, syncs and closes.
    bool finish(std::string* err);
    uint64_t bytes() const { return bytes_; }   // appended so far
    bool direct() const { return direct_; }     // O_DIRECT (or F_NOCACHE) in use
private:
    struct Seg { uint8_t* buf; size_t len; uint64_t off; };
    void worker();
    bool submit(std::string* err);
    void shutdown();
    int fd_;
    bool direct_, open_;
    uint64_t off_, cap_, bytes_;
    size_t seg_;
    Seg cur_;
    std::vector<uint8_t*> all_, free_;
    std::deque<Seg> queue_;
    std::vector<std::thread> threads_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_;
    size_t busy_;
    std::string err_;
};

struct MonoPiece { const void* data; size_t size; };
struct MonoPlaceStats { uint64_t bytes = 0; double seconds = 0; bool direct = false; int threads = 0; };
// Convenience: one blob from pieces in memory.
bool mono_place(const std::string& path, uint64_t offset, uint64_t capacity, const std::vector<MonoPiece>& pieces,
                int threads, MonoPlaceStats* st, std::string* err);

#endif
