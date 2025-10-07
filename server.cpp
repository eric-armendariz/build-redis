// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <time.h>
// C++
#include <string>
#include <vector>
// proj
#include "hashtable.hpp"
#include "zset.hpp"
#include "common.hpp"
#include "list.h"
#include "heap.h"
#include "threadpool.h"
#include <iostream>

typedef std::vector<uint8_t> Buffer;

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg) {
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static uint64_t getMonotonicMs() {
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000 + tv.tv_nsec / 1000 / 1000;
}

static void fd_set_nb(int fd) {
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }
}

// append to the back
static void bufAppend(Buffer &buf, const uint8_t *data, size_t len) {
    if (len == 0) {
        return;
    }
    assert(data != nullptr);
    buf.insert(buf.end(), data, data + len);
}
// remove from the front
static void bufConsume(Buffer &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + n);
}

const size_t k_max_msg = 32 << 20;  // likely larger than the kernel buffer

const uint64_t k_idle_timeout_ms = 5 * 1000;

struct Conn {
    int fd = -1;
    // state of conn for the event loop
    bool wantRead = false;
    bool wantWrite = false;
    bool wantClose = false;
    // buffers containing I/O of conn
    Buffer incoming;
    Buffer outgoing;
    // timers
    uint64_t lastActiveMs = 0;
    DList idleNode;
};

enum {
    ERR_UNKNOWN = 1,
    ERR_TOO_BIG = 2,
    ERR_BAD_ARG = 3,
};

bool readUInt32(const uint8_t *&curr, const uint8_t *end, uint32_t &out) {
    if (curr + 4 > end) {
        return false;
    }
    memcpy(&out, curr, 4);
    curr += 4;
    return true;
}

bool readStr(const uint8_t *&curr, const uint8_t *end, size_t size, std::string &out) {
    if (curr + size > end) {
        return false;
    }
    out.assign(curr, curr + size);
    curr += size;
    return true;
}

uint32_t parseRequest(const uint8_t *data, size_t size, std::vector<std::string> &out) {
    const uint8_t *end = data + size;
    uint32_t nStr = 0;
    if (!readUInt32(data, end, nStr)) {
        return -1;
    }

    while (out.size() < nStr) {
        uint32_t len = 0;
        if (!readUInt32(data, end, len)) {
            return -1;
        }

        out.push_back(std::string());
        if (!readStr(data, end, len, out.back())) {
            return -1;
        }
    }
    if (data != end) {
        return -1;
    }
    return 0;
}

// supported data types
enum {
    TAG_NIL = 0,
    TAG_ERR = 1,
    TAG_INT = 2,
    TAG_STR = 3,
    TAG_DBL = 4,
    TAG_ARR = 5,
};

// help functions for the serialization
static void bufAppendU8(Buffer &buf, uint8_t data) {
    buf.push_back(data);
}
static void bufAppendU32(Buffer &buf, uint32_t data) {
    bufAppend(buf, (const uint8_t *)&data, 4);
}
static void bufAppendI64(Buffer &buf, int64_t data) {
    bufAppend(buf, (const uint8_t *)&data, 8);
}
static void bufAppendDbl(Buffer &buf, double data) {
    bufAppend(buf, (const uint8_t *)&data, 8);
}

static void outNil(Buffer &out) {
    bufAppendU8(out, TAG_NIL);
}

static void outErr(Buffer &out, uint32_t code, const std::string &msg) {
    bufAppendU8(out, TAG_ERR);
    bufAppendU32(out, code);
    bufAppendU32(out, (uint32_t)msg.size());
    bufAppend(out, (const uint8_t *)msg.data(), msg.size());
}

static void outInt(Buffer &out, int64_t val) {
    bufAppendU8(out, TAG_INT);
    bufAppendI64(out, val);
}

static void outStr(Buffer &out, const char *s, size_t size) {
    bufAppendU8(out, TAG_STR);
    bufAppendU32(out, (uint32_t)size);
    bufAppend(out, (const uint8_t *)s, size);
}

static void outDbl(Buffer &out, double val) {
    bufAppendU8(out, TAG_DBL);
    bufAppendDbl(out, val);
}

static void outArr(Buffer &out, size_t size) {
    bufAppendU8(out, TAG_ARR);
    bufAppendU32(out, (uint32_t)size);
}

static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

static struct {
    HMap db;
    // a map of all client connections
    std::vector<Conn *> fd2conn;
    // timers for idle connections
    DList idleList;
    // timers for TTLs
    std::vector<HeapItem> heap;
    ThreadPool threadPool;
} gData;

static Conn *handleAccept(int fd) {
    // accept
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0) {
        msg_errno("accept() error");
        return NULL;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
    );

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd);

    // create a `struct Conn`
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->wantRead = true;
    conn->lastActiveMs = getMonotonicMs();
    dlistInsertBefore(&gData.idleList, &conn->idleNode);
    return conn;
}

static void connDestroy(Conn *conn) {
    (void)close(conn->fd);
    gData.fd2conn[conn->fd] = NULL;
    dlistDetach(&conn->idleNode);
    delete conn;
}

enum {
    T_INIT = 0,
    T_STR  = 1,
    T_ZSET = 2,
};

struct Entry {
    struct HNode node;
    std::string key;
    // for TTL
    size_t heapIdx = -1;
    // value
    uint32_t type = 0;
    union {
        std::string str;
        ZSet zset;
    };

    explicit Entry(uint32_t type) : type(type) {
        if (type == T_ZSET) {
            new (&zset) ZSet;
        } else if (type == T_STR) {
            new (&str) std::string;
        }
    }

    ~Entry() {
        if (type == T_ZSET) {
            zsetClear(&zset);
        } else if (type == T_STR) {
            str.~basic_string();
        }
    }
};

struct LookupKey {
    struct HNode node;
    std::string key;
};

static void heapUpsert(std::vector<HeapItem> &a, size_t pos, HeapItem t) {
    if (pos < a.size()) {
        a[pos] = t;
    } else {
        pos = a.size();
        a.push_back(t);
    }
    heapUpdate(a.data(), pos, a.size());
}

static void heapDelete(std::vector<HeapItem> &a, size_t pos) {
    // swap erased item with last item
    a[pos] = a.back();
    a.pop_back();
    if (pos < a.size()) {
        heapUpdate(a.data(), pos, a.size());
    }
}

// set or remove the TTL
static void entrySetTTL(Entry *ent, int64_t ttlMs) {
    if (ttlMs < 0 && ent->heapIdx != (size_t)-1) {
        // remove negative ttl's
        heapDelete(gData.heap, ent->heapIdx);
        ent->heapIdx = (size_t)-1;
    } else if (ttlMs >= 0) {
        uint64_t expireAt = getMonotonicMs() + (uint64_t)ttlMs;
        HeapItem item = {expireAt, &ent->heapIdx};
        heapUpsert(gData.heap, ent->heapIdx, item);
    } 
}

static Entry *entryNew(uint32_t type) {
    Entry *ent = new Entry(type);
    ent->type = type;
    return ent;
}

static void entryDelSync(Entry *ent) {
    if (ent->type == T_ZSET) {
        zsetClear(&ent->zset);
    }
    delete ent;
}

static void entryDelFunc(void *arg) {
    entryDelSync((Entry *)arg);
}

static void entryDel(Entry *ent) {
    entrySetTTL(ent, -1);
    // Run destructor in thread pool for large data structures
    size_t setSize = (ent->type == T_ZSET) ? hmSize(&ent->zset.hmap) : 0;
    const size_t largeContainerSize = 1000;
    if (setSize > largeContainerSize) {
        threadPoolQueue(&gData.threadPool, &entryDelFunc, ent);
    } else {
        entryDelSync(ent);
    }
}

static bool entryEq(HNode *node, HNode *key) {
    struct Entry *ent = container_of(node, struct Entry, node);
    struct LookupKey *keydata = container_of(key, struct LookupKey, node);
    return ent->key == keydata->key;
}

static size_t outBeginArr(Buffer &out) {
    out.push_back(TAG_ARR);
    bufAppendU32(out, 0);
    return out.size() - 4;
}

static void outEndArr(Buffer &out, size_t ctx, uint32_t n) {
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}

static const ZSet k_empty_zset;

static ZSet *expectZset(std::string &name) {
    LookupKey key;
    key.key.swap(name);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (!node) {
        return (ZSet *)&k_empty_zset;
    }
    Entry *ent = container_of(node, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

static void doZQuery(std::vector<std::string> &cmd, Buffer &out) {
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return outErr(out, ERR_BAD_ARG, "expect score to be number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset)) {
        return outErr(out, ERR_BAD_ARG, "expect offset to be number");
    }
    if (!str2int(cmd[5], limit)) {
        return outErr(out, ERR_BAD_ARG, "expect limit to be number");
    }

    ZSet *zset = expectZset(cmd[1]);
    if (!zset) {
        return outErr(out, ERR_BAD_ARG, "expected zset");
    }
    
    if (limit <= 0) {
        return outArr(out, 0);
    }
    // search for key
    ZNode *znode = zsetSeekge(zset, score, name.data(), name.size());
    znode = znodeOffset(znode, offset);
    // iterate and output
    size_t ctx = outBeginArr(out);
    int64_t n = 0;
    while (znode && n < limit) {
        outStr(out, znode->name, znode->len);
        outDbl(out, znode->score);
        znode = znodeOffset(znode, +1);
        n += 2;
    }
    outEndArr(out, ctx, (uint32_t)n);
}

static void doZAdd(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    double score = 0;
    if (!str2dbl(cmd[2], score)) {
        return outErr(out, ERR_BAD_ARG, "expect score to be float");
    }
    // hashtable lookup
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    Entry *ent = NULL;
    if (node) {
        ent = container_of(node, Entry, node);
        if (ent->type != T_ZSET) {
            return outErr(out, ERR_BAD_ARG, "expected zset");
        }
    } else {
        ent = entryNew(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hmInsert(&gData.db, &ent->node);
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zsetInsert(&ent->zset, name.data(), name.size(), score);
    return outInt(out, (int64_t)added);
}

static void doZRem(std::vector<std::string> &cmd, Buffer &out) {
    ZSet *zset = expectZset(cmd[1]);
    if (!zset) {
        return outErr(out, ERR_BAD_ARG, "expected zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zsetLookup(zset, name.data(), name.size());
    if (znode) {
        zsetDelete(zset, znode);
    }
    return outInt(out, znode ? 1 : 0);
}

static void doZScore(std::vector<std::string> &cmd, Buffer &out) {
    ZSet *zset = expectZset(cmd[1]);
    if (!zset) {
        return outErr(out, ERR_BAD_ARG, "expected zset");
    }
    const std::string &name = cmd[2];
    ZNode *znode = zsetLookup(zset, name.data(), name.size());
    return znode ? outDbl(out, znode->score) : outNil(out);
}

static void doGet(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (!node) {
        return outErr(out, ERR_UNKNOWN, "key not found");
    }
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR) {
        return outErr(out, ERR_BAD_ARG, "expected string");
    
    }
    return outStr(out, ent->str.data(), ent->str.size());
}

static void doSet(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR) {
            return outErr(out, ERR_BAD_ARG, "expected string");
        }
        ent->str.swap(cmd[2]);
    } else {
        Entry *entry = entryNew(T_STR);
        entry->key.swap(key.key);
        entry->str.swap(cmd[2]);
        entry->node.hcode = key.node.hcode;
        hmInsert(&gData.db, &entry->node);
    }
    return outNil(out);
}

static void doDel(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmDelete(&gData.db, &key.node, &entryEq);
    if (node) {
        entryDel(container_of(node, Entry, node));
    } else {
        outErr(out, ERR_UNKNOWN, "key not found");
    }
    return outInt(out, node ? 1 : 0);
}

static bool cbKeys(HNode *node, void *arg) {
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    outStr(out, key.data(), key.size());
    return true;
}

static void doKeys(std::vector<std::string> &, Buffer &out) {
    outArr(out, (uint32_t)hmSize(&gData.db));
    hmForEach(&gData.db, &cbKeys, (void *) &out);
}

// PEXPIRE key ttl_ms
static void doExpire(std::vector<std::string> &cmd, Buffer &out) {
    int64_t ttlMs = 0;
    if (!str2int(cmd[2], ttlMs)) {
        return outErr(out, ERR_BAD_ARG, "expect ttl to be number");
    }
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (node) {
        Entry *ent = container_of(node, Entry, node);
        entrySetTTL(ent, ttlMs);
    }
    return outInt(out, node ? 1 : 0);
}

// PTTL key
static void doTtl(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (!node) {
        return outInt(out, -2);
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->heapIdx == (size_t)-1) {
        return outInt(out, -1);
    }
    uint64_t expireAt = gData.heap[ent->heapIdx].val;
    uint64_t nowMs = getMonotonicMs();
    return outInt(out, expireAt > nowMs ? (int64_t)(expireAt - nowMs) : 0);
}

static void responseBegin(Buffer &out, size_t *headerPos) {
    *headerPos = out.size();
    bufAppendU32(out, 0);
}

static size_t responseSize(Buffer &out, size_t header) {
    return out.size() - header - 4;
}

static void responseEnd(Buffer &out, size_t header) {
    size_t size = responseSize(out, header);
    if (size > k_max_msg) {
        out.resize(header + 4);
        outErr(out, ERR_TOO_BIG, "response is too big");
        size = responseSize(out, header);
    }
    // message header
    uint32_t len = (uint32_t)size;
    memcpy(&out[header], &len, 4);   
}


static void doRequest(std::vector<std::string> &cmd, Buffer &out) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        return doGet(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        return doSet(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        return doDel(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") { 
        return doKeys(cmd, out);
    } else if (cmd.size() == 4 && cmd[0] == "zadd") {
        return doZAdd(cmd, out);
    } else if (cmd.size() == 6 && cmd[0] == "zquery") {
        return doZQuery(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zscore") {
        return doZScore(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zrem") {
        return doZRem(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "pexpire") {
        return doExpire(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "pttl") {
        return doTtl(cmd, out);
    } else {
        return outErr(out, ERR_UNKNOWN, "unknown command");
    }
}

static bool tryOneRequest(Conn *conn) {
    // 3. Try to parse the accumulated buffer.
    // Protocol: message header
    if (conn->incoming.size() < 4) {
        return false;
    }
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) {
        conn->wantClose = true;
        return false;
    }
    // Protocol: message body
    if (conn->incoming.size() < 4 + len) {
        return false;
    }
    const uint8_t *request = &conn->incoming[4];
    std::vector<std::string> cmd;
    if (parseRequest(request, len, cmd) < 0) {
        msg("bad req");
        conn->wantClose = true;
        return false;
    }
    // 4. Process the parsed message
    // generate the response
    size_t headerPos = 0;
    responseBegin(conn->outgoing, &headerPos);
    doRequest(cmd, conn->outgoing);
    responseEnd(conn->outgoing, headerPos);

    // 5. Remove the message from conn->incoming.
    bufConsume(conn->incoming, 4 + len);
    return true;
}

static void handleWrite(Conn *conn) {
    assert(conn->outgoing.size() > 0);
    ssize_t rv = write(conn->fd, &conn->outgoing[0], conn->outgoing.size());
    if (rv < 0 && errno == EAGAIN) {
        return; 
    }
    if (rv < 0) {
        msg_errno("write() error");
        conn->wantClose = true;
        return;
    }
    // remove written data from conn->outgoing
    bufConsume(conn->outgoing, (size_t)rv);
    // update readiness intention
    if (conn->outgoing.size() == 0) {
        conn->wantWrite = false;
        conn->wantRead = true;
    }
}

static void handleRead(Conn *conn) {
    // 1. Do a non-blocking read
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN) {
        return; // actually not ready
    }
    // handle IO error
    if (rv < 0) {
        msg_errno("read() error");
        conn->wantClose = true;
        return; // want close
    }
    // handle EOF
    if (rv == 0) {
        if (conn->incoming.size() == 0) {
            msg("client closed");
        } else {
            msg("unexpected EOF");
        }
        conn->wantClose = true;
        return;
    }
    // 2. Add new data to the Conn->incoming buf
    bufAppend(conn->incoming, buf, (size_t)rv);
    // 3. Try to parse the accumulated buffer.
    while (tryOneRequest(conn)) {}
    // update readiness intention
    if (conn->outgoing.size() > 0) {
        conn->wantWrite = true;
        conn->wantRead = false;
        return handleWrite(conn);
    }
}

static int32_t nextTimerMs() {
    uint32_t nowMs = getMonotonicMs();
    // max val since this is unsigned
    uint64_t nextMs = (uint64_t)-1;
    // idle timers from clients
    if (!dlistEmpty(&gData.idleList)) {
        Conn *conn = container_of(gData.idleList.next, Conn, idleNode);
        nextMs = conn->lastActiveMs + k_idle_timeout_ms;
    }
    // TTL timers on DB
    if (!gData.heap.empty() && gData.heap[0].val <= nextMs) {
        nextMs = gData.heap[0].val;
    }
    // timeout value
    if (nextMs == (uint64_t)-1) {
        return -1;
    }
    if (nextMs <= nowMs) {
        return 0;
    }
    return (int32_t)(nextMs - nowMs);
}

static void processTimers() {
    uint64_t nowMs = getMonotonicMs();
    // idle timers from clients
    while (!dlistEmpty(&gData.idleList)) {
        Conn *conn = container_of(gData.idleList.next, Conn, idleNode);
        uint64_t nextMs = conn->lastActiveMs + k_idle_timeout_ms;
        if (nextMs >= nowMs) {
            break;
        }
        fprintf(stderr, "removing idle connection: %d\n", conn->fd);
        connDestroy(conn);
    }
    // TTL timers for DB entries
    const std::vector<HeapItem> &heap = gData.heap;
    const size_t kMaxWorks = 2000;
    size_t nworks = 0;
    while (!heap.empty() && heap[0].val <= nowMs && nworks++ < kMaxWorks) {
        Entry *ent = container_of(heap[0].ref, Entry, heapIdx);
        hmDelete(&gData.db, &ent->node, &entryEq);
        entryDel(ent);
    }
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }
    fd_set_nb(fd);
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }

    memset(&gData, 0, sizeof(gData));
    dlistInit(&gData.idleList);
    threadPoolInit(&gData.threadPool, 4);
    // event loop
    std::vector<struct pollfd> pollArgs;
    while (true) {
        // prepare args of poll()
        pollArgs.clear();
        // put listening sockets in first position
        struct pollfd pfd = {fd, POLLIN, 0};
        pollArgs.push_back(pfd);
        // the rest are connection sockets
        for (Conn *conn : gData.fd2conn) {
            if (!conn) {
                continue;
            }
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // poll() flags from latest intent
            if (conn->wantRead) {
                pfd.events |= POLLIN;
            }
            if (conn->wantWrite) {
                pfd.events |= POLLOUT;
            }
            pollArgs.push_back(pfd);
        }
        int32_t timeoutMs = nextTimerMs();
        // poll() the client conns
        int rv = poll(pollArgs.data(), (nfds_t)pollArgs.size(), timeoutMs);
        if (rv < 0 && errno == EINTR) {
            continue;
        } else if (rv < 0) {
            die("poll()");
        }

        // handle the listening socket
        if (pollArgs[0].revents & POLLIN) {
            if (Conn *conn = handleAccept(fd)) {
                if (gData.fd2conn.size() <= (size_t)conn->fd) {
                    gData.fd2conn.resize(conn->fd + 1);
                }
                assert(!gData.fd2conn[conn->fd]);
                gData.fd2conn[conn->fd] = conn;
            }
        }

        // handle connection sockets
        for (size_t i = 1; i < pollArgs.size(); ++i) {
            uint32_t ready = pollArgs[i].revents;
            if (ready == 0) {
                continue;
            }

            Conn *conn = gData.fd2conn[pollArgs[i].fd];
            conn->lastActiveMs = getMonotonicMs();
            dlistDetach(&conn->idleNode);
            dlistInsertBefore(&gData.idleList, &conn->idleNode);
            if (ready & POLLIN) {
                assert(conn->wantRead);
                handleRead(conn);
            }
            if (ready & POLLOUT) {
                assert(conn->wantWrite);
                handleWrite(conn);
            }

            // close sockets from error or app logic
            if ((ready & POLLERR) || conn->wantClose) {
                connDestroy(conn);
            }
        }
        processTimers();
    }
    return 0;
}