// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
// system
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <string>
#include <vector>
// proj
#include "hashtable.hpp"
#include <iostream>

#define container_of(ptr, T, member) \
    ((T *)( (char *)ptr - offsetof(T, member) ))

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

struct Conn {
    int fd = -1;
    // state of conn for the event loop
    bool wantRead = false;
    bool wantWrite = false;
    bool wantClose = false;
    // buffers containing I/O of conn
    Buffer incoming;
    Buffer outgoing;
};

enum {
    ERR_UNKNOWN = 1,
    ERR_TOO_BIG = 2,
};

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
    return conn;
}

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

static struct {
    HMap db;
} gData;

struct Entry {
    struct HNode node;
    std::string key;
    std::string val;
};

static bool entryEq(HNode *a, HNode *b) {
    struct Entry *left = container_of(a, struct Entry, node);
    struct Entry *right = container_of(b, struct Entry, node);
    return left->key == right->key;
}

// FNV hash
static uint64_t strHash(const uint8_t *data, size_t len) {
    uint32_t h = 0x811C9DC5;
    for (size_t i = 0; i < len; i++) {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static void doGet(std::vector<std::string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (!node) {
        return outErr(out, ERR_UNKNOWN, "key not found");
    }
    const std::string &val = container_of(node, Entry, node)->val;
    return outStr(out, val.data(), val.size());
}

static void doSet(std::vector<std::string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    // hashtable lookup
    HNode *node = hmLookup(&gData.db, &key.node, &entryEq);
    if (node) {
        container_of(node, Entry, node)->val.swap(cmd[2]);
    } else {
        Entry *entry = new Entry();
        entry->key.swap(key.key);
        entry->val.swap(cmd[2]);
        entry->node.hcode = key.node.hcode;
        hmInsert(&gData.db, &entry->node);
    }
    return outNil(out);
}

static void doDel(std::vector<std::string> &cmd, Buffer &out) {
    Entry key;
    key.key.swap(cmd[1]);
    key.node.hcode = strHash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hmDelete(&gData.db, &key.node, &entryEq);
    if (node) {
        delete container_of(node, Entry, node);
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
        doGet(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        doSet(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        doDel(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") { 
        doKeys(cmd, out);
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
    // a map of all client connections
    std::vector<Conn *> fd2conn;
    // event loop
    std::vector<struct pollfd> pollArgs;
    while (true) {
        // prepare args of poll()
        pollArgs.clear();
        // put listening sockets in first position
        struct pollfd pfd = {fd, POLLIN, 0};
        pollArgs.push_back(pfd);
        // the rest are connection sockets
        for (Conn *conn : fd2conn) {
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
        // poll() the client conns
        int rv = poll(pollArgs.data(), (nfds_t)pollArgs.size(), -1);
        if (rv < 0 && errno == EINTR) {
            continue;
        } else if (rv < 0) {
            die("poll()");
        }

        // handle the listening socket
        if (pollArgs[0].revents & POLLIN) {
            if (Conn *conn = handleAccept(fd)) {
                if (fd2conn.size() <= (size_t)conn->fd) {
                    fd2conn.resize(conn->fd + 1);
                }
                assert(!fd2conn[conn->fd]);
                fd2conn[conn->fd] = conn;
            }
        }

        // handle connection sockets
        for (size_t i = 1; i < pollArgs.size(); ++i) {
            uint32_t ready = pollArgs[i].revents;
            if (ready == 0) {
                continue;
            }

            Conn *conn = fd2conn[pollArgs[i].fd];
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
                (void)close(conn->fd);
                fd2conn[conn->fd] = NULL;
                delete conn;
            }
        }
    }
    return 0;
}