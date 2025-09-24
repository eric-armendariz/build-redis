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
#include <string>
// C++
#include <vector>
#include <map>

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
static void bufAppend(std::vector<uint8_t> &buf, const uint8_t *data, size_t len) {
    if (len == 0) {
        return;
    }
    assert(data != nullptr);
    buf.insert(buf.end(), data, data + len);
}
// remove from the front
static void bufConsume(std::vector<uint8_t> &buf, size_t n) {
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
    std::vector<uint8_t> incoming;
    std::vector<uint8_t> outgoing;
};

enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2,
};

struct Response {
    uint32_t status = 0;
    std::vector<uint8_t> data;
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

static std::map<std::string, std::string> gData;

static void doRequest(std::vector<std::string> &cmd, Response &resp) {
    if (cmd.size() == 2 && cmd[0] == "get") {
        auto it = gData.find(cmd[1]);
        if (it == gData.end()) {
            resp.status = RES_NX;
            return;
        }
        const std::string &val = it->second;
        resp.data.assign(val.begin(), val.end());
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        gData[cmd[1]].swap(cmd[2]);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        gData.erase(cmd[1]);
    } else {
        resp.status = RES_ERR;
    }
}

void makeResponse(std::vector<std::uint8_t> &buf, Response &resp) {
    uint32_t respLen = 4 + (uint32_t) resp.data.size();
    bufAppend(buf, (const uint8_t *)&respLen, 4);
    bufAppend(buf, (const uint8_t *)&resp.status, 4);
    bufAppend(buf, resp.data.data(), resp.data.size());
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
    Response resp;
    doRequest(cmd, resp);
    makeResponse(conn->outgoing, resp);

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