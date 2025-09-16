#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <cassert>

const size_t max_message_size = 4096;

void die(const char* msg) {
    perror(msg);
    exit(1);
}

void msg(const char* message) {
    fprintf(stderr, "%s\n", message);
}

static int32_t read_full(int fd, char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error, or unexpected EOF
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0) {
            return -1;  // error
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t handleRequest(int fd) {
    // Process request header.
    char rbuf[max_message_size + 4];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err) {
        return err;
    }
    uint32_t len = 0;
    memcpy(&len, rbuf, 4);
    if (len > max_message_size) {
        msg("too long");
        return -1;
    }

    // Process request body.
    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read_full()");
        return err;
    }
    printf("client says: %.*s\n", len, &rbuf[4]);

    // Reply with same protocol.
    const char reply[] = "world";
    char wbuf[4 + sizeof(reply)];
    len = (uint32_t)strlen(reply);
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], reply, len);
    return write_all(fd, wbuf, 4 + len);
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);
    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("bind()");
    }

    rv = listen(fd, SOMAXCONN);
    if (rv) {
        die("listen()");
    }
    while (true) {
        struct sockaddr_in client_addr = {};
        socklen_t addrlen = sizeof(client_addr);
        int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
        if (connfd < 0) {
            continue;
        }

        while (true) {
            int32_t err = handleRequest(connfd);
            if (err) {
                break;
            }
        }
        close(connfd);
    }
    return 0;
}