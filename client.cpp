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

static int32_t query(int fd, const char* text) {
    if (strlen(text) > max_message_size) {
        msg("too long");
        return -1;
    }

    // Write request to server.
    uint32_t len = (uint32_t)strlen(text);
    char wbuf[4 + len];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], text, len);
    if (int32_t err = write_all(fd, wbuf, 4 + len)) {
        msg("write_all()");
        return err;
    }

    // Read response.
    char rbuf[4 + max_message_size];
    if (int32_t err = read_full(fd, rbuf, 4)) {
        msg("read_full()");
        return err;
    }
    len = 0;
    if (len > max_message_size) {
        msg("too long");
        return -1;
    }
    memcpy(&len, rbuf, 4);
    if (int32_t err = read_full(fd, &rbuf[4], len)) {
        msg("read_full()");
        return err;
    }
    printf("server says: %.*s\n", len, &rbuf[4]);
    return 0;
}

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv < 0) {
        die("connect()");
    }

    char msg[] = "hello2";
    int32_t resp = query(fd, msg);
    query(fd, "sup\n gang\n alyssa and eric forever!!!");
    close(fd);
    return 0;
}