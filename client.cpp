#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>

// Note: much of the client code copied from: https://build-your-own.org/redis/#table-of-contents

static void msg(const char *msg) {
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
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

const size_t k_max_msg = 4096; 

// the `query` function was simply splited into `send_req` and `read_res`.
static int32_t send_req(int fd, const std::vector<std::string> &cmd) {
    uint32_t len = 4;
    for (const std::string &s : cmd) {
        len += 4 + (uint32_t)s.size();
    }
    if (len > k_max_msg) {
        return -1;
    }

    char wbuf[4 + k_max_msg];
    memcpy(&wbuf[0], &len, 4);
    uint32_t cmdSize = (uint32_t)cmd.size();
    memcpy(&wbuf[4], &cmdSize, 4);
    size_t offset = 8;
    for (const std::string &s : cmd) {
        uint32_t sSize = (uint32_t)s.size();
        memcpy(&wbuf[offset], &sSize, 4);
        offset += 4;
        memcpy(&wbuf[offset], s.data(), s.size());
        offset += s.size();
    }
    return write_all(fd, wbuf, len + 4);
}

enum {
    TAG_NIL = 0,
    TAG_ERR = 1,
    TAG_INT = 2,
    TAG_STR = 3,
    TAG_DBL = 4,
    TAG_ARR = 5,
};
static int32_t printResponse(const uint8_t *data, size_t size) {
    if (size < 1) {
        msg("bad response");
        return -1;
    }
    switch (data[0]) {
        case TAG_NIL:
            printf("(nil)\n");
            return 1;
            
        case TAG_ERR: {
            if (size < 9) {
                msg("bad response");
                return -1;
            }
            uint32_t code = 0;
            memcpy(&code, &data[1], 4);
            uint32_t len = 0;
            memcpy(&len, &data[5], 4);
            if (size < 9 + len) {
                msg("bad response");
                return -1;
            }
            printf("(err) %d %.*s\n", code, len, &data[1 + 8]);
            return 1 + 8 + len;
        }
        
        case TAG_STR: {
            if (size < 5) {
                msg("bad response");
                return -1;
            }
            uint32_t len = 0;
            memcpy(&len, &data[1], 4);
            if (size < 5 + len) {
                msg("bad response");
                return -1;
            }
            printf("(str) %.*s\n", len, &data[5]);
            return 5 + len;
        }
        
        case TAG_INT: {
            if (size < 9) {
                msg("bad response");
                return -1;
            }
            int64_t val = 0;
            memcpy(&val, &data[1], 8);
            printf("(int) %ld\n", val);
            return 9;
        }
        
        case TAG_DBL: {
            if (size < 9) {
                msg("bad response");
                return -1;
            }
            double dbl = 0;
            memcpy(&dbl, &data[1], 8);
            printf("(dbl) %g\n", dbl);
            return 9;
        }
        
        case TAG_ARR: {
            if (size < 5) {
                msg("bad response");
                return -1;
            }
            uint32_t len = 0; 
            memcpy(&len, &data[1], 4);
            printf("(arr) len=%u\n", len);
            size_t arr_bytes = 1 + 4;
            for (uint32_t i = 0; i < len; ++i) {
                int32_t rv = printResponse(&data[arr_bytes], size - arr_bytes);
                if (rv < 0) {
                    return rv;
                }
                arr_bytes += (size_t)rv;
            }
            printf("(arr) end\n");
            return (int32_t)arr_bytes;
        }
        
        default:
            msg("bad response");
            return -1;
    }
}

static int32_t read_res(int fd) {
    // 4 bytes header
    char rbuf[4 + k_max_msg];
    errno = 0;
    int32_t err = read_full(fd, rbuf, 4);
    if (err) {
        if (errno == 0) {
            msg("EOF");
        } else {
            msg("read() error");
        }
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, rbuf, 4);  // assume little endian
    if (len > k_max_msg) {
        msg("too long");
        return -1;
    }

    // reply body
    err = read_full(fd, &rbuf[4], len);
    if (err) {
        msg("read() error");
        return err;
    }

    // print the result
    int32_t rv = printResponse((uint8_t *)&rbuf[4], len);
    if (rv < 0) {
        msg("bad response");
        rv = -1;
    }
    return rv;
}

int main(int argc, char **argv) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);  // 127.0.0.1
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect");
    }

    std::vector<std::string> cmd;
    for (int i = 1; i < argc; ++i) {
        cmd.push_back(argv[i]);
    }
    int32_t err = send_req(fd, cmd);
    if (err) {
        goto L_DONE;
    }
    err = read_res(fd);
    if (err) {
        goto L_DONE;
    }

L_DONE:
    close(fd);
    return 0;
}