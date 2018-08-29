//
// Created by Yi Lu on 7/24/18.
//

#pragma once
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace scar {

class Socket {

public:
  Socket() {
    fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
  }

  Socket(int fd) : fd(fd) {}

  int connect(const char *addr, int port) {
    sockaddr_in serv = make_endpoint(addr, port);
    return ::connect(fd, (const sockaddr *)(&serv), sizeof(serv));
  }

  void disable_nagle_algorithm() {
    // disable Nagle's algorithm
    int flag = 1;
    int res = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
    CHECK(res >= 0);
  }

  int close() { return ::close(fd); }

  long read_n_bytes(char *buf, long size) {
    long n = 0;
    while (n < size) {
      long bytes_read = read(buf + n, size - n);
      if (bytes_read == 0) {
        CHECK(n == 0); // no partial reading is support
        return 0;      // remote socket is closed.
      }
      n += bytes_read;
    }
    return n;
  }

  long read_n_bytes_async(char *buf, long size) {
    long n = 0;
    while (n < size) {
      long bytes_read = read_async(buf + n, size - n);
      if (bytes_read == -1) { // non blocking
        CHECK(errno == EWOULDBLOCK || errno == EAGAIN);
        if (n == 0)
          return -1;
        else
          continue;
      }
      if (bytes_read == 0) {
        CHECK(n == 0); // no partial reading is support
        return 0;      // remote socket is closed.
      }
      n += bytes_read;
    }
    return n;
  }

  long write_n_bytes(const char *buf, long size) {
    long n = 0;
    while (n < size) {
      long bytes_written = write(buf + n, size - n);
      n += bytes_written;
    }
    return n;
  }

  template <class T> long write_number(const T &n) {
    return write_n_bytes(reinterpret_cast<const char *>(&n), sizeof(T));
  }

  template <class T> long read_number(T &n) {
    return read_n_bytes(reinterpret_cast<char *>(&n), sizeof(T));
  }

  template <class T> long read_number_async(T &n) {
    return read_n_bytes_async(reinterpret_cast<char *>(&n), sizeof(T));
  }

  long read(char *buf, long size) {
    if (size > 0) {
      return recv(fd, buf, size, 0);
    }
    return 0;
  }

  long read_async(char *buf, long size) {
    if (size > 0) {
      return recv(fd, buf, size, MSG_DONTWAIT);
    }
    return 0;
  }

  long write(const char *buf, long size) {
    if (size > 0) {
      return send(fd, buf, size, 0);
    }
    return 0;
  }

  static sockaddr_in make_endpoint(const char *addr, int port) {
    sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));

    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr(addr);
    serv.sin_port = htons(port); // convert to big-endian order
    return serv;
  }

private:
  int fd;
};

class Listener {
public:
  Listener(const char *addr, int port, int max_connections) {
    fd = socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    bind(addr, port);
    listen(max_connections);
  }

  Socket accept() {
    int acc_fd = ::accept(fd, 0, 0);
    CHECK(acc_fd >= 0);
    return Socket(acc_fd);
  }

  int close() { return ::close(fd); }

private:
  void bind(const char *addr, int port) {
    sockaddr_in serv = Socket::make_endpoint(addr, port);
    int ret = ::bind(fd, (sockaddr *)(&serv), sizeof(serv));
    CHECK(ret >= 0);
  }

  void listen(int max_connections) { ::listen(fd, max_connections); }

  int fd;
};
} // namespace scar