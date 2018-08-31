//
// Created by Yi Lu on 7/17/18.
//

#pragma once

#include <iostream>
#include <string>

#include "Serialization.h"
#include "StringPiece.h"

namespace scar {
class Encoder {
public:
  Encoder(std::string &bytes) : bytes(bytes) {}

  template <class T> friend Encoder &operator<<(Encoder &enc, const T &rhs);

  StringPiece toStringPiece() {
    return StringPiece(bytes.data(), bytes.size());
  }

  void write_n_bytes(const void *ptr, std::size_t size) {
    bytes.append(static_cast<const char *>(ptr), size);
  }

private:
  std::string &bytes;
};

template <class T> Encoder &operator<<(Encoder &enc, const T &rhs) {
  Serializer<T> serializer;
  enc.bytes += serializer(rhs);
  return enc;
}

class Decoder {
public:
  Decoder(StringPiece bytes) : bytes(bytes) {}

  template <class T> friend Decoder &operator>>(Decoder &dec, T &rhs);

  void read_n_bytes(void *ptr, std::size_t size) {
    CHECK(bytes.size() >= size);
    std::memcpy(ptr, bytes.data(), size);
    bytes.remove_prefix(size);
  }

private:
  StringPiece bytes;
};

template <class T> Decoder &operator>>(Decoder &dec, T &rhs) {
  Deserializer<T> deserializer;
  std::size_t size;
  rhs = deserializer(dec.bytes, size);
  dec.bytes.remove_prefix(size);
  return dec;
}
} // namespace scar
