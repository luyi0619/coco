//
// Created by Yi Lu on 7/13/18.
//

#pragma once

#include <array>
#include <iostream>
#include <string>

#include "Hash.h"
#include "Serialization.h"
#include "StringPiece.h"

namespace scar {

template <std::size_t N> class FixedString {
public:
  static_assert(N > 0, "string length should be positive.");

  using size_type = std::size_t;

  FixedString() { assign(""); }

  FixedString(const char *str) { assign(std::string(str)); }

  FixedString(const std::string &str) { assign(str); }

  int compare(const FixedString &that) const {
    size_type minSize = length_ < that.length_ ? length_ : that.length_;
    int r = 0;
    for (auto i = 0u; i < minSize; i++) {
      if (data_[i] < that.data_[i]) {
        r = -1;
        break;
      }
      if (data_[i] > that.data_[i]) {
        r = 1;
        break;
      }
    }
    if (r < 0)
      return -1;
    if (r > 0)
      return 1;
    if (length_ < that.length_)
      return -1;
    if (length_ > that.length_)
      return 1;
    return 0;
  }

  bool operator<(const FixedString &that) const { return compare(that) < 0; }

  bool operator<=(const FixedString &that) const { return compare(that) <= 0; }

  bool operator>(const FixedString &that) const { return compare(that) > 0; }

  bool operator>=(const FixedString &that) const { return compare(that) >= 0; }

  bool operator==(const FixedString &that) const { return compare(that) == 0; }

  bool operator!=(const FixedString &that) const { return compare(that) != 0; }

  FixedString &assign(const std::string &str) {
    return assign(str, str.length());
  }

  FixedString &assign(const std::string &str, size_type length) {
    CHECK(length <= str.length());
    CHECK(length <= N);
    std::copy(str.begin(), str.end(), data_.begin());
    length_ = length;
    data_[length_] = 0;
    return *this;
  }

  const char *c_str() { return &data_[0]; }

  std::size_t hash_code() const {
    std::hash<char> h;
    std::size_t hashCode = 0;
    for (auto i = 0u; i < length_; i++) {
      hashCode = scar::hash_combine(hashCode, h(data_[i]));
    }
    return hashCode;
  }

  size_type length() const { return length_; }

  size_type size() const { return length_; }

  std::string toString() const {
    std::string str;
    // the last char is \0
    std::copy(data_.begin(), data_.end() - 1, std::back_inserter(str));
    return str;
  }

private:
  std::array<char, N + 1> data_;
  size_type length_;
};

template <class C, std::size_t N>
inline std::basic_ostream<C> &operator<<(std::basic_ostream<C> &os,
                                         const FixedString<N> &str) {
  os << str.toString();
  return os;
}

template <std::size_t N> class Serializer<FixedString<N>> {
public:
  std::string operator()(const FixedString<N> &v) {
    return Serializer<std::string::size_type>()(v.size()) + v.toString();
  }
};

template <std::size_t N> class Deserializer<FixedString<N>> {
public:
  FixedString<N> operator()(StringPiece str, std::size_t &size) const {
    std::string::size_type len =
        Deserializer<typename FixedString<N>::size_type>()(str, size);
    str.remove_prefix(sizeof(len));
    size += len;
    FixedString<N> result;
    result.assign(str.data(), len);
    return result;
  }
};

} // namespace scar

namespace std {
template <std::size_t N> struct hash<scar::FixedString<N>> {
  std::size_t operator()(const scar::FixedString<N> &k) const {
    return k.hash_code();
  }
};
} // namespace std
