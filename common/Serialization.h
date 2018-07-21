//
// Created by Yi Lu on 7/17/18.
//

#ifndef SCAR_SERIALIZATION_H
#define SCAR_SERIALIZATION_H

#include <string>
#include <cstring>
#include <folly/String.h>

namespace scar {
    template<class T>
    class Serializer {
    public:
        std::string operator()(const T &v) {
            std::string result(sizeof(T), 0);
            memcpy(&result[0], &v, sizeof(T));
            return result;
        }
    };

    template<class T>
    class Deserializer {
    public:
        T operator()(folly::StringPiece str, std::size_t &size) const {
            T result;
            size = sizeof(T);
            memcpy(&result, const_cast<char *>(&str[0]), size);
            return result;
        }
    };

    template<>
    class Serializer<std::string> {
    public:
        std::string operator()(const std::string &v) {
            return Serializer<std::string::size_type>()(v.size()) + v;
        }
    };

    template<>
    class Deserializer<std::string> {
    public:
        std::string operator()(folly::StringPiece str, std::size_t &size) const {
            std::string::size_type len = Deserializer<std::string::size_type>()(str, size);
            str.advance(sizeof(len));
            size += len;
            return std::string(str.begin(), str.begin() + len);
        }
    };

}

#endif //SCAR_SERIALIZATION_H
