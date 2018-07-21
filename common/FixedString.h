//
// Created by Yi Lu on 7/13/18.
//

#ifndef SCAR_FIXEDSTRING_H
#define SCAR_FIXEDSTRING_H

#include <folly/FixedString.h>
#include <folly/String.h>
#include "Hash.h"
#include "Serialization.h"

namespace scar {

    template<std::size_t N>
    class FixedString : public folly::FixedString<N> {
    public:
        using folly::FixedString<N>::FixedString;

        std::size_t hash_code() const {
            std::hash<char> h;
            std::size_t hashCode = 0;
            for (auto i = 0u; i < this->size(); i++) {
                hashCode = scar::hash_combine(hashCode, h((*this)[i]));
            }
            return hashCode;
        }

        FixedString &assignStdString(const std::string str) {
            folly::FixedString<N>::assign(str.c_str(), str.size());
            return *this;
        }
    };


    template<class C, std::size_t N>
    inline std::basic_ostream<C> &operator<<(
            std::basic_ostream<C> &os,
            const FixedString<N> &string) {
        os << static_cast<folly::FixedString<N>>(string);
        return os;
    }

    template<std::size_t N>
    class Serializer<FixedString<N>> {
    public:
        std::string operator()(const FixedString<N> &v) {
            return Serializer<std::string::size_type>()(v.size()) + v.toStdString();
        }
    };

    template<std::size_t N>
    class Deserializer<FixedString<N>> {
    public:
        FixedString<N> operator()(folly::StringPiece str, std::size_t &size) const {
            std::string::size_type len = Deserializer<typename FixedString<N>::size_type>()(str, size);
            str.advance(sizeof(len));
            size += len;
            FixedString<N> result;
            result.assign(str.data(), len);
            return result;
        }
    };

}

namespace std {
    template<unsigned int N>
    struct hash<scar::FixedString<N>> {
        std::size_t operator()(const scar::FixedString<N> &k) const {
            return k.hash_code();
        }
    };
}

#endif //SCAR_FIXEDSTRING_H
