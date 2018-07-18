//
// Created by Yi Lu on 7/17/18.
//

#ifndef SCAR_ENCODER_H
#define SCAR_ENCODER_H

#include <string>
#include <folly/String.h>
#include "Serialization.h"

#include <iostream>

namespace scar {
    class Encoder {
    public:
        template<class T>
        friend Encoder &operator<<(Encoder &enc, const T &rhs);

        folly::StringPiece toStringPiece() {
            return folly::StringPiece(bytes.data(), bytes.data() + bytes.size());
        }

    private:
        std::string bytes;
    };

    template<class T>
    Encoder &operator<<(Encoder &enc, const T &rhs) {
        Serializer<T> serializer;
        enc.bytes += serializer(rhs);
        return enc;
    }

    class Decoder {
    public:
        Decoder(folly::StringPiece bytes) : bytes(bytes) {}

        template<class T>
        friend Decoder &operator>>(Decoder &dec, T &rhs);

    private:
        folly::StringPiece bytes;
    };

    template<class T>
    Decoder &operator>>(Decoder &dec, T &rhs) {
        Deserializer<T> deserializer;
        std::size_t size;
        rhs = deserializer(dec.bytes, size);
        dec.bytes.advance(size);
        return dec;
    }
};

#endif //SCAR_ENCODER_H
