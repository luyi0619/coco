//
// Created by Yi Lu on 7/17/18.
//

#ifndef SCAR_SERIALIZATION_H
#define SCAR_SERIALIZATION_H

#include <string>
#include <cstring>

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
        T operator()(const std::string &str) const {
            T result;
            memcpy(&result, const_cast<char *>(&str[0]), str.size());
            return result;
        }
    };


    template<>
    class Serializer<std::string> {
    public:
        std::string operator()(const std::string &v) {
            return v;
        }
    };

    template<>
    class Deserializer<std::string> {
    public:
        std::string operator()(const std::string &str) const {
            return str;
        }
    };

}

#endif //SCAR_SERIALIZATION_H
