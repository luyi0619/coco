//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "common/ClassOf.h"
#include "common/FixedString.h"
#include "common/Hash.h"
#include "common/Serialization.h"
#include "core/SchemaDef.h"

namespace scar {
namespace retwis {
static constexpr auto __BASE_COUNTER__ = __COUNTER__ + 1;
static constexpr auto RETWIS_SIZE = 400;
} // namespace retwis
} // namespace scar

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) x(scar) x(retwis)

#define RETWIS_KEY_FIELDS(x, y) x(int32_t, KEY)
#define RETWIS_VALUE_FIELDS(x, y) x(FixedString<RETWIS_SIZE>, VALUE)

DO_STRUCT(retwis, RETWIS_KEY_FIELDS, RETWIS_VALUE_FIELDS, NAMESPACE_FIELDS)

namespace scar {

template <> class Serializer<retwis::retwis::value> {
public:
  std::string operator()(const retwis::retwis::value &v) {
    return Serializer<decltype(v.VALUE)>()(v.VALUE);
  }
};

template <> class Deserializer<retwis::retwis::value> {
public:
  std::size_t operator()(StringPiece str, retwis::retwis::value &result) const {

    std::size_t sz = Deserializer<decltype(result.VALUE)>()(str, result.VALUE);
    str.remove_prefix(sz);
    return sz;
  }
};

template <> class ClassOf<retwis::retwis::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(retwis::retwis::value::VALUE)>::size();
  }
};

} // namespace scar