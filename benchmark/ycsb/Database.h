//
// Created by Yi Lu on 7/15/18.
//

#ifndef SCAR_YCSB_DATABASE_H
#define SCAR_YCSB_DATABASE_H

#include "common/FixedString.h"
#include "database/SchemaDef.h"

namespace scar {
    namespace ycsb {

        constexpr auto YCSB_FIELD_SIZE = 10;

#define YCSB_KEY_FIELDS(x, y) \
    x(int32_t, Y_KEY)
#define YCSB_VALUE_FIELDS(x, y) \
    x(FixedString<YCSB_FIELD_SIZE>, Y_F01) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F02) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F03) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F04) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F05) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F06) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F07) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F08) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F09) \
    y(FixedString<YCSB_FIELD_SIZE>, Y_F10)

        DO_STRUCT(ycsb, YCSB_KEY_FIELDS, YCSB_VALUE_FIELDS)

    }
}

#endif //SCAR_YCSB_DATABASE_H
