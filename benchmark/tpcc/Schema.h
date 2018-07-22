//
// Created by Yi Lu on 7/15/18.
//

#ifndef SCAR_TPCC_SCHEMA_H
#define SCAR_TPCC_SCHEMA_H

#include "common/FixedString.h"
#include "common/Hash.h"
#include "core/SchemaDef.h"


// table definition for ycsb
namespace scar {
    namespace tpcc {
        static constexpr std::size_t __BASE_COUNTER__ = __COUNTER__ + 1;
    }
}

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) \
    x(scar) \
    x(tpcc)


#define WAREHOUSE_KEY_FIELDS(x, y) \
    x(int32_t,W_ID)
#define WAREHOUSE_VALUE_FIELDS(x, y) \
    x(FixedString<10>,W_NAME) \
    y(FixedString<20>,W_STREET_1) \
    y(FixedString<20>,W_STREET_2) \
    y(FixedString<20>,W_CITY) \
    y(FixedString<2>,W_STATE) \
    y(FixedString<9>,W_ZIP) \
    y(float,W_TAX) \
    y(float,W_YTD)

DO_STRUCT(warehouse, WAREHOUSE_KEY_FIELDS, WAREHOUSE_VALUE_FIELDS, NAMESPACE_FIELDS)


#define DISTRICT_KEY_FIELDS(x, y) \
    x(int32_t,D_W_ID)  \
    y(int32_t,D_ID)
#define DISTRICT_VALUE_FIELDS(x, y) \
    x(FixedString<10>,D_NAME) \
    y(FixedString<20>,D_STREET_1) \
    y(FixedString<20>,D_STREET_2) \
    y(FixedString<20>,D_CITY) \
    y(FixedString<2>,D_STATE) \
    y(FixedString<9>,D_ZIP) \
    y(float,D_TAX) \
    y(float,D_YTD) \
    y(int32_t,D_NEXT_O_ID)

DO_STRUCT(district, DISTRICT_KEY_FIELDS, DISTRICT_VALUE_FIELDS, NAMESPACE_FIELDS)

#define CUSTOMER_KEY_FIELDS(x, y) \
    x(int32_t,C_W_ID)  \
    y(int32_t,C_D_ID)  \
    y(int32_t,C_ID)
#define CUSTOMER_VALUE_FIELDS(x, y) \
    x(FixedString<16>,C_FIRST) \
    y(FixedString<2>,C_MIDDLE) \
    y(FixedString<16>,C_LAST) \
    y(FixedString<20>,C_STREET_1) \
    y(FixedString<20>,C_STREET_2) \
    y(FixedString<20>,C_CITY) \
    y(FixedString<2>,C_STATE) \
    y(FixedString<9>,C_ZIP) \
    y(FixedString<16>,C_PHONE) \
    y(uint64_t,C_SINCE) \
    y(FixedString<2>,C_CREDIT) \
    y(float,C_CREDIT_LIM) \
    y(float,C_DISCOUNT) \
    y(float,C_BALANCE) \
    y(float,C_YTD_PAYMENT) \
    y(int32_t,C_PAYMENT_CNT) \
    y(int32_t,C_DELIVERY_CNT) \
    y(FixedString<500>,C_DATA)

DO_STRUCT(customer, CUSTOMER_KEY_FIELDS, CUSTOMER_VALUE_FIELDS, NAMESPACE_FIELDS)


#define CUSTOMER_NAME_IDX_KEY_FIELDS(x, y) \
    x(int32_t,C_W_ID) \
    y(int32_t,C_D_ID) \
    y(FixedString<16>,C_LAST)
#define CUSTOMER_NAME_IDX_VALUE_FIELDS(x, y) \
    x(int32_t,C_ID)

DO_STRUCT(customer_name_idx, CUSTOMER_NAME_IDX_KEY_FIELDS, CUSTOMER_NAME_IDX_VALUE_FIELDS, NAMESPACE_FIELDS)


#define HISTORY_KEY_FIELDS(x, y) \
    x(int32_t,H_W_ID)  \
    y(int32_t,H_D_ID)  \
    y(int32_t,H_C_W_ID)  \
    y(int32_t,H_C_D_ID)  \
    y(int32_t,H_C_ID)  \
    y(uint64_t,H_DATE)
#define HISTORY_VALUE_FIELDS(x, y) \
    x(float,H_AMOUNT) \
    y(FixedString<24>,H_DATA)

DO_STRUCT(history, HISTORY_KEY_FIELDS, HISTORY_VALUE_FIELDS, NAMESPACE_FIELDS)

#define NEW_ORDER_KEY_FIELDS(x, y) \
    x(int32_t,NO_W_ID)  \
    y(int32_t,NO_D_ID)  \
    y(int32_t,NO_O_ID)
#define NEW_ORDER_VALUE_FIELDS(x, y) \
    x(int32_t,NO_DUMMY)

DO_STRUCT(new_order, NEW_ORDER_KEY_FIELDS, NEW_ORDER_VALUE_FIELDS, NAMESPACE_FIELDS)

#define ORDER_KEY_FIELDS(x, y) \
    x(int32_t,O_W_ID)  \
    y(int32_t,O_D_ID)  \
    y(int32_t,O_ID)
#define ORDER_VALUE_FIELDS(x, y) \
    x(float,O_C_ID) \
    y(uint64_t,O_ENTRY_D) \
    y(int32_t,O_CARRIER_ID) \
    y(int8_t,O_OL_CNT) \
    y(bool,O_ALL_LOCAL)

DO_STRUCT(order, ORDER_KEY_FIELDS, ORDER_VALUE_FIELDS, NAMESPACE_FIELDS)


#define ORDER_LINE_KEY_FIELDS(x, y) \
    x(int32_t,OL_W_ID)  \
    y(int32_t,OL_D_ID)  \
    y(int32_t,OL_O_ID)  \
    y(int8_t,OL_NUMBER)
#define ORDER_LINE_VALUE_FIELDS(x, y) \
    x(int32_t,OL_I_ID) \
    y(int32_t,OL_SUPPLY_W_ID) \
    y(uint64_t,OL_DELIVERY_D) \
    y(int8_t,OL_QUANTITY) \
    y(float,OL_AMOUNT) \
    y(FixedString<24>,OL_DIST_INFO)

DO_STRUCT(order_line, ORDER_LINE_KEY_FIELDS, ORDER_LINE_VALUE_FIELDS, NAMESPACE_FIELDS)


#define ITEM_KEY_FIELDS(x, y) \
    x(int32_t,I_ID)
#define ITEM_VALUE_FIELDS(x, y) \
    x(int32_t,I_IM_ID) \
    y(FixedString<24>,I_NAME) \
    y(float,I_PRICE) \
    y(FixedString<50>,I_DATA)

DO_STRUCT(item, ITEM_KEY_FIELDS, ITEM_VALUE_FIELDS, NAMESPACE_FIELDS)


#define STOCK_KEY_FIELDS(x, y) \
    x(int32_t,S_W_ID) \
    y(int32_t,S_I_ID)
#define STOCK_VALUE_FIELDS(x, y) \
    x(int16_t,S_QUANTITY) \
    y(FixedString<24>,S_DIST_01) \
    y(FixedString<24>,S_DIST_02) \
    y(FixedString<24>,S_DIST_03) \
    y(FixedString<24>,S_DIST_04) \
    y(FixedString<24>,S_DIST_05) \
    y(FixedString<24>,S_DIST_06) \
    y(FixedString<24>,S_DIST_07) \
    y(FixedString<24>,S_DIST_08) \
    y(FixedString<24>,S_DIST_09) \
    y(FixedString<24>,S_DIST_10) \
    y(float,S_YTD) \
    y(int32_t,S_ORDER_CNT) \
    y(int32_t,S_REMOTE_CNT) \
    y(FixedString<50>,S_DATA)

DO_STRUCT(stock, STOCK_KEY_FIELDS, STOCK_VALUE_FIELDS, NAMESPACE_FIELDS)

#endif //SCAR_TPCC_SCHEMA_H
