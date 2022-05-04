// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <type_traits>

#include "vec/columns/column_dict_encoded_string.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_number_base.h"

namespace doris::vectorized {

template <typename T>
class DataTypeDictEncodedString final : public DataTypeNumberBase<T> {
public:
    DataTypeDictEncodedString() {}
    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    bool can_be_used_in_bit_operations() const override { return true; }
    bool can_be_used_in_boolean_context() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    TypeIndex get_type_id() const override {
        if constexpr (std::is_same_v<T, UInt8>) {
            return TypeIndex::DictUInt8;
        } else if constexpr (std::is_same_v<T, UInt16>) {
            return TypeIndex::DictUInt16;
        } else {
            static_assert(std::is_same_v<T, UInt32>);
            return TypeIndex::DictUInt32;
        }
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column) const override {
        return sizeof(uint32_t) + column.size() * sizeof(FieldType) + sizeof(DictId);
    }

    char* serialize(const IColumn& column, char* buf) const override {
        // row num
        const auto row_num = column.size();
        *reinterpret_cast<uint32_t*>(buf) = row_num;
        buf += sizeof(uint32_t);

        // column data
        auto ptr = column.convert_to_full_column_if_const();
        const ColumnDictEncodedString<T>* col =
                assert_cast<const ColumnDictEncodedString<T>*>(ptr.get());
        const auto* origin_data = col->get_data().data();
        memcpy(buf, origin_data, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);

        // dict id
        *reinterpret_cast<DictId*>(buf) = col->get_dict_id();
        buf += sizeof(DictId);

        return buf;
    }

    const char* deserialize(const char* buf, IColumn* column) const override {
        // row num
        uint32_t row_num = *reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);

        // column data
        ColumnDictEncodedString<T>* col = assert_cast<ColumnDictEncodedString<T>*>(column);
        auto& container = col->get_data();
        container.resize(row_num);
        memcpy(container.data(), buf, row_num * sizeof(FieldType));
        buf += row_num * sizeof(FieldType);
        
        // dict id
        col->set_dict_id(*reinterpret_cast<const DictId*>(buf));
        buf += sizeof(DictId);

        return buf;
    }

    MutableColumnPtr create_column() const override { return ColumnDictEncodedString<T>::create(); }
};

using DataTypeDictEncodedStringUInt8 = DataTypeDictEncodedString<UInt8>;
using DataTypeDictEncodedStringUInt16 = DataTypeDictEncodedString<UInt16>;
using DataTypeDictEncodedStringUInt32 = DataTypeDictEncodedString<UInt32>;

} // namespace doris::vectorized
