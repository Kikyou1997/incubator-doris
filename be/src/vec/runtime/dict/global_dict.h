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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/string-value.h
// and modified by Doris

#pragma once

#include "runtime/string_value.h"
#include "vec/runtime/dict/dict.h"
#include "vec/columns/column.h"

namespace doris {
namespace vectorized{  
class GlobalDict: public Dict<int>{
public:        
    GlobalDict(const std::vector<std::string>& data): Dict(data) {};
    //string column to int column
    MutableColumnPtr encode(ColumnPtr column);
    //int column to string column
    MutableColumnPtr decode(ColumnPtr column);
    int id(){ return _id; };
    size_t version() { return _version; };
private:

    int cardinality();
    // dict version
    size_t _version;
    int _id;

};

using GlobalDictSPtr = std::shared_ptr<GlobalDict>;


}   //namespace vectorized
}   //namespace doris