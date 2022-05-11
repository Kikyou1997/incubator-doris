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

#include "exec/exec_node.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class MemPool;
namespace vectorized {

class DecodeNode : public doris::ExecNode {
public:
    DecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

private:
    TDecodeNode _decode_node;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc;
    std::map<int, int> _slot_to_dict;
    std::map<int, int> _slot_to_pos;
    std::map<int, GlobalDictSPtr> _dicts;
};
} // namespace vectorized
} // namespace doris