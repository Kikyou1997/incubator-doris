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

#include "vec/exec/decode_node.h"
namespace doris::vectorized {

DecodeNode::DecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _decode_node(tnode.decode_node),
          _tuple_id(tnode.decode_node.tuple_id),
          _slot_to_dict(tnode.decode_node.slot_to_dict) {
    assert(!_slot_to_dict.empty());
}

Status DecodeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    assert(state);
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    for (const auto& item : _decode_node.slot_to_dict) {
        _dicts.emplace(item.first, state->get_global_dict(item.first));
    }

    for (const auto& slot : _slot_to_dict) {
        int pos = 0;
        for (const auto& slot_desc : _tuple_desc->slots()) {
            if (slot.first == slot_desc->id()) {
                _slot_to_pos.insert({slot.first, pos});
                break;
            }
            ++pos;
        }
    }
    return Status::OK();
}
Status DecodeNode::prepare(RuntimeState* state){
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    return Status::OK();
} 

Status DecodeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    for (const auto& slot : _slot_to_pos) {
        const auto it = _dicts.find(slot.first);
        assert(it != _dicts.end());
        assert(slot.second < block->columns());
        if (!it->second->decode(block->get_by_position(slot.second))) {
            return Status::Aborted("Decode dict encoded column failed");
        }
    }
    return Status::OK();
}

} // namespace doris::vectorized