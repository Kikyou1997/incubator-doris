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

#include "vec/exec/meta_scan_node.h"
namespace doris::vectorized {

    MetaScanNode::MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        :ExecNode(pool, tnode, descs),
        _tuple_id(tnode.meta_scan_node.tuple_id),
        _meta_scan_node(tnode.meta_scan_node),
        _tuple_desc(nullptr),
        _slot_to_dict(tnode.meta_scan_node.slot_to_dict)
    {
        //TODO
    }
    
    Status MetaScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
        //TODO
        return Status::OK();
    }

}