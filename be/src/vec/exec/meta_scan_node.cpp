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
#include "vec/exec/volap_scan_node.h"
namespace doris::vectorized {

    MetaScanNode::MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        :ExecNode(pool, tnode, descs),
        _tuple_id(tnode.meta_scan_node.tuple_id),
        _meta_scan_node(tnode.meta_scan_node),
        _tuple_desc(nullptr),
        _slot_to_dict(tnode.meta_scan_node.slot_to_dict)
    {
        
        _inner_tnode.olap_scan_node.__set_tuple_id(tnode.meta_scan_node.tuple_id);
        _inner_tnode.olap_scan_node.__set_is_preaggregation(tnode.meta_scan_node.is_preaggregation);
        _inner_tnode.olap_scan_node.__set_sort_column(tnode.meta_scan_node.sort_column);
        _inner_tnode.olap_scan_node.__set_table_name(tnode.meta_scan_node.table_name);
        _inner_tnode.olap_scan_node.__set_key_column_name(tnode.meta_scan_node.key_column_name);
        _inner_tnode.olap_scan_node.__set_key_column_type(tnode.meta_scan_node.key_column_type);
        _inner_scan_node = std::make_unique<VOlapScanNode>(pool, _inner_tnode, descs);
    }
    
    Status MetaScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
        return _inner_scan_node->get_next(state, block, eos);
    }
    Status MetaScanNode::init(const TPlanNode& tnode, RuntimeState* state){
        return _inner_scan_node->init(_inner_tnode, state);
    }
    Status MetaScanNode::prepare(RuntimeState* state) {
        return _inner_scan_node->prepare(state);
    }
    Status MetaScanNode::open(RuntimeState* state){
        return _inner_scan_node->open(state);
    }
}