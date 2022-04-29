
//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
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

package org.apache.doris.planner;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.thrift.TDecodeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

public class DecodeNode extends PlanNode {
    private Map<Integer, Integer> slotIdToDictId = new HashMap();
    private Analyzer analyzer;

    protected DecodeNode(PlanNodeId id, PlanNode child, String planNodeName, Analyzer analyzer) {
        super(id, planNodeName);
        this.addChild(child);
        this.tblRefIds = child.tblRefIds;
        this.analyzer = analyzer;
    }

    public void addDecodingNeededSlots(SlotId slotId, int dictId) {
        this.slotIdToDictId.put(slotId.asInt(), dictId);
    }

    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.DECODE_NODE;
        msg.decode_node = new TDecodeNode(this.slotIdToDictId);
    }

    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder(prefix);
        StringJoiner dictColInfo = new StringJoiner(", ", "Decode col: ", "");
        Iterator var5 = this.slotIdToDictId.entrySet().iterator();

        while(var5.hasNext()) {
            Map.Entry<Integer, Integer> entry = (Map.Entry)var5.next();
            dictColInfo.add(String.format("<%s, %s>", entry.getKey(), entry.getValue()));
        }

        output.append(dictColInfo.toString());
        output.append("\n");
        return output.toString();
    }
}
