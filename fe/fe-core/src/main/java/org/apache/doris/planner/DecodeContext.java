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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.ColumnDict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DecodeContext {

    private final Map<Integer, ColumnDict> slotIdToColumnDict = new HashMap<>();

    private final Map<Integer, Integer> slotIdToDictSlotId = new HashMap<>();

    private final  Set<Integer> encodeNeededSlotSet = new HashSet<>();

    private boolean needEncode;

    private final Set<Integer> dictOptimizationDisabledSlot = new HashSet<>();

    private final DescriptorTable tableDescriptor;

    private final PlannerContext ctx_;

    private final Map<PlanNode, DecodeNode> childToDecodeNode = new HashMap<>();

    public DecodeContext(PlannerContext ctx_, DescriptorTable tableDescriptor) {
        this.ctx_ = ctx_;
        this.tableDescriptor = tableDescriptor;
    }

    public void addAvailableDict(int slotId, ColumnDict dict) {
        slotIdToColumnDict.put(slotId, dict);
    }

    public Set<Integer> getOriginSlotSet() {
        return slotIdToColumnDict.keySet();
    }

    public ColumnDict getColumnDictBySlotId(int slotId) {
        return slotIdToColumnDict.get(slotId);
    }

    public Set<Integer> getDictCodableSlot() {
        return slotIdToColumnDict.keySet();
    }

    public Set<Integer> getAllEncodeNeededSlot() {
        return encodeNeededSlotSet;
    }



    public Set<Integer> getDictOptimizationDisabledSlot() {
        return dictOptimizationDisabledSlot;
    }


    public TupleDescriptor generateTupleDesc(TupleId src) {
        TupleDescriptor originTupleDesc = tableDescriptor.getTupleDesc(src);
        TupleDescriptor tupleDesc = tableDescriptor.copyTupleDescriptor(src, "tuple-with-dict-slots");
        tupleDesc.setTable(originTupleDesc.getTable());
        tupleDesc.setRef(originTupleDesc.getRef());
        tupleDesc.setAliases(originTupleDesc.getAliases_(), originTupleDesc.hasExplicitAlias());
        tupleDesc.setCardinality(originTupleDesc.getCardinality());
        tupleDesc.setIsMaterialized(originTupleDesc.getIsMaterialized());
        return tupleDesc;
    }

    public void addSlotToDictSlot(int slotId, int dictSlotId) {
        slotIdToDictSlotId.put(slotId, dictSlotId);
    }

    public SlotDescriptor getDictSlotDesc(int slotId) {
        int dictSlotId = slotIdToDictSlotId.get(slotId);
        return tableDescriptor.getSlotDesc(new SlotId(dictSlotId));
    }

    public void updateSlotRefType(SlotRef slotRef) {
        int slotId = slotRef.getSlotId().asInt();
        SlotDescriptor dictSlotDesc = this.getDictSlotDesc(slotId);
        slotRef.setDesc(dictSlotDesc);
        slotRef.setType(Type.INT);
    }

    public DecodeNode newDecodeNode(PlanNode child, List<Integer> originSlotIdSet, ArrayList<TupleId> output) {
        Map<Integer, Integer> slotIdToDictId = new HashMap<>();
        for (Integer originSlotId : originSlotIdSet) {
            Integer dictSlotId = slotIdToDictSlotId.get(originSlotId);
            ColumnDict columnDict = slotIdToColumnDict.get(originSlotId);
            slotIdToDictId.put(dictSlotId, columnDict.getId());
            tableDescriptor.putDict(columnDict.getId(), columnDict);
        }
        DecodeNode decodeNode =  new DecodeNode(ctx_.getNextNodeId(), child, slotIdToDictId, output);
        childToDecodeNode.put(child, decodeNode);
        return decodeNode;
    }

    public Map<PlanNode, DecodeNode> getChildToDecodeNode() {
        return childToDecodeNode;
    }

    public int getDictId(int slotId) {
        ColumnDict columnDict = slotIdToColumnDict.get(slotId);
        return columnDict.getId();
    }

    public boolean isNeedEncode() {
        return needEncode;
    }

    public void setNeedEncode(boolean needEncode) {
        this.needEncode = needEncode;
    }

    public void addEncodeNeededSlot(int slotId) {
        encodeNeededSlotSet.add(slotId);
    }

    public boolean needEncode() {
        return !encodeNeededSlotSet.isEmpty();
    }

    public DescriptorTable getTableDesc() {
        return tableDescriptor;
    }

    public void getDecodeRequiredSlotIdOfExpr(Expr expr, List<Integer> slotIdList) {
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            SlotDescriptor slotDesc = slotRef.getDesc();
            List<Expr> sourceExprList = slotDesc.getSourceExprs();
            if (!sourceExprList.isEmpty()) {
                for (Expr sourceExpr : sourceExprList) {
                    getDecodeRequiredSlotIdOfExpr(sourceExpr, slotIdList);
                }
                return;
            }
            Column column = slotRef.getColumn();
            int slotId = slotRef.getSlotId().asInt();
            if (column != null && slotIdToDictSlotId.containsKey(slotId)) {
                slotIdList.add(slotId);
                return;
            }
         } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            String functionName = functionCallExpr.getFnName().getFunction();
            if (FunctionSet.DICT_SUPPORT_FUNC_SET.contains(functionName)) {
                return;
            }
        }
        List<Expr> children = expr.getChildren();
        for (Expr child : children) {
            getDecodeRequiredSlotIdOfExpr(child, slotIdList);
        }

    }
}
