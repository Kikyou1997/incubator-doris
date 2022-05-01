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

import com.google.common.base.Preconditions;
import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.ColumnDict;

import org.apache.doris.statistics.IDictManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DictPlanner {

    private DictContext dictContext = new DictContext();

    private Analyzer analyzer;

    public DictPlanner(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void assignDict(PlanNode root) {
        getAllPotentialAvailableDict(null, root);
        Set<AggregationNode> aggNodeSet = dictContext.aggNodeToSlotList.keySet();
        for (AggregationNode aggNode : aggNodeSet) {
            for (PlanNode child : aggNode.getChildren()) {
                removeUnavailableDictByPlanNode(aggNode, child);
            }
        }
    }

    private void getAllPotentialAvailableDict(PlanNode parent, PlanNode child) {
        if (child instanceof OlapScanNode) {
            getAllPotentialAvailableDictFromScanNode((OlapScanNode) child);
            return;
        }
        for (PlanNode planNode : child.getChildren()) {
            getAllPotentialAvailableDict(child, planNode);
        }
        if (child instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) child;
            dictContext.recordParentOfAgg(aggNode, parent);
            addToContextIfUsingDictColumn(aggNode);
        }
    }

    private void addToContextIfUsingDictColumn(AggregationNode aggNode) {
        AggregateInfo aggregateInfo = aggNode.getAggInfo();
        List<Expr> groupingExprList = aggregateInfo.getGroupingExprs();
        for (Expr expr: groupingExprList) {
            if (expr instanceof SlotRef && dictContext.havingDict((SlotRef) expr)) {
               dictContext.addAlternativeDictColForAggNode(aggNode, (SlotRef) expr);
            }
        }
    }

    // TODO(kikyo): process all PlanNode type
    private void removeUnavailableDictByPlanNode(AggregationNode root, PlanNode node) {
        List<Expr> conjunctList = node.getConjuncts();
        removeUnavailableDictByExprList(root, conjunctList);
        if (node instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) node;
            AggregateInfo aggInfo = aggNode.getAggInfo();
            List<Expr> groupingExprList = aggInfo.getGroupingExprs();
            List<FunctionCallExpr> aggExprList = aggInfo.getAggregateExprs();
            removeUnavailableDictByExprList(root, groupingExprList);
            removeUnavailableDictByExprList(root, aggExprList);
        } else if (node instanceof HashJoinNode) {
            HashJoinNode hjNode = (HashJoinNode) node;
            List<BinaryPredicate> eqJoinExprList = hjNode.getEqJoinConjuncts();
            removeUnavailableDictByExprList(root, eqJoinExprList);
        } else if (node instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) node;
            Set<SlotId> scanNodeSlotIdSet = olapScanNode.getTupleDesc().getSlots().stream().map(SlotDescriptor::getId).collect(Collectors.toSet());
            List<SlotRef> aggNodeDictSlotList = dictContext.getDictSlotForAggNode(root);
            scanNodeSlotIdSet.forEach(x -> {
              SlotRef correspondingSlot = null;
              for (SlotRef slotRef: aggNodeDictSlotList) {
                  SlotId slotId = slotRef.getSlotId();
                  if (slotId.equals(x)) {
                      SlotDescriptor slotDesc = slotRef.getDesc();
                      int slotOffset = slotDesc.getSlotOffset();
                      TupleDescriptor tupleDesc = slotDesc.getParent();
//                      TupleDescriptor tupleWithDict =
//                          analyzer.getDescTbl().copyTupleDescriptor(tupleDesc.getId(), "tuple-with-dict");
                      ColumnDict dict = dictContext.getDictBySlot(slotRef);
                      Preconditions.checkState(dict != null);
                      analyzer.putDict(slotId.asInt(), dict);
                      tupleDesc.updateSlotType(slotOffset, Type.INT);
                      correspondingSlot =  slotRef;
                      break;
                  }
              }

              if (correspondingSlot != null) {
                  olapScanNode.addDictAppliedSlot(correspondingSlot.getSlotId());
              }
            });
        }
        for (PlanNode planNode: node.getChildren()) {
            removeUnavailableDictByPlanNode(root, planNode);
        }
    }

    private <T extends Expr> void removeUnavailableDictByExprList(AggregationNode aggNode, List<T> exprList) {
        for (T expr: exprList) {
            removeUnavailableDictByExpr(aggNode, expr);
        }
    }
    private <T extends Expr> void removeUnavailableDictByExpr(AggregationNode aggNode, T expr) {
        if (expr instanceof SlotRef || expr instanceof LiteralExpr) {
            return;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            FunctionParams functionParams = functionCallExpr.getFnParams();
            for (Expr paramExpr : functionParams.exprs()) {
                if (paramExpr instanceof SlotRef) {
                    dictContext.removeFromAlternativeDicts(aggNode, (SlotRef) paramExpr);
                } else {
                    removeUnavailableDictByExpr(aggNode, expr);
                }
            }
        }
        List<Expr> exprList = expr.getChildren();
        for (Expr childExpr: exprList) {
            if (childExpr instanceof  SlotRef) {
                removeUnavailableDictBySlotRef(aggNode, (SlotRef) childExpr);
            } else {
                removeUnavailableDictByExpr(aggNode, childExpr);
            }
        }
    }

    private void removeUnavailableDictBySlotRef(AggregationNode aggNode, SlotRef slotRef) {
        dictContext.removeFromAlternativeDicts(aggNode, slotRef);
    }

    private void getAllPotentialAvailableDictFromScanNode(OlapScanNode root) {
        TupleDescriptor tupleDesc = root.getTupleDesc();
        long tableId = root.getOlapTable().getId();
        List<SlotDescriptor> slotsList = tupleDesc.getSlots();
        for (SlotDescriptor slotDesc : slotsList) {
            Column column = slotDesc.getColumn();
            String colName = column.getName();
            ColumnDict columnDict = tryToGetColumnDict(tableId, colName);
            if (columnDict == null) {
                continue;
            }
            dictContext.addPotentialAvailableDict(tableId, colName, columnDict);
        }
    }

    private ColumnDict tryToGetColumnDict(long tableId, String columnName) {
        IDictManager dictManager = IDictManager.getInstance();
        ColumnDict dict = dictManager.getDict(tableId, columnName);
        return dict;
    }

    private static class DictContext {

        Map<Long, Map<String, ColumnDict>> tableIdToColumnDict = new HashMap<>();

        private Map<AggregationNode, List<SlotRef>> aggNodeToSlotList = new HashMap<>();

        private Map<AggregationNode, PlanNode> aggToParent = new HashMap<>();

        public void addPotentialAvailableDict(long tableId, String columnName, ColumnDict dict) {
            Map<String, ColumnDict> colToDict = tableIdToColumnDict.getOrDefault(tableId, new HashMap<>());
            colToDict.put(columnName, dict);
            tableIdToColumnDict.put(tableId, colToDict);
        }

        public void removeFromAlternativeDicts(AggregationNode aggNode, SlotRef slotRef) {
            List<SlotRef> slotRefList = aggNodeToSlotList.get(aggNode);
            if (slotRefList == null) {
                return;
            }
            slotRefList.remove(slotRef);
        }

        public boolean havingDict(SlotRef slotRef) {
            return getDictBySlot(slotRef) != null;
        }

        public ColumnDict getDictBySlot(SlotRef slotRef) {
            long tableId = slotRef.getTable().getId();
            String columnName = slotRef.getColumnName();
            Map<String, ColumnDict> colToDict = tableIdToColumnDict.get(tableId);
            if (colToDict == null) {
                return null;
            }
            return colToDict.get(columnName);
        }

        public void addAlternativeDictColForAggNode(AggregationNode aggNode, SlotRef slotRef) {
            List<SlotRef> slotRefList = aggNodeToSlotList.getOrDefault(aggNode, new ArrayList<>());
            slotRefList.add(slotRef);
            aggNodeToSlotList.put(aggNode, slotRefList);
        }

        public List<SlotRef> getDictSlotForAggNode(AggregationNode aggNode) {
            return aggNodeToSlotList.getOrDefault(aggNode, new ArrayList<>());
        }

        public void recordParentOfAgg(AggregationNode agg, PlanNode parent) {
            aggToParent.put(agg, parent);
        }
    }

}
