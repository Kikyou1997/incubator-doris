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


import org.apache.doris.analysis.AggregateInfo;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.statistics.ColumnDict;

import org.apache.doris.statistics.IDictManager;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DictPlanner {

    private DictContext dictContext;

    private Analyzer analyzer;

    public void assignDict(PlanNode root) {
        getAllPotentialAvailableDict(root);
        Set<AggregationNode> aggNodeSet = dictContext.aggNodeToSlotList.keySet();
        for (AggregationNode aggNode : aggNodeSet) {
            for (PlanNode child : aggNode.getChildren()) {
                removeUnavailableDictByPlanNode(aggNode, child);
            }
        }

    }

    private void updateTuples(PlanNode root) {

    }

    private void getAllPotentialAvailableDict(PlanNode root) {
        if (root instanceof OlapScanNode) {
            getAllPotentialAvailableDictFromScanNode((OlapScanNode) root);
            return;
        }
        for (PlanNode planNode : root.getChildren()) {
            getAllPotentialAvailableDict(planNode);
        }
        if (root instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) root;
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
        List<Expr> conjuncts = node.getConjuncts();
        removeUnavailableDictByExprList(root, conjuncts);
        if (node instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) node;
            List<Expr> groupingExprList = aggNode.getAggInfo().getGroupingExprs();
            removeUnavailableDictByExprList(aggNode, groupingExprList);
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
                  if (slotRef.getSlotId().equals(x)) {
                      correspondingSlot =  slotRef;
                      break;
                  }
              }
              if (correspondingSlot != null) {
                  olapScanNode.addDict(correspondingSlot.getSlotId(), dictContext.getDictIdBySlot(correspondingSlot));
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
        if (expr instanceof SlotRef) {
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

            }
        }


    }

    private void removeUnavailableDictBySlotRef(AggregationNode aggNode, SlotRef slotRef) {
        String columnName = slotRef.getColumnName();
        long tableId = slotRef.getTable().getId();
        dictContext.removeFromAlternativeDicts(aggNode, slotRef);
    }

    private void getAllPotentialAvailableDictFromScanNode(OlapScanNode root) {
        TupleDescriptor tupleDesc = root.getTupleDesc();
        long tableId = root.getOlapTable().getId();
        List<SlotDescriptor> slotsList = tupleDesc.getSlots();
        for (SlotDescriptor slotDesc : slotsList) {
            Column column = slotDesc.getColumn();
            String colName = column.getName();
            int dictId = tryToGetColumnDict(tableId, colName);
            if (dictId < 0) {
                continue;
            }
            dictContext.addPotentialAvailableDict(tableId, colName, dictId);
        }
    }

    private int tryToGetColumnDict(long tableId, String columnName) {
        IDictManager dictManager = IDictManager.getInstance();
        ColumnDict dict = dictManager.getColumnDict(tableId, columnName);
        if (dict == null) {
            return -1;
        }
        return dict.getDictId();
    }

    private static class DictContext {

        Map<Long, Map<String, Integer>> tableIdToColumnDict = new HashMap<>();

        private Set<SlotDescriptor> downstreamSlotSet = new HashSet<>();

        private Map<AggregationNode, List<SlotRef>> aggNodeToSlotList = new HashMap<>();

        public void addPotentialAvailableDict(long tableId, String columnName, int dictId) {
            Map<String, Integer> colToDict = tableIdToColumnDict.getOrDefault(tableId, new HashMap<>());
            colToDict.put(columnName, dictId);
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
            return getDictIdBySlot(slotRef) != null;
        }

        public Integer getDictIdBySlot(SlotRef slotRef) {
            long tableId = slotRef.getTable().getId();
            String columnName = slotRef.getColumnName();
            Map<String, Integer> colToDict = tableIdToColumnDict.get(tableId);
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
    }

}
