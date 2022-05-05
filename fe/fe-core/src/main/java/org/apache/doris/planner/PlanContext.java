package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.ColumnDict;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanContext {

    private Map<Long, Set<Integer>> tableIdToDictCodableSlotSet = new HashMap<>();

    private Map<Integer, ColumnDict> slotIdToColumnDict = new HashMap<>();

    private Map<Integer, Integer> slotIdToDictSlotId = new HashMap<>();

    private Map<Integer, Integer> tupleIdToNewTupleWithDictSlot = new HashMap<>();

    private boolean couldEncoded;

    private Set<Integer> dictOptimizationDisabledSlot = new HashSet<>();

    private Set<Integer> dictCodableSlotSet = new HashSet<>();

    private DescriptorTable tableDescriptor;

    private PlannerContext ctx_;

    private Map<PlanNode, DecodeNode> childToDecodeNode = new HashMap<>();

    public PlanContext(PlannerContext ctx_, DescriptorTable tableDescriptor) {
        this.ctx_ = ctx_;
        this.tableDescriptor = tableDescriptor;
    }

    public void addDictCodableSlot(long tableId, int slotId) {
        Set<Integer> slotSet = tableIdToDictCodableSlotSet.getOrDefault(tableId, new HashSet<>());
        slotSet.add(slotId);
        dictCodableSlotSet.add(slotId);
    }

    public void addAvailableDict(int slotId, ColumnDict dict) {
        slotIdToColumnDict.put(slotId, dict);
    }

    public Set<Integer> getOriginSlotSet() {
        return slotIdToColumnDict.keySet();
    }

    public Set<Integer> getAllDictCodableSlot() {
        return dictCodableSlotSet;
    }

    public Set<Integer> getDictOptimizationDisabledSlot() {
        return dictOptimizationDisabledSlot;
    }

    public boolean isCouldEncoded() {
        return couldEncoded;
    }

    public void setCouldEncoded(boolean couldEncoded) {
        this.couldEncoded = couldEncoded;
    }

    public TupleDescriptor generateTupleDesc(TupleId src) {
        TupleDescriptor originTupleDesc = tableDescriptor.getTupleDesc(src);
        TupleDescriptor tupleDesc = tableDescriptor.copyTupleDescriptor(src, "tuple-with-dict-slots");
        tupleDesc.setTable(originTupleDesc.getTable());
        tupleDesc.setRef(originTupleDesc.getRef());
        tupleDesc.setAliases(originTupleDesc.getAliases_(), originTupleDesc.hasExplicitAlias());
        tupleDesc.setCardinality(originTupleDesc.getCardinality());
        tupleDesc.setIsMaterialized(originTupleDesc.getIsMaterialized());
        tupleIdToNewTupleWithDictSlot.put(src.asInt(), tupleDesc.getId().asInt());
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

    public DecodeNode newDecodeNode(PlanNode child, Set<Integer> originSlotIdSet, ArrayList<TupleId> output) {
        Map<Integer, Integer> slotIdToDictId = new HashMap<>();
        for (Integer originSlotId : originSlotIdSet) {
            Integer dictSlotId = slotIdToDictSlotId.get(originSlotId);
            slotIdToDictId.put(originSlotId, dictSlotId);
        }
        DecodeNode decodeNode =  new DecodeNode(ctx_.getNextNodeId(), child, slotIdToDictId, output);
        childToDecodeNode.put(child, decodeNode);
        return decodeNode;
    }

    public Map<PlanNode, DecodeNode> getChildToDecodeNode() {
        return childToDecodeNode;
    }

    public void addSlotToDictIntoDescTbl(int slotId) {
        tableDescriptor.putDict(slotId, slotIdToColumnDict.get(slotId));
    }
}
