package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Type;
import org.apache.doris.statistics.ColumnDict;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PlanContext {
    private Map<Long, Set<Integer>> tableIdToDictCodableSlotSet = new HashMap<>();

    private Map<Integer, ColumnDict> slotIdToColumnDict = new HashMap<>();

    private Map<Integer, Integer> slotIdToDictSlotId = new HashMap<>();

    private Map<Integer, Integer> tupleIdToNewTupleWithDictSlot = new HashMap<>();
    private boolean encoded;

    private Set<Integer> dictOptimizationDisabledSlot = new HashSet<>();

    private Set<Integer> dictCodableSlotSet = new HashSet<>();

    private DescriptorTable tableDescriptor;

    public PlanContext(DescriptorTable tableDescriptor) {
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

    public Set<Integer> getAllDictCodableSlot() {
        return dictCodableSlotSet;
    }

    public Set<Integer> getDictOptimizationDisabledSlot() {
        return dictOptimizationDisabledSlot;
    }

    public boolean isEncoded() {
        return encoded;
    }

    public void setEncoded(boolean encoded) {
        this.encoded = encoded;
    }

    public TupleDescriptor generateTupleDesc(TupleId src) {
       TupleDescriptor tupleDesc = tableDescriptor.copyTupleDescriptor(src, "tuple-with-dict-slots");
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

}
