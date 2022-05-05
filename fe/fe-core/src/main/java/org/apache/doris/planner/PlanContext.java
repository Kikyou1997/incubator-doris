package org.apache.doris.planner;

import org.apache.doris.statistics.ColumnDict;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PlanContext {
    private Map<Long, Set<Integer>> tableIdToDictCodableSlotSet = new HashMap<>();

    private Map<Integer, ColumnDict> slotIdToColumnDict = new HashMap<>();

    private boolean encoded;

    private Set<Integer> dictOptimizationDisabledSlot = new HashSet<>();

    private Set<Integer> dictCodableSlotSet = new HashSet<>();

    
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
}
