package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.statistics.ColumnDict;
import org.apache.doris.statistics.IDictManager;

import java.util.List;

public class DictPlanner2 {

    private PlanContext context;

    public void plan(PlanNode plan) {
        findDictCodableSlot(plan);
        // for now, we only support the column used for aggregation, if it was used in
        // some other expressions then we simply disable the dict optimization on it
        filterSupportedDictSlot(plan);

    }

    private void filterSupportedDictSlot(PlanNode plan) {

    }


    private void findDictCodableSlot(PlanNode node) {

        if (node instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) node;
            TupleDescriptor tupleDesc = olapScanNode.getTupleDesc();
            long tableId = olapScanNode.getOlapTable().getId();
            List<SlotDescriptor> slotsList = tupleDesc.getSlots();
            for (SlotDescriptor slotDesc : slotsList) {
                Column column = slotDesc.getColumn();
                String colName = column.getName();
                ColumnDict columnDict = tryToGetColumnDict(tableId, colName);
                if (columnDict == null) {
                    continue;
                }
                int slotId = slotDesc.getId().asInt();
                context.addDictCodableSlot(tableId, slotId);
                context.addAvailableDict(slotId, columnDict);
                return;
            }
        }

        for (PlanNode planNode : node.getChildren()) {
            findDictCodableSlot(planNode);
        }

    }

    private ColumnDict tryToGetColumnDict(long tableId, String columnName) {
        IDictManager dictManager = IDictManager.getInstance();
        ColumnDict dict = dictManager.getDict(tableId, columnName);
        return dict;
    }

    public static class ExprProcessor {

        public static void process(Expr expr, PlanContext context) {

        }



    }

}
