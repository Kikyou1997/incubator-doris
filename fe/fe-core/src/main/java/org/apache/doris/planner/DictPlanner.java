package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.statistics.ColumnDict;
import org.apache.doris.statistics.IDictManager;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DictPlanner {

    private final DecodeContext context;

    public DictPlanner(PlannerContext ctx, DescriptorTable tableDesc) {
        this.context = new DecodeContext(ctx, tableDesc);
    }

    public PlanNode plan(PlanNode plan) {

        findDictCodableSlot(plan);
        // for now, we only support the dict column used for aggregation, if it was used in
        // some other expressions then we simply disable the dict optimization on it
        filterSupportedDictSlot(plan);

        updateNodes(plan);

        generateDecodeNode(null, plan);

        plan = insertDecodeNode(null, plan);

        return plan;

    }

    private PlanNode insertDecodeNode(PlanNode parent, PlanNode plan) {
        Map<PlanNode, DecodeNode> childToDecodeMap = context.getChildToDecodeNode();
        DecodeNode decodeNode = childToDecodeMap.get(plan);

        if (parent != null && decodeNode != null) {
            List<PlanNode> children = parent.getChildren();
            for (int idx = 0; idx < children.size(); idx++) {
                PlanNode cur = children.get(idx);
                if (!cur.id.equals(plan.id)) {
                    continue;
                }
                children.set(idx, decodeNode);
                break;
            }
        }

        List<PlanNode> children = plan.getChildren();
        for (PlanNode child : children) {
            insertDecodeNode(plan, child);
        }
        if (parent == null) {
            if (decodeNode == null) {
                return plan;
            }
            return decodeNode;
        }
        return parent;
    }

    public void traversePlanTopDown(PlanNode plan, Function<PlanNode, Void> func) {
        func.apply(plan);
        for (PlanNode child : plan.getChildren()) {
            traversePlanTopDown(child, func);
        }
    }

    private void generateDecodeNode(PlanNode parent, PlanNode plan) {
        plan.generateDecodeNode(context);
        for (PlanNode child: plan.getChildren()) {
            generateDecodeNode(plan, child);
        }
    }

    private void updateNodes(PlanNode plan) {
        for (PlanNode child: plan.getChildren()) {
            updateNodes(child);
        }
        plan.updateSlots(context);
    }

    private void filterSupportedDictSlot(PlanNode plan) {
        plan.filterDictSlot(context);
        for (PlanNode child: plan.getChildren()) {
            findDictCodableSlot(child);
        }
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

}
