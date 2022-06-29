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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;

import java.util.List;
import java.util.Optional;

/**
 * interface for all concrete logical plan operator.
 */
public abstract class LogicalOperator extends Operator {
    public LogicalOperator(OperatorType type) {
        super(type);
    }

    public LogicalOperator(OperatorType type, long limited) {
        super(type, limited);
    }

    public LogicalProperties computeLogicalProperties(Plan... inputs) {
        return new LogicalProperties(() -> computeOutput(inputs));
    }

    public abstract List<Slot> computeOutput(Plan... inputs);

    @Override
    public LogicalPlan toTreeNode(GroupExpression groupExpression) {
        LogicalProperties logicalProperties = groupExpression.getParent().getLogicalProperties();
        switch (type.childCount) {
            case 0:
                return new LogicalLeafPlan(this, Optional.of(groupExpression), Optional.of(logicalProperties));
            case 1:
                return new LogicalUnaryPlan(this, Optional.of(groupExpression),
                        Optional.of(logicalProperties), new GroupPlan(groupExpression.child(0)));
            case 2:
                return new LogicalBinaryPlan(this, Optional.of(groupExpression),
                        Optional.of(logicalProperties), new GroupPlan(groupExpression.child(0)),
                        new GroupPlan(groupExpression.child(1)));
            default:
                throw new RuntimeException(String.format("Unexpected children count: %d", type.childCount));
        }

    }
}
