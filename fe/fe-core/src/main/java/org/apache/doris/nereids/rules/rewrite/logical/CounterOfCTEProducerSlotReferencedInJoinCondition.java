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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.logical.CounterOfCTEProducerSlotReferencedInJoinCondition.Context;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CounterOfCTEProducerSlotReferencedInJoinCondition extends DefaultPlanVisitor<Plan, Context>
        implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, new Context(jobContext.getCascadesContext()));
    }

    @Override
    public Plan visit(Plan plan, Context context) {
        plan.children().forEach(child -> child.accept(this, context));
        return plan;
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Context context) {
        join.left().accept(this, context);
        join.right().accept(this, context);
        List<Expression> joinConditions = join.getHashJoinConjuncts();
        for (Expression expression : joinConditions) {
            if (!(expression instanceof EqualTo)) {
                continue;
            }
            EqualTo equalTo = (EqualTo) expression;
            if (!(equalTo.left() instanceof SlotReference)) {
                continue;
            }
            Slot leftSlot = (SlotReference) equalTo.left();
            Slot cteSlot = context.aliasSlotToCTESlot.get(leftSlot);
            if (cteSlot == null) {
                if (context.cteSlotToCTEId.containsKey(leftSlot)) {
                    cteSlot = leftSlot;
                } else {
                    continue;
                }
            }
            CTEId cteId = context.cteSlotToCTEId.get(cteSlot);
            Map<Slot, Slot> consumerToProducerOutputMap = context.cteIdToConsumerToProducerOutputMap.get(cteId);
            Slot producerSlot = consumerToProducerOutputMap.get(cteSlot);
            context.cascadesContext.addCTESlotRefCount(cteId, producerSlot);
        }
        return join;
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Context context) {
        project.child().accept(this, context);
        for (NamedExpression p : project.getProjects()) {
            if (p instanceof Alias) {
                Alias alias = (Alias) p;
                if (alias.child() instanceof SlotReference) {
                    SlotReference slotReference = (SlotReference) alias.child();
                    // Value might be null.
                    context.aliasSlotToCTESlot.put(alias.toSlot(), context.aliasSlotToCTESlot.remove(slotReference));
                }
            }
        }
        return project;
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, Context context) {
        CTEId cteId = cteConsumer.getCteId();
        Map<Slot, Slot> consumerToProducerOutputMap =
                context.cteIdToConsumerToProducerOutputMap.computeIfAbsent(cteId, k -> new HashMap<>());
        consumerToProducerOutputMap.putAll(cteConsumer.getConsumerToProducerOutputMap());
        for (Slot slot : cteConsumer.getOutput()) {
            context.cteSlotToCTEId.put(slot, cteId);
        }
        return cteConsumer;
    }

    public static class Context {
        // Map alias of slot to slot from consumer
        Map<NamedExpression, Slot> aliasSlotToCTESlot = new HashMap<>();
        Map<Slot, CTEId> cteSlotToCTEId = new HashMap<>();

        Map<CTEId, Map<Slot, Slot>> cteIdToConsumerToProducerOutputMap = new HashMap<>();

        CascadesContext cascadesContext;

        public Context(CascadesContext cascadesContext) {
            this.cascadesContext = cascadesContext;
        }
    }
}
