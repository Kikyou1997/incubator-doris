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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.planner.HashJoinNode;

import com.google.common.collect.ImmutableList;

public class CTEPostProcessor extends PlanPostProcessor {

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        return plan.accept(this, ctx);
    }

    @Override
    public Plan visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, CascadesContext context) {
        distribute = distribute.withChildren(ImmutableList.of(distribute.child().accept(this, context)));
        Plan cur = distribute;
        do {
            cur = cur.child(0);
        } while (!(cur instanceof HashJoinNode
                || cur instanceof PhysicalDistribute || cur instanceof LeafPlan) && cur instanceof UnaryPlan);
        if (!(cur instanceof PhysicalCTEConsumer)) {
            return distribute;
        }
        DistributionSpec distributionSpec = distribute.getDistributionSpec();
        PhysicalCTEConsumer cteConsumer = (PhysicalCTEConsumer) cur;
        context.addDistributionForCTE(cteConsumer.getConsumerId(), distributionSpec);
        return distribute.child();
    }
}
