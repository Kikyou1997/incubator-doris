//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class DictCaseTest {

    private static String runningDir = "fe/mocked/DemoTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createDorisCluster(runningDir, 2);
        ctx = UtFrameUtils.createDefaultCtx();
        String createDbStmtStr = "create database tpch";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        String createTblStr = "CREATE TABLE tpch.`lineitem` (\n" +
            "  `l_orderkey` integer NOT NULL,\n" +
            "  `l_linenumber` integer NOT NULL,\n" +
            "  `l_partkey` integer NOT NULL,\n" +
            "  `l_suppkey` integer NOT NULL,\n" +
            "  `l_quantity` decimal(12, 2) NOT NULL,\n" +
            "  `l_extendedprice` decimal(12, 2) NOT NULL,\n" +
            "  `l_discount` decimal(12, 2) NOT NULL,\n" +
            "  `l_tax` decimal(12, 2) NOT NULL,\n" +
            "  `l_returnflag` char(1) NOT NULL,\n" +
            "  `l_linestatus` char(1) NOT NULL,\n" +
            "  `l_shipdate` date NOT NULL,\n" +
            "  `l_commitdate` date NOT NULL,\n" +
            "  `l_receiptdate` date NOT NULL,\n" +
            "  `l_shipinstruct` char(25) NOT NULL,\n" +
            "  `l_shipmode` char(10) NOT NULL,\n" +
            "  `l_comment` varchar(44) NOT NULL\n" +
            ") DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 32 PROPERTIES (\"replication_num\" = \"1\")";
        CreateTableStmt createColocateTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTblStr, ctx);
        Catalog.getCurrentCatalog().createTable(createColocateTableStmt);
    }

    @Test
    public void dictPlanTest() throws Exception {
        ConnectContext.get().getSessionVariable().setGlobalDictTest(true);
        String sql = "select count(*) , l_shipmode from tpch.lineitem group by l_shipmode";
        String plan = UtFrameUtils.getSQLPlanOrErrorMsg(ctx, sql);
        System.out.println(plan);

    }
}
