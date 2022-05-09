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

package org.apache.doris.statistics;

import com.google.common.collect.Lists;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface IDictManager {

    ColumnDict getDict(long tableId, String colName);

    static IDictManager getInstance() {
        // TODO: for test only, delete it later
        return new IDictManager() {
            @Override
            public ColumnDict getDict(long tableId, String colName) {
                if (ConnectContext.get().getSessionVariable().isDictTest()) {
                    if (colName.equalsIgnoreCase("l_shipmode")) {
                        return new ColumnDict() {
                            @Override
                            public List<String> getDict() {
                                List<String> l = Lists.newArrayList("FOB", "MAIL", "RAIL", "SHIP", "TRUCK",
                                    "REG AIR", "AIR");
                                Collections.sort(l);
                                return l;
                            }
                        };
                    }
                }
                switch (colName) {
                    case "v1":
                        return new ColumnDict() {
                            public List<String> getDict() {
                                List<String> l = new ArrayList<>();
                                l.add("test col1");
                                return l;
                            }
                        };
                    case "v2":
                        return new ColumnDict() {
                            public List<String> getDict() {
                                List<String> l = new ArrayList<>();
                                l.add("test col2");
                                return l;
                            }
                        };
                }
                return null;
            }
        };
    }
}
