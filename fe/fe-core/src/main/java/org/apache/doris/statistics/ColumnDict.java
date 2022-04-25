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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ColumnDict {

    private List<String> dict = new ArrayList<>();

    {
        dict.add("RAIL");
        dict.add("FOB");
        dict.add("MAIL");
        dict.add("SHIP");
        dict.add("TRUCK");
        dict.add("REG AIR");
        dict.add("AIR");
        Collections.sort(dict);
    }
    private int id;

    public int getDictId() {
        return 0;
    }

    public List<String> getDict() {
        return this.dict;
    }
}
