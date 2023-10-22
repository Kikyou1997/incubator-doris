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

package org.apache.doris.qe;

import org.apache.doris.common.Config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

// CHECKSTYLE OFF
public class MyFileUtil {

    public static final Logger LOG = LogManager.getLogger(MyFileUtil.class);

    public static synchronized void appendSQL(String sql) {
        if (Config.log_analyze_sql) {
            return;
        }
        try (BufferedWriter writer =
                new BufferedWriter(new FileWriter(Config.path, true)) /* 'true' for append mode */) {
            writer.write(sql);
            writer.newLine(); // Add a newline character after the line
        } catch (IOException e) {
            LOG.warn("I was fucked, ", e);
        }
    }
}
