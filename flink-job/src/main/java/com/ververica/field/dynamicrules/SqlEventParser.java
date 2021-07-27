/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.field.dynamicrules;

import com.ververica.field.dynamicrules.functions.SqlEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SqlEventParser {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static SqlEvent parsePlain(String sqlString) throws InstantiationException, IllegalAccessException {
    return SqlEvent.createFromCsv(sqlString, SqlEvent.class);
  }

  public SqlEvent fromString(String line) throws IOException, IllegalAccessException, InstantiationException {
    if (line.length() > 0 && '{' == line.charAt(0)) {
      return parseJson(line);
    } else {
      return parsePlain(line);
    }
  }

  private SqlEvent parseJson(String sqlString) throws IOException {
    return objectMapper.readValue(sqlString, SqlEvent.class);
  }
}
