package com.ververica.field.dynamicrules.util;

public class SchemaHelper {
    static public String getSchemaContents(){
        return "################################################################################\n" +
                "#  Licensed to the Apache Software Foundation (ASF) under one\n" +
                "#  or more contributor license agreements.  See the NOTICE file\n" +
                "#  distributed with this work for additional information\n" +
                "#  regarding copyright ownership.  The ASF licenses this file\n" +
                "#  to you under the Apache License, Version 2.0 (the\n" +
                "#  \"License\"); you may not use this file except in compliance\n" +
                "#  with the License.  You may obtain a copy of the License at\n" +
                "#\n" +
                "#      http://www.apache.org/licenses/LICENSE-2.0\n" +
                "#\n" +
                "#  Unless required by applicable law or agreed to in writing, software\n" +
                "#  distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
                "#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
                "#  See the License for the specific language governing permissions and\n" +
                "# limitations under the License.\n" +
                "################################################################################\n" +
                "\n" +
                "#==============================================================================\n" +
                "# TEST ENVIRONMENT FILE\n" +
                "# General purpose default environment file.\n" +
                "#==============================================================================\n" +
                "\n" +
                "# this file has variables that can be filled with content by replacing $VAR_XXX\n" +
                "\n" +
                "tables:\n" +
                "  - name: TableNumber1\n" +
                "    type: source-table\n" +
                "    $VAR_UPDATE_MODE\n" +
                "    schema:\n" +
                "      - name: IntegerField1\n" +
                "        type: INT\n" +
                "      - name: StringField1\n" +
                "        type: VARCHAR\n" +
                "      - name: TimestampField1\n" +
                "        type: TIMESTAMP\n" +
                "    connector:\n" +
                "      type: filesystem\n" +
                "      path: \"$VAR_SOURCE_PATH1\"\n" +
                "    format:\n" +
                "      type: csv\n" +
                "      fields:\n" +
                "        - name: IntegerField1\n" +
                "          type: INT\n" +
                "        - name: StringField1\n" +
                "          type: VARCHAR\n" +
                "        - name: TimestampField1\n" +
                "          type: TIMESTAMP\n" +
                "      line-delimiter: \"\\n\"\n" +
                "      comment-prefix: \"#\"\n" +
                "\n" +
                "execution:\n" +
                "  planner: \"$VAR_PLANNER\"\n" +
                "  type: \"$VAR_EXECUTION_TYPE\"\n" +
                "  time-characteristic: event-time\n" +
                "  periodic-watermarks-interval: 99\n" +
                "  parallelism: 1\n" +
                "  max-parallelism: 16\n" +
                "  min-idle-state-retention: 1000\n" +
                "  max-idle-state-retention: 600000\n" +
                "  result-mode: \"$VAR_RESULT_MODE\"\n" +
                "  max-table-result-rows: \"$VAR_MAX_ROWS\"\n" +
                "  restart-strategy:\n" +
                "    type: \"$VAR_RESTART_STRATEGY_TYPE\"\n" +
                "    max-failures-per-interval: 10\n" +
                "    failure-rate-interval: 99000\n" +
                "    delay: 1000\n" +
                "\n" +
                "configuration:\n" +
                "  table.optimizer.join-reorder-enabled: false\n" +
                "\n" +
                "deployment:\n" +
                "  response-timeout: 5000\n";
    }
}
