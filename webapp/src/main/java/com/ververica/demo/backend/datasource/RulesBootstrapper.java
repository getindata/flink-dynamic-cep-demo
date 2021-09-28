/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.demo.backend.datasource;

import com.ververica.demo.backend.repositories.SqlRepository;
import com.ververica.demo.backend.repositories.SqlRepositoryEvent;
import com.ververica.demo.backend.services.FlinkSqlService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class RulesBootstrapper implements ApplicationRunner {

  private SqlRepository sqlRepository;
  private FlinkSqlService flinkSqlService;

  @Autowired
  public RulesBootstrapper(SqlRepository userRepository, FlinkSqlService flinkSqlService) {
    this.sqlRepository = userRepository;
    this.flinkSqlService = flinkSqlService;
  }

  public void run(ApplicationArguments args) {
    String payload1 = "SELECT SUM(paymentAmount)\nFROM source_table\nWHERE paymentAmount <= 20";

    SqlRepositoryEvent sql1 = new SqlRepositoryEvent(payload1, 1);

    sqlRepository.save(sql1);

    List<SqlRepositoryEvent> sqls = sqlRepository.findAll();
    sqls.forEach(sql -> flinkSqlService.addSql(sql));
  }
}
