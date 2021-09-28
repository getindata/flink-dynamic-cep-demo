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

package com.ververica.demo.backend.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.configurations.PropertyLogger;
import com.ververica.demo.backend.repositories.SqlRepository;
import com.ververica.demo.backend.repositories.SqlRepositoryEvent;
import com.ververica.demo.backend.services.FlinkSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api")
class SqlsController {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyLogger.class);

  private final SqlRepository repository;
  private final FlinkSqlService flinkSqlService;

  SqlsController(SqlRepository repository, FlinkSqlService flinkSqlService) {
    this.repository = repository;
    this.flinkSqlService = flinkSqlService;
  }

  private final ObjectMapper mapper = new ObjectMapper();

  @GetMapping("/sqls")
  List<SqlRepositoryEvent> all() {
    return repository.findAll();
  }

  @PostMapping("/sqls")
  SqlRepositoryEvent newSql(@RequestBody SqlRepositoryEvent newSql) throws IOException {
    LOGGER.info("New SQL: " + newSql.content);
    SqlRepositoryEvent savedSql = repository.save(newSql);

    flinkSqlService.addSql(savedSql);
    return savedSql;
  }

  @GetMapping("/sqls/pushToFlink")
  void pushToFlink() {
    List<SqlRepositoryEvent> sqls = repository.findAll();
    for (SqlRepositoryEvent sql : sqls) {
      flinkSqlService.addSql(sql);
    }
  }

  @DeleteMapping("/sqls/{id}")
  void deleteSql(@PathVariable Integer id) {
    Optional<SqlRepositoryEvent> maybeSql = repository.findById(id);

    if (maybeSql.isPresent()) {
      SqlRepositoryEvent sqlToRemove = maybeSql.get();
      repository.deleteById(id);
      flinkSqlService.deleteSql(sqlToRemove);
    }
  }
}
