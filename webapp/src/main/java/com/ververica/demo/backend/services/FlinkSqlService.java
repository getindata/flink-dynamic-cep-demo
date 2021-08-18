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

package com.ververica.demo.backend.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.demo.backend.configurations.PropertyLogger;
import com.ververica.demo.backend.repositories.SqlRepositoryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class FlinkSqlService {

  private KafkaTemplate<String, String> kafkaTemplate;
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyLogger.class);

  @Value("${kafka.topic.sqls}")
  private String topic;

  private final ObjectMapper mapper = new ObjectMapper();

  @Autowired
  public FlinkSqlService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void addSql(SqlRepositoryEvent sql) {
    String toSend = "1970-01-01 00:01:01," + sql.content;
    LOGGER.info("To send: " + toSend);
    kafkaTemplate.send(topic, toSend);
  }

  public void deleteSql(SqlRepositoryEvent sql) {
    String toSend = "REMOVE," + sql.content;
    LOGGER.info("To send: " + toSend);
    kafkaTemplate.send(topic, toSend);
  }
}
