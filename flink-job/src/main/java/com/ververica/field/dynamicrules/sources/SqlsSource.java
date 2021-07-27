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

package com.ververica.field.dynamicrules.sources;

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.KafkaUtils;
import com.ververica.field.dynamicrules.functions.SqlDeserializer;
import com.ververica.field.dynamicrules.functions.SqlEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.ververica.field.config.Parameters.*;

public class SqlsSource {

  private static final int SQLS_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createSqlsSource(Config config) throws IOException {

    String sourceType = config.get(SQLS_SOURCE);
    SqlsSource.Type rulesSourceType = SqlsSource.Type.valueOf(sourceType.toUpperCase());

    switch (rulesSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String rulesTopic = config.get(SQLS_TOPIC);
        FlinkKafkaConsumer011<String> kafkaConsumer =
            new FlinkKafkaConsumer011<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      case PUBSUB:
        return PubSubSource.newBuilder()
            .withDeserializationSchema(new SimpleStringSchema())
            .withProjectName(config.get(GCP_PROJECT_NAME))
            .withSubscriptionName(config.get(GCP_PUBSUB_RULES_SUBSCRIPTION))
            .build();
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<SqlEvent> stringsStreamToSqls(DataStream<String> sqlStrings) {
    return sqlStrings
        .flatMap(new SqlDeserializer())
        .name("SQL Deserialization")
        .setParallelism(SQLS_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<SqlEvent>(
                Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(SqlEvent element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    SOCKET("Rules Source (Socket)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
