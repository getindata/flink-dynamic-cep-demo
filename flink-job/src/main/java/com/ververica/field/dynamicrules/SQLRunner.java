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

import com.ververica.field.config.Config;
import com.ververica.field.dynamicrules.converters.StringConverter;
import com.ververica.field.dynamicrules.converters.TransactionStringConverter;
import com.ververica.field.dynamicrules.functions.BroadcastEmbeddedFlinkFunction;
import com.ververica.field.dynamicrules.sinks.AlertsSink;
import com.ververica.field.dynamicrules.sources.SqlsSource;
import com.ververica.field.dynamicrules.sources.TransactionEventsSource;
import com.ververica.field.dynamicrules.functions.SqlEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.ververica.field.config.Parameters.*;

@Slf4j
public class SQLRunner {

  private Config config;

  SQLRunner(Config config) {
    this.config = config;
  }

  public void run() throws Exception {

    SqlsSource.Type sqlsSourceType = getSqlsSourceType();

    boolean isLocal = config.get(LOCAL_EXECUTION);
    boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS);
    int checkpointsInterval = config.get(CHECKPOINT_INTERVAL);
    int minPauseBtwnCheckpoints = config.get(CHECKPOINT_INTERVAL);

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment(sqlsSourceType, isLocal);

    if (enableCheckpoints) {
      env.enableCheckpointing(checkpointsInterval);
      env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
    }

    // Streams setup
    DataStream<TransactionEvent> transactions = getTransactionsStream(env).keyBy("payeeId");
    AssignerWithPeriodicWatermarks assigner =
        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS));


    BroadcastEmbeddedFlinkFunction<String, TransactionEvent> embeddedFlinkFunction =
        new BroadcastEmbeddedFlinkFunction<String, TransactionEvent>(
            TypeInformation.of(new TypeHint<TransactionEvent>() {}),
            Arrays.asList(
                "transactionId",
                "eventTime",
                "payeeId",
                "beneficiaryId",
                "paymentAmount",
                "paymentType",
                "ingestionTimestamp"),
            TransactionStringConverter.class,
            assigner);

    DataStream<SqlEvent> sqls = getSqlsUpdateStream(env);

    MapStateDescriptor<String, SqlEvent> ruleStateDescriptor =
        new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<SqlEvent>() {}));

    BroadcastStream<SqlEvent> sqlBroadcastStream = sqls.broadcast(ruleStateDescriptor);

    DataStream<Tuple4<String, Boolean, Row, Long>> output =
        transactions.connect(sqlBroadcastStream).process(embeddedFlinkFunction).setParallelism(1);

    output.print().name("Alert STDOUT Sink");

    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(output.map(Alert::fromTuple));

    alertsJson
            .addSink(AlertsSink.createAlertsSink(config))
            .setParallelism(1)
            .name("Alerts JSON Sink");

    env.execute();
  }

  private DataStream<TransactionEvent> getTransactionsStream(StreamExecutionEnvironment env) {
    // Data stream setup
    SourceFunction<String> transactionSource =
        TransactionEventsSource.createTransactionEventsSource(config);
    int sourceParallelism = config.get(SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        env.addSource(transactionSource)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<TransactionEvent> transactionsStream =
        TransactionEventsSource.stringsStreamToTransactionEvents(transactionsStringsStream);
    return transactionsStream.assignTimestampsAndWatermarks(
        new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS)));
  }

  private DataStream<SqlEvent> getSqlsUpdateStream(StreamExecutionEnvironment env)
      throws IOException {

    SqlsSource.Type sqlsSourceEnumType = getSqlsSourceType();

    SourceFunction<String> sqlsSource = SqlsSource.createSqlsSource(config);
    DataStream<String> sqlsStrings =
        env.addSource(sqlsSource).name(sqlsSourceEnumType.getName()).setParallelism(1);
    return SqlsSource.stringsStreamToSqls(sqlsStrings);
  }

  private SqlsSource.Type getSqlsSourceType() {
    String sqlsSource = config.get(SQLS_SOURCE);
    return SqlsSource.Type.valueOf(sqlsSource.toUpperCase());
  }

  private StreamExecutionEnvironment configureStreamExecutionEnvironment(
      SqlsSource.Type sqlsSourceEnumType, boolean isLocal) {
    Configuration flinkConfig = new Configuration();
    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

    StreamExecutionEnvironment env =
        isLocal
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
            : StreamExecutionEnvironment.getExecutionEnvironment();

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
    env.getCheckpointConfig()
        .setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));

    configureRestartStrategy(env, sqlsSourceEnumType);
    return env;
  }

  private void configureRestartStrategy(
      StreamExecutionEnvironment env, SqlsSource.Type rulesSourceEnumType) {
    switch (rulesSourceEnumType) {
      case SOCKET:
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
        break;
      case KAFKA:
        // Default - unlimited restart strategy.
        //        env.setRestartStrategy(RestartStrategies.noRestart());
    }
  }

  private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends TransactionEvent>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }
}
