//package com.ververica.field.function;
//
//import com.ververica.field.dynamicrules.assigners.PeriodicTimestampAssigner;
//import com.ververica.field.dynamicrules.converters.StringConverter;
//import com.ververica.field.function.data.ShortBillingEvent;
//import com.ververica.field.function.sources.TestSourceConfig;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.junit.Test;
//import scala.reflect.ClassTag;
//
//import java.util.Arrays;
//
//public class BroadcastEmbeddedFlinkFunctionTest {
//  String rootTestDir = System.getProperty("user.dir");
//
//  String testInputData =
//      rootTestDir + "/src/test/resources/example-input-data/balance_change_example_data_3.csv";
//  String testBroadcastData =
//      rootTestDir + "/src/test/resources/example-input-data/broadcast_example_data_empty.csv";
//
//  int timeSpeedMultiplier = 10;
//  int processingTimeDelaySeconds = 0;
//  ClassTag<ShortBillingEvent> tag = scala.reflect.ClassTag$.MODULE$.apply(ShortBillingEvent.class);
//
//  StringConverter<ShortBillingEvent> converterIn =
//      new StringConverter<ShortBillingEvent>() {
//        @Override
//        public String toString(ShortBillingEvent input) {
//          return String.join(
//              ",",
//              new String[] {
//                input.msisdn,
//                input.eventDate,
//                input.balanceBefore.toString(),
//                input.balanceAfter.toString()
//              });
//        }
//
//        @Override
//        public ShortBillingEvent toValue(String input) {
//          return (ShortBillingEvent) new ShortBillingEvent().apply(input.trim(), ",");
//        }
//      };
//
//  TestSourceConfig sourceConfig =
//      new TestSourceConfig(testInputData, processingTimeDelaySeconds, timeSpeedMultiplier, ",");
//  AssignerWithPeriodicWatermarks assigner =
//      new PeriodicTimestampAssigner<ShortBillingEvent>(Integer.toUnsignedLong(sourceConfig.timeSpeedMultiplier()), 100L);
//
//  //    @Test
//  //    public void runBalanceChange() throws Exception {
//  //
//  //        for (int i = 1; i <= 3; i++) {
//  //            new BroadcastEmbeddedFlinkFunctionTestBase<String, ShortBillingEvent>(
//  //                    "/example-input-data/balance_change_example_data_" + i + ".csv",
//  //                    "/example-input-data/broadcast_example_data_empty.csv",
//  //                    timeSpeedMultiplier,
//  //                    processingTimeDelaySeconds,
//  //                    //                        "SELECT msisdn, eventDate FROM source_table WHERE
//  // 1=1",
//  //                    "SELECT t.msisdn, t.eventDate, user_tm FROM ( SELECT msisdn, eventDate,
//  // CAST(user_action_time AS TIMESTAMP) user_tm, CASE WHEN balanceBefore <=  10 AND balanceAfter >
//  // 10 THEN 'A' WHEN balanceBefore >  10 AND balanceAfter <=  10 THEN 'C' ELSE 'B' END AS
//  // patternVal FROM source_table ) t LEFT JOIN ( SELECT x.msisdn, x.`timestamp` , x.begin_date,
//  // x.end_date FROM source_table MATCH_RECOGNIZE ( PARTITION BY msisdn ORDER BY user_action_time
//  // MEASURES LAST(user_action_time) AS `timestamp`, A.eventDate AS begin_date, C.eventDate AS
//  // end_date ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B*? C)  WITHIN INTERVAL
//  // '25' SECOND DEFINE A AS A.balanceBefore <=  10 AND A.balanceAfter >  10, C AS C.balanceBefore >
//  //  10 AND C.balanceAfter <=  10 ) AS x ) p ON t.msisdn = p.msisdn AND t.eventDate = p.begin_date
//  // WHERE  t.patternVal = 'A' AND (p.msisdn is null OR timestampdiff(SECOND, CAST(p.begin_date AS
//  // TIMESTAMP), CAST(p.end_date AS TIMESTAMP)) > 25)",
//  //                    converterIn,
//  //                    Arrays.asList("msisdn",
//  //                            "eventDate",
//  //                            "balanceBefore",
//  //                            "balanceAfter"),
//  //                    "/expected-output-data/balance_change_alerts_" + i + ".csv",
//  //                    assigner,
//  //                    "msisdn",
//  //                    TypeInformation.of(new TypeHint<ShortBillingEvent>() {
//  //                    }),
//  //                    tag
//  //            ).run();
//  //            System.out.println("CASE " + i + " PASSED");
//  //        }
//  //
//  //    }
//  //
//  //    @Test
//  //    public void runCoreServiceUsage() throws Exception {
//  //
//  //        ClassTag<CoreServiceUsageBillingEvent> tag =
//  // scala.reflect.ClassTag$.MODULE$.apply(CoreServiceUsageBillingEvent.class);
//  //
//  //        StringConverter<CoreServiceUsageBillingEvent> converterIn = new
//  // StringConverter<CoreServiceUsageBillingEvent>() {
//  //            @Override
//  //            public String toString(CoreServiceUsageBillingEvent input) {
//  //                return String.join(",", new String[]{
//  //                        input.msisdn,
//  //                        input.eventDate,
//  //                        input.sourceEventType,
//  //                        input.sourceStatus,
//  //                        input.servedZone,
//  //                        input.otherZone,
//  //                        input.consumptionAmount.toString()
//  //                });
//  //            }
//  //
//  //            @Override
//  //            public CoreServiceUsageBillingEvent toValue(String input) {
//  //                return (CoreServiceUsageBillingEvent) new
//  // CoreServiceUsageBillingEvent().apply(input.trim(), ",");
//  //            }
//  //        };
//  //        AssignerWithPeriodicWatermarks assigner = new
//  // PeriodicTimestampAssigner<CoreServiceUsageBillingEvent>(sourceConfig.timeSpeedMultiplier(),
//  // 100).value();
//  //
//  //
//  //        new BroadcastEmbeddedFlinkFunctionTestBase<String, CoreServiceUsageBillingEvent>(
//  //                "/example-input-data/core_service_usage_example_data.csv",
//  //                "/example-input-data/broadcast_example_data_empty.csv",
//  //                timeSpeedMultiplier,
//  //                processingTimeDelaySeconds,
//  //                "SELECT t.msisdn, t.user_tm, t.first_other_zone, t.last_other_zone,
//  // t.first_served_zone, t.last_served_zone, t.event_type FROM source_table MATCH_RECOGNIZE (
//  // PARTITION BY msisdn    ORDER BY user_action_time    MEASURES        CAST(user_action_time AS
//  // TIMESTAMP) AS user_tm,        FIRST(otherZone) AS first_other_zone,        LAST(otherZone) AS
//  // last_other_zone,        FIRST(servedZone) AS first_served_zone,        LAST(servedZone) AS
//  // last_served_zone,        LAST(sourceEventType) AS event_type    ONE ROW PER MATCH    AFTER
//  // MATCH SKIP PAST LAST ROW    PATTERN (A+? B)  WITHIN INTERVAL '45' SECOND    DEFINE      B AS
//  // SUM(consumptionAmount) > 10 ) AS t",
//  //                converterIn,
//  //                Arrays.asList("msisdn",
//  //                        "eventDate",
//  //                        "sourceEventType",
//  //                        "sourceStatus",
//  //                        "servedZone",
//  //                        "otherZone",
//  //                        "consumptionAmount"),
//  //                "/expected-output-data/core_service_usage_alerts.csv",
//  //                assigner,
//  //                "msisdn",
//  //                TypeInformation.of(new TypeHint<CoreServiceUsageBillingEvent>() {
//  //                }),
//  //                tag
//  //        ).run();
//  //
//  //
//  //    }
//  //
//  //
//  //    @Test
//  //    public void runSubscriberTermination() throws Exception {
//  //
//  //        ClassTag<SubscriberTerminationBillingEvent> tag =
//  // scala.reflect.ClassTag$.MODULE$.apply(SubscriberTerminationBillingEvent.class);
//  //
//  //        StringConverter<SubscriberTerminationBillingEvent> converterInST = new
//  // StringConverter<SubscriberTerminationBillingEvent>() {
//  //            @Override
//  //            public String toString(SubscriberTerminationBillingEvent input) {
//  //                return String.join(",", new String[]{
//  //                        input.msisdn,
//  //                        input.eventDate,
//  //                        input.sourceEventType,
//  //                        input.sourceStatus
//  //                });
//  //            }
//  //
//  //            @Override
//  //            public SubscriberTerminationBillingEvent toValue(String input) {
//  //                return (SubscriberTerminationBillingEvent) new
//  // SubscriberTerminationBillingEvent().apply(input.trim(), ",");
//  //            }
//  //        };
//  //        AssignerWithPeriodicWatermarks assigner = new
//  // PeriodicTimestampAssigner<SubscriberTerminationBillingEvent>(sourceConfig.timeSpeedMultiplier(), 100).value();
//  //
//  //
//  //        new BroadcastEmbeddedFlinkFunctionTestBase<String, SubscriberTerminationBillingEvent>(
//  //                "/example-input-data/subscriber_termination_example_data.csv",
//  //                "/example-input-data/broadcast_example_data_empty.csv",
//  //                timeSpeedMultiplier,
//  //                processingTimeDelaySeconds,
//  //                "SELECT msisdn, eventDate FROM source_table WHERE 1=1",
//  ////                "SELECT t.msisdn, t.user_tm, t.termination_type  FROM source_table
//  // MATCH_RECOGNIZE (        PARTITION BY msisdn        ORDER BY user_action_time        MEASURES
//  //          CAST(user_action_time AS TIMESTAMP) AS user_tm,            LAST(sourceEventType) AS
//  // termination_type        ONE ROW PER MATCH        AFTER MATCH SKIP PAST LAST ROW        PATTERN
//  // (A B C)  WITHIN INTERVAL '30' SECOND     DEFINE       C AS A.sourceStatus IN ('UPDATE',
//  // 'DELETE') AND B.sourceStatus IN ('UPDATE', 'DELETE') AND A.sourceStatus <> B.sourceStatus OR
//  //         A.sourceStatus IN ('UPDATE', 'DELETE') AND C.sourceStatus IN ('UPDATE', 'DELETE') AND
//  // A.sourceStatus <> C.sourceStatus OR            B.sourceStatus IN ('UPDATE', 'DELETE') AND
//  // C.sourceStatus IN ('UPDATE', 'DELETE') AND B.sourceStatus <> C.sourceStatus      ) AS t",
//  //                converterInST,
//  //                Arrays.asList("msisdn",
//  //                        "eventDate",
//  //                        "sourceEventType",
//  //                        "sourceStatus"),
//  //                "/expected-output-data/subscriber_termination_alerts.csv",
//  //                assigner,
//  //                "msisdn",
//  //                TypeInformation.of(new TypeHint<SubscriberTerminationBillingEvent>() {
//  //                }),
//  //                tag
//  //        ).run();
//  //
//  //
//  //    }
//  //
////
////      @Test
////      public void runBroadcastSQL() throws Exception {
////
////          new BroadcastEmbeddedFlinkFunctionTestBase<String, ShortBillingEvent>(
////                  "src/test/resources/example-input-data/balance_change_example_data_3.csv",
////                  "src/test/resources/example-input-data/broadcast_example_data.csv",
////                  timeSpeedMultiplier,
////                  processingTimeDelaySeconds,
////                  "SELECT msisdn FROM source_table WHERE 1=1",
////                  converterIn,
////                  Arrays.asList("msisdn",
////                          "eventDate",
////                          "balanceBefore",
////                          "balanceAfter"),
////                  "src/test/resources/expected-output-data/balance_change_alerts_broadcast.csv",
////                  assigner,
////                  "msisdn",
////                  TypeInformation.of(new TypeHint<ShortBillingEvent>() {
////                  }),
////                  tag
////          ).run();
////      }
//
//  //    @Test
//  //    public void runSimultaneousSQL() throws Exception {
//  //        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//  //        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//  //
//  //        TestSourceConfig sourceConfig = new TestSourceConfig(rootTestDir +
//  // "/target/test-classes" + "/example-input-data/balance_change_example_data_3.csv",
//  // processingTimeDelaySeconds, timeSpeedMultiplier, ",");
//  //
//  //
//  //        DataStream<ShortBillingEvent> input = env
//  //                .addSource(new TestSource<>(sourceConfig, tag))
//  //                .returns(TypeInformation.of(new TypeHint<ShortBillingEvent>() {
//  //                }))
//  //                .assignTimestampsAndWatermarks(assigner)
//  //                .name("input")
//  //                .setParallelism(1)
//  //                .keyBy("msisdn");
//  //
//  //        Table inputTable = tableEnv.fromDataStream(input);
//  //        tableEnv.createTemporaryView("source_table", inputTable);
//  //        Table Table1 = tableEnv.sqlQuery("SELECT msisdn FROM source_table WHERE 1=1");
//  //        Table Table2 = tableEnv.sqlQuery("SELECT msisdn, balanceBefore FROM source_table WHERE
//  // 1=1");
//  //        Table Table3 = tableEnv.sqlQuery("SELECT msisdn, balanceBefore, balanceAfter FROM
//  // source_table WHERE 1=1");
//  //
//  //        DataStream<Row> res1 = tableEnv.toAppendStream(Table1, Row.class);
//  //        DataStream<Row> res2 = tableEnv.toAppendStream(Table2, Row.class);
//  //        DataStream<Row> res3 = tableEnv.toAppendStream(Table3, Row.class);
//  //
//  //        res1.print();
//  //        res2.print();
//  //        res3.print();
//  //
//  //
//  //        env.execute();
//  //    }
//  //
//  //
//  //    @Test
//  //    public void runEmbeddedLocalFlink() throws Exception {
//  //
//  //        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//  //
//  //        BroadcastEmbeddedFlinkFunction<String, ShortBillingEvent> countLengthFunction =
//  //                new BroadcastEmbeddedFlinkFunction<String, ShortBillingEvent>(
//  ////                        "SELECT msisdn, eventDate FROM source_table WHERE 1=1",
//  //                        "SELECT t.msisdn, t.eventDate, user_tm FROM ( SELECT msisdn, eventDate,
//  // CAST(user_action_time AS TIMESTAMP) user_tm, CASE WHEN balanceBefore <=  10 AND balanceAfter >
//  // 10 THEN 'A' WHEN balanceBefore >  10 AND balanceAfter <=  10 THEN 'C' ELSE 'B' END AS
//  // patternVal FROM source_table ) t LEFT JOIN ( SELECT x.msisdn, x.`timestamp` , x.begin_date,
//  // x.end_date FROM source_table MATCH_RECOGNIZE ( PARTITION BY msisdn ORDER BY user_action_time
//  // MEASURES LAST(user_action_time) AS `timestamp`, A.eventDate AS begin_date, C.eventDate AS
//  // end_date ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B*? C)  WITHIN INTERVAL
//  // '25' SECOND DEFINE A AS A.balanceBefore <=  10 AND A.balanceAfter >  10, C AS C.balanceBefore >
//  //  10 AND C.balanceAfter <=  10 ) AS x ) p ON t.msisdn = p.msisdn AND t.eventDate = p.begin_date
//  // WHERE  t.patternVal = 'A' AND (p.msisdn is null OR timestampdiff(SECOND, CAST(p.begin_date AS
//  // TIMESTAMP), CAST(p.end_date AS TIMESTAMP)) > 25)",
//  //                        TypeInformation.of(new TypeHint<ShortBillingEvent>() {
//  //                        }),
//  //                        Arrays.asList("msisdn",
//  //                                "eventDate",
//  //                                "balanceBefore",
//  //                                "balanceAfter"),
//  //                        converterIn,
//  //                        assigner
//  //                );
//  //
//  //        DataStream<ShortBillingEvent> billings = env
//  //                .addSource(new TestSource<ShortBillingEvent>(sourceConfig, tag))
//  //                .returns(TypeInformation.of(new TypeHint<ShortBillingEvent>() {
//  //                }))
//  //                .assignTimestampsAndWatermarks(assigner)
//  //                .name("billings")
//  //                .setParallelism(1)
//  //                .keyBy("msisdn");
//  //
//  //
//  //        DataStream<SqlEvent> sqls = env
//  //                .readTextFile(testBroadcastData).map(line -> new SqlEvent().apply(line, "\\|"))
//  //                .name("sqls")
//  //                .setParallelism(1);
//  //
//  //
//  //        MapStateDescriptor<String, SqlEvent> ruleStateDescriptor = new MapStateDescriptor<>(
//  //                "RulesBroadcastState",
//  //                BasicTypeInfo.STRING_TYPE_INFO,
//  //                TypeInformation.of(new TypeHint<SqlEvent>() {
//  //                }));
//  //
//  //
//  //        BroadcastStream<SqlEvent> sqlBroadcastStream = sqls
//  //                .broadcast(ruleStateDescriptor);
//  //
//  //        billings
//  //                .connect(sqlBroadcastStream)
//  //                .process(countLengthFunction)
//  //                .returns(new TypeHint<Tuple3<String, Boolean, Row>>() {
//  //                })
//  //                .print();
//  //
//  //        env.execute();
//  //    }
//
//}
