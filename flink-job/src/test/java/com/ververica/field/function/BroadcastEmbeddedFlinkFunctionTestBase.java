//package com.ververica.field.function;
//
//import com.ververica.field.dynamicrules.converters.StringConverter;
//import com.ververica.field.dynamicrules.functions.BroadcastEmbeddedFlinkFunction;
//import com.ververica.field.function.sources.TestSource;
//import com.ververica.field.function.sources.TestSourceConfig;
//import com.ververica.field.dynamicrules.functions.SqlEvent;
//import com.ververica.field.dynamicrules.sources.TimeBasedEvent;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
//import org.apache.flink.types.Row;
//import scala.reflect.ClassTag;
//
//import java.io.File;
//import java.util.List;
//
//public class BroadcastEmbeddedFlinkFunctionTestBase<KEY, IN extends TimeBasedEvent> {
//  String rootTestDir = System.getProperty("user.dir") + "/target/test-classes";
//
//  String testInputDataPath;
//  String testBroadcastDataPath;
//  String defaultSql;
//  String keyByField;
//
//  int timeSpeedMultiplier;
//  int processingTimeDelaySeconds;
//
//  StringConverter<IN> inConverter;
//  List<String> expressions;
//  String expectedOutputDataPath;
//  AssignerWithPeriodicWatermarks<IN> assigner;
//
//  TypeInformation<IN> inTypeInfo;
//
//  StreamExecutionEnvironment env;
//  ClassTag<IN> tag;
//
//  public BroadcastEmbeddedFlinkFunctionTestBase(
//      String testInputDataPath,
//      String testBroadcastDataPath,
//      int timeSpeedMultiplier,
//      int processingTimeDelaySeconds,
//      String defaultSql,
//      StringConverter<IN> inConverter,
//      List<String> expressions,
//      String expectedOutputDataPath,
//      AssignerWithPeriodicWatermarks<IN> assigner,
//      String keyByField,
//      TypeInformation<IN> inTypeInfo,
//      ClassTag<IN> tag) {
//    this.testInputDataPath = new File(testInputDataPath).getAbsolutePath();
//    this.testBroadcastDataPath = new File(testBroadcastDataPath).getAbsolutePath();
//    this.timeSpeedMultiplier = timeSpeedMultiplier;
//    this.processingTimeDelaySeconds = processingTimeDelaySeconds;
//    this.defaultSql = defaultSql;
//    this.inConverter = inConverter;
//    this.expressions = expressions;
//    this.expectedOutputDataPath = new File(expectedOutputDataPath).getAbsolutePath();
//    this.assigner = assigner;
//    this.keyByField = keyByField;
//    this.inTypeInfo = inTypeInfo;
//    this.tag = tag;
//  }
//
//  public void run() throws Exception {
//    String actualOutputDataPath = rootTestDir + "/actual-output-data";
//
//    try {
//      TestHelpers.deleteDir(actualOutputDataPath);
//    } catch (Exception e) {
//
//    }
//
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//    TestSourceConfig sourceConfig =
//        new TestSourceConfig(
//            testInputDataPath, processingTimeDelaySeconds, timeSpeedMultiplier, ",");
//
//    BroadcastEmbeddedFlinkFunction<KEY, IN> embeddedFlinkFunction =
//        new BroadcastEmbeddedFlinkFunction<>(
//            defaultSql, inTypeInfo, expressions, inConverter, assigner);
//
//    DataStream<IN> input =
//        env.addSource(new TestSource<>(sourceConfig, tag))
//            .returns(inTypeInfo)
//            .assignTimestampsAndWatermarks(assigner)
//            .name("input")
//            .setParallelism(1)
//            .keyBy(keyByField);
//
//    DataStream<SqlEvent> sqls =
//        env.readTextFile(testBroadcastDataPath)
//            .map(line -> (SqlEvent) new SqlEvent().apply(line, "\\|"))
//            .name("sqls")
//            .setParallelism(1);
//
//    MapStateDescriptor<String, SqlEvent> ruleStateDescriptor =
//        new MapStateDescriptor<>(
//            "RulesBroadcastState",
//            BasicTypeInfo.STRING_TYPE_INFO,
//            TypeInformation.of(new TypeHint<SqlEvent>() {}));
//
//    BroadcastStream<SqlEvent> sqlBroadcastStream = sqls.broadcast(ruleStateDescriptor);
//
//    DataStream<Tuple3<String, Boolean, Row>> output =
//        input.connect(sqlBroadcastStream).process(embeddedFlinkFunction).setParallelism(1);
//
//    output.print();
//
//    output.writeAsText(actualOutputDataPath);
//    env.execute();
//
//    TestHelpers.assertExpectedEqualsActual(expectedOutputDataPath, actualOutputDataPath, true);
//  }
//}
