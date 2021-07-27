package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.converters.StringConverter;
import com.ververica.field.dynamicrules.logger.CustomTimeLogger;
import com.ververica.field.dynamicrules.functions.SqlEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
// chcemy stworzyć operator na podstawie KeyedBroadcastProcessFunction,
// który będzie jednym wejściem przyjmował eventy, drugim kody SQL,
// a dla każdego klucza w stanie stworzymy local environment ,
// wewnątrz niego datastream, który jest uzupełniany kolejnymi eventami w wyniku funkcji
// processElement ,
// a kody SQL będą widokami nad tym datastreamem

/**
 * Function, that accepts patterns and routing instructions and executes them on NFA.
 *
 * @param <KEY>
 * @param <IN>
 */
public class BroadcastEmbeddedFlinkFunction<KEY, IN>
    extends KeyedBroadcastProcessFunction<KEY, IN, SqlEvent, Tuple4<String, Boolean, Row, Long>> {
  private static final AtomicInteger counter = new AtomicInteger(0);
  private static final AtomicInteger portCounter = new AtomicInteger(0);
  private StringConverter converterIn;
  private List<BroadcastEmbeddedFlinkCluster<IN>> clusters = new ArrayList<>();
  private TypeInformation<IN> inTypeInfo;
  private List<String> expressions;
  private AssignerWithPeriodicWatermarks<IN> assigner;

  private int subtaskIndex;

  private CustomTimeLogger customLogger;
  private long startTime;

  public BroadcastEmbeddedFlinkFunction(
      String defaultSql,
      TypeInformation<IN> inTypeInfo,
      List<String> expressions,
      Class converterIn,
      AssignerWithPeriodicWatermarks<IN> assigner) throws IllegalAccessException, InstantiationException {
    this.startTime = System.currentTimeMillis();
    this.customLogger = new CustomTimeLogger(startTime);

    this.clusters.add(
        new BroadcastEmbeddedFlinkCluster<>(
            defaultSql, inTypeInfo, expressions, converterIn, assigner, startTime));
    this.inTypeInfo = inTypeInfo;
    this.expressions = expressions;
    this.converterIn =  (StringConverter) converterIn.newInstance();
    this.assigner = assigner;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    clusters.get(0).open(generateSourcePort());
  }

  @Override
  public void close() throws Exception {
    for (BroadcastEmbeddedFlinkCluster<IN> cluster : clusters) cluster.close();
    super.close();
  }

  @Override
  public void processElement(
      IN value, ReadOnlyContext ctx, Collector<Tuple4<String, Boolean, Row, Long>> out) throws Exception {
    try {
      int valueNumber = counter.getAndIncrement();

      customLogger.log(
          "Processing value number "
              + valueNumber
              + " : ("
              + value.toString()
              + ") //// Subtask index: "
              + subtaskIndex);

      customLogger.log("Converter in: " + converterIn);
      String strValue = converterIn.toString(value);

      for (BroadcastEmbeddedFlinkCluster<IN> cluster : clusters) {
        cluster.write(strValue);
      }
      for (BroadcastEmbeddedFlinkCluster<IN> cluster : clusters) {
        List<Tuple4<String, Boolean, Row, Long>> output = cluster.retrieveResults();
        for (Tuple4<String, Boolean, Row, Long> line : output) {
          out.collect(line);
        }
      }
    } catch (Exception e) {
      customLogger.log("processElement exception: " + e.toString());
      throw e;
    }
  }

  @Override
  public void processBroadcastElement(
      SqlEvent value, Context ctx, Collector<Tuple4<String, Boolean, Row, Long>> out) throws Exception {

    BroadcastEmbeddedFlinkCluster<IN> cluster =
        new BroadcastEmbeddedFlinkCluster<IN>(
            value.sqlQuery, inTypeInfo, expressions, converterIn.getClass(), assigner, startTime);

    cluster.open(generateSourcePort());
    clusters.add(cluster);
  }

  private int generateSourcePort() {
    int valueNumber = portCounter.getAndIncrement();

    return 34100 + valueNumber;
  }
}
