package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.converters.StringConverter;
import com.ververica.field.dynamicrules.executor.CustomLocalExecutor;
import com.ververica.field.dynamicrules.logger.CustomTimeLogger;
import com.ververica.field.dynamicrules.sources.CustomSocketTextStreamFunction;
import com.ververica.field.dynamicrules.util.SchemaHelper;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.Socket;
import java.util.*;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

public class BroadcastEmbeddedFlinkCluster<IN> implements Serializable {
  private static final int NUM_TMS = 1;
  private static final int NUM_SLOTS_PER_TM = 1;
  private static StringConverter converterIn;
  private TypeInformation<IN> inTypeInfo;
  private CustomLocalExecutor executor;
  private String sessionId;
  @Getter @Setter private String sql;
  private DataStreamSource<String> inputSource;
  private List<String> expressions;
  private StreamTableEnvironment tableEnv;
  private AssignerWithPeriodicWatermarks<IN> assigner;
  private ResultDescriptor resultDescriptor;
  private Socket clientSocket;
  private OutputStreamWriter writer;
  private MiniClusterWithClientResource miniClusterResource;
  private CustomTimeLogger customLogger;
  private Class converterInClass;

  public BroadcastEmbeddedFlinkCluster(
      String sql,
      TypeInformation<IN> inTypeInfo,
      List<String> expressions,
      Class converterInClass,
      AssignerWithPeriodicWatermarks<IN> assigner,
      long startTime)
      throws IllegalAccessException, InstantiationException {
    this.customLogger = new CustomTimeLogger(startTime);

    this.sql = sql;
    this.inTypeInfo = inTypeInfo;
    this.expressions = expressions;
    this.converterInClass = converterInClass;
    BroadcastEmbeddedFlinkCluster.converterIn = (StringConverter) converterInClass.newInstance();
    this.assigner = assigner;
  }

  public void open(int dsSourcePort) throws Exception {
    customLogger.log("Opening new cluster - communicating on port " + dsSourcePort);
    miniClusterResource =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(getConfig())
                .setNumberTaskManagers(NUM_TMS)
                .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                .build());

    miniClusterResource.before();
    ClusterClient<?> clusterClient = miniClusterResource.getClusterClient();

    executor = createDefaultExecutor(clusterClient);

    SessionContext sessionContext = new SessionContext("default", new Environment());
    sessionId = executor.openSession(sessionContext);

    Runtime.getRuntime().addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));

    StreamExecutionEnvironment keyEnv =
        (StreamExecutionEnvironment)
            FieldUtils.readField(executor.getExecutionContext(sessionId), "streamExecEnv", true);
    tableEnv =
        (StreamTableEnvironment) executor.getExecutionContext(sessionId).getTableEnvironment();

    String dsSourceHostName = "localhost";

    inputSource =
        keyEnv.addSource(
            new CustomSocketTextStreamFunction(
                dsSourceHostName, dsSourcePort, "\n", 0, customLogger),
            "My Socket Stream");
    clientSocket = new Socket(dsSourceHostName, dsSourcePort);
    customLogger.log("Client socket port" + clientSocket.getLocalPort());
    customLogger.log("Client socket channel" + clientSocket.getChannel());
    customLogger.log("Client socket inetAddress" + clientSocket.getInetAddress());
    customLogger.log("Client socket toString" + clientSocket.toString());
    writer = new OutputStreamWriter(clientSocket.getOutputStream());

    customLogger.log("Converter in: " + converterIn);
    if (converterIn == null) {
      BroadcastEmbeddedFlinkCluster.converterIn = (StringConverter) converterInClass.newInstance();
      customLogger.log("Fixed converter in: " + converterIn);
    }
    DataStream<IN> inputDS =
        inputSource
            .map((MapFunction<String, IN>) s -> (IN) converterIn.toValue(s))
            .returns(inTypeInfo)
            .assignTimestampsAndWatermarks(assigner);

    Expression[] defaultExpressions = {$("user_action_time").rowtime()};

    Table inputTable =
        tableEnv.fromDataStream(
            inputDS,
            Stream.concat(
                    expressions.stream().map(Expressions::$), Arrays.stream(defaultExpressions))
                .toArray(Expression[]::new));

    tableEnv.createTemporaryView("source_table", inputTable);
    customLogger.log("Executing SQL :" + sql);
    resultDescriptor = executor.executeQuery(sessionId, sql);
  }

  public void close() {
    executor.closeSession(sessionId);
    miniClusterResource.after();
  }

  public void write(String value) throws IOException {
    customLogger.log("Writing! Client socket port" + clientSocket.getLocalPort());
    customLogger.log("Writing! Client socket channel" + clientSocket.getChannel());
    customLogger.log("Writing! Client socket inetAddress" + clientSocket.getInetAddress());
    customLogger.log("Writing! Client socket toString" + clientSocket.toString());
    writer.write(value + "\n");
    writer.flush();
  }

  public List<Tuple4<String, Boolean, Row, Long>> retrieveResults() throws InterruptedException {
    //        customLogger.log("Retrieving results from SQL: " + sql);
    List<Tuple2<Boolean, Row>> result =
        cyclicRetrieveChangelogResult(executor, sessionId, resultDescriptor.getResultId(), 1);
    List<Tuple4<String, Boolean, Row, Long>> output = new ArrayList<>();
    for (Tuple2<Boolean, Row> line : result)
      output.add(new Tuple4<>(sql, line.f0, line.f1, System.currentTimeMillis()));
    return output;
  }

  private Configuration getConfig() {
    Configuration config = new Configuration();
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4m"));
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);

    return config;
  }

  private <T> CustomLocalExecutor createDefaultExecutor(ClusterClient<T> clusterClient)
      throws Exception {

    final Map<String, String> replaceVars = new HashMap<>();
    replaceVars.put("$VAR_PLANNER", "old");
    replaceVars.put("$VAR_EXECUTION_TYPE", "streaming");
    replaceVars.put("$VAR_RESULT_MODE", "changelog");
    replaceVars.put("$VAR_UPDATE_MODE", "update-mode: append");
    replaceVars.put("$VAR_MAX_ROWS", "100");
    replaceVars.put("$VAR_RESTART_STRATEGY_TYPE", "failure-rate");
    return createModifiedExecutor(clusterClient, replaceVars);
  }

  private <T> CustomLocalExecutor createModifiedExecutor(
      ClusterClient<T> clusterClient, Map<String, String> replaceVars) throws Exception {
    return new CustomLocalExecutor(
        EnvironmentFileUtil.parseModified(replaceVars),
        Collections.emptyList(),
        clusterClient.getFlinkConfiguration(),
        new DefaultCLI(clusterClient.getFlinkConfiguration()),
        new DefaultClusterClientServiceLoader());
  }

  private List<Tuple2<Boolean, Row>> cyclicRetrieveChangelogResult(
      LocalExecutor executor, String sessionId, String resultID, int iterations)
      throws InterruptedException {
    int iterCount = 0;
    final List<Tuple2<Boolean, Row>> actualResults = new ArrayList<>();
    try {
      while (iterCount < iterations) {
        Thread.sleep(100); // slow the processing down
        final TypedResult<List<Tuple2<Boolean, Row>>> result =
            executor.retrieveResultChanges(sessionId, resultID);
        if (result.getType() == TypedResult.ResultType.PAYLOAD) {
          actualResults.addAll(result.getPayload());
        } else if (result.getType() == TypedResult.ResultType.EOS) {
          //                    customLogger.log("EOS!!!");
          break;
        }
        iterCount++;
      }
    } catch (Exception e) {
      customLogger.log("CYCLIC EXCEPTION!!! " + e.toString());
      throw e;
    }
    return actualResults;
  }

  private static class EmbeddedShutdownThread extends Thread {

    private final String sessionId;
    private final Executor executor;

    public EmbeddedShutdownThread(String sessionId, Executor executor) {
      this.sessionId = sessionId;
      this.executor = executor;
    }

    @Override
    public void run() {
      // Shutdown the executor
      System.out.println("\nShutting down the session...");
      executor.closeSession(sessionId);
      System.out.println("done.");
    }
  }

  public static class EnvironmentFileUtil {

    public static Environment parseModified(Map<String, String> replaceVars) throws IOException {
      String schema = SchemaHelper.getSchemaContents();

      for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
        schema = schema.replace(replaceVar.getKey(), replaceVar.getValue());
      }

      return Environment.parse(schema);
    }
  }
}
