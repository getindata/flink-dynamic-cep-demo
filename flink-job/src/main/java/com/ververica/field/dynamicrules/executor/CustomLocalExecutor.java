package com.ververica.field.dynamicrules.executor;

import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.local.ExecutionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import java.net.URL;
import java.util.List;

public class CustomLocalExecutor extends LocalExecutor {
  public CustomLocalExecutor(
      Environment defaultEnvironment,
      List<URL> dependencies,
      Configuration flinkConfig,
      CustomCommandLine commandLine,
      ClusterClientServiceLoader clusterClientServiceLoader) {
    super(defaultEnvironment, dependencies, flinkConfig, commandLine, clusterClientServiceLoader);
  }

  public ExecutionContext<?> getExecutionContext(String sessionId) {
    return super.getExecutionContext(sessionId);
  }
}
