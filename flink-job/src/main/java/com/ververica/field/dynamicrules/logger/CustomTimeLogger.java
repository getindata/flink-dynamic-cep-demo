package com.ververica.field.dynamicrules.logger;

import java.io.Serializable;

public class CustomTimeLogger implements Serializable {

  private long creationTime;

  public CustomTimeLogger(long creationTime) {
    this.creationTime = creationTime;
  }

  public void log(String message) {
    long currentTime = System.currentTimeMillis() - creationTime;
    System.out.println("[" + currentTime + "] - " + message);
  }
}
