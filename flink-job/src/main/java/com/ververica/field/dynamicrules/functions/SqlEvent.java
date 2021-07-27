package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.sources.TimeBasedEvent;
import com.ververica.field.dynamicrules.util.TimestampHelpers;

public class SqlEvent extends TimeBasedEvent {
  public String eventDate;
  public String sqlQuery;

  public SqlEvent() {
    this("1970-01-01 00:00:00", "");
  }

  public SqlEvent(String eventDate, String sqlQuery) {
    this.eventDate = eventDate;
    this.sqlQuery = sqlQuery;
  }

  @Override
  public Long getTimestamp() {
    return TimestampHelpers.toUnixtime(eventDate);
  }

  @Override
  public TimeBasedEvent apply(String inputLine, String delimiter) {
    String[] splitInput = inputLine.split(delimiter, 2);
    return new SqlEvent(splitInput[0], splitInput[1]);
  }
}
