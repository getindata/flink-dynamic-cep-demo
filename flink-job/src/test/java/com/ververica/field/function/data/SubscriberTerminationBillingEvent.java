package com.ververica.field.function.data;

import com.ververica.field.dynamicrules.util.TimestampHelpers;
import com.ververica.field.dynamicrules.sources.TimeBasedEvent;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
public class SubscriberTerminationBillingEvent extends TimeBasedEvent implements Serializable {

  public String msisdn;
  public String eventDate;
  public String sourceEventType;
  public String sourceStatus;

  public SubscriberTerminationBillingEvent(String inputLine, String delimiter) {
    String[] splitInput = inputLine.split(delimiter, 4);

    msisdn = splitInput[0];
    eventDate = splitInput[1];
    sourceEventType = splitInput[2];
    sourceStatus = splitInput[3];
  }

  @Override
  public Long getTimestamp() {
    return TimestampHelpers.toUnixtime(eventDate);
  }

  @Override
  public TimeBasedEvent apply(String inputLine, String delimiter) {
    return new SubscriberTerminationBillingEvent(inputLine, delimiter);
  }

  @Override
  public int compare(TimeBasedEvent o1, TimeBasedEvent o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compareTo(TimeBasedEvent other) {
    if (other == null) return 1;
    else if (getTimestamp() > other.getTimestamp()) return 1;
    else if (getTimestamp() == other.getTimestamp()) return 0;
    else return -1;
  }
}
