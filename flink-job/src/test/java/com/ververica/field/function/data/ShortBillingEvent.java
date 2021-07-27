package com.ververica.field.function.data;

import com.ververica.field.dynamicrules.util.TimestampHelpers;
import com.ververica.field.dynamicrules.sources.TimeBasedEvent;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@NoArgsConstructor
@Data
public class ShortBillingEvent extends TimeBasedEvent implements Serializable {

  public String msisdn;
  public String eventDate;
  public Long balanceBefore;
  public Long balanceAfter;

  public ShortBillingEvent(String inputLine, String delimiter) {
    String[] splitInput = inputLine.split(delimiter, 4);

    msisdn = splitInput[0];
    eventDate = splitInput[1];
    balanceBefore = Long.parseLong(splitInput[2]);
    balanceAfter = Long.parseLong(splitInput[3]);
  }

  @Override
  public Long getTimestamp() {
    return TimestampHelpers.toUnixtime(eventDate);
  }

  @Override
  public TimeBasedEvent apply(String inputLine, String delimiter) {
    return new ShortBillingEvent(inputLine, delimiter);
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
