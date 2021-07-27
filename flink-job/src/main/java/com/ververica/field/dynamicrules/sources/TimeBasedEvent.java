package com.ververica.field.dynamicrules.sources;

import org.apache.flink.cep.EventComparator;

import java.io.Serializable;

public abstract class TimeBasedEvent  implements Comparable<TimeBasedEvent>, EventComparator<TimeBasedEvent>, Serializable {
  public abstract Long getTimestamp();

  public abstract TimeBasedEvent apply(String inputLine, String delimiter); // = ","

  @Override
  public int compareTo(TimeBasedEvent other) {
        if(other == null) return  1;
        return getTimestamp().compareTo(other.getTimestamp());
  }
    @Override
    public int compare(TimeBasedEvent o1, TimeBasedEvent o2){
      return o1.compareTo(o2);
  }

  public static <T extends TimeBasedEvent> T createFromCsv(String inputLine, Class<T> type) throws IllegalAccessException, InstantiationException {
         return (T) type.newInstance().apply(inputLine, ",");
    }

}