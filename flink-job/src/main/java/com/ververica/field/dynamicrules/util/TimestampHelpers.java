package com.ververica.field.dynamicrules.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class TimestampHelpers {

  static String defaultDatetimeFormat = "yyyy-MM-dd HH:mm:ss";


  static public Long toUnixtime(String datetime){
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(defaultDatetimeFormat);
    LocalDateTime localDateTime = LocalDateTime.parse(datetime, formatter);
    return localDateTime.toEpochSecond(ZoneOffset.UTC);
  }
}
