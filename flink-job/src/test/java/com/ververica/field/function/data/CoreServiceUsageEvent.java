package com.ververica.field.function.data;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class CoreServiceUsageEvent {

  String msisdn;
  Long eventDate;
  String firstOtherZone;
  String lastOtherZone;
  String firstServedZone;
  String lastServedZone;
  String eventType;
}
