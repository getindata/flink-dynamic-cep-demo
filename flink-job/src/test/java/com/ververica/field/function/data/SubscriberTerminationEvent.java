package com.ververica.field.function.data;

import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class SubscriberTerminationEvent {

  String msisdn;
  String eventDate;
  String terminationType;
}
