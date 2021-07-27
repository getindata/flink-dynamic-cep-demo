package com.ververica.field.dynamicrules.converters;

import com.ververica.field.dynamicrules.TransactionEvent;

public class TransactionStringConverter extends StringConverter<TransactionEvent> {
  @Override
  public TransactionEvent toValue(String input) {
    return TransactionEvent.fromString(input);
  }

  @Override
  public String toString(TransactionEvent input) {
    return String.join(
        ",",
        new String[] {
          String.valueOf(input.transactionId),
          String.valueOf(input.eventTime),
          String.valueOf(input.payeeId),
          String.valueOf(input.beneficiaryId),
          String.valueOf(input.paymentAmount),
          String.valueOf(input.paymentType),
          String.valueOf(input.ingestionTimestamp),
        });
  }
};
