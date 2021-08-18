# Dynamic Fraud Detection Demo with Apache Flink

## Introduction


### Instructions (local execution with netcat):

1. Start `netcat`:
```
nc -lk 9999
```
2. Run main method of `com.ververica.field.dynamicrules.Main`
3. Submit to netcat in correct format:
timestamp,SQL

##### Examples:

```
2021-06-25 10:38:30,SELECT payeeId FROM source_table WHERE paymentAmount > 10
2021-06-25 10:39:30,SELECT beneficiaryId FROM source_table WHERE paymentAmount > 10
2021-06-25 10:40:30,SELECT beneficiaryId FROM source_table WHERE paymentType = 'CSH'
2021-06-25 10:41:30,SELECT SUM(paymentAmount) FROM source_table WHERE paymentAmount < 10
2021-06-25 10:42:30,SELECT paymentType, MAX(paymentAmount) FROM source_table GROUP BY paymentType
2021-06-25 10:43:30,SELECT paymentType, MIN(paymentAmount) FROM source_table GROUP BY paymentType
2021-06-25 10:44:30,SELECT t.payeeId, t.first_payment, t.second_payment FROM source_table MATCH_RECOGNIZE ( PARTITION BY payeeId ORDER BY user_action_time MEASURES FIRST(paymentAmount) AS first_payment, LAST(paymentAmount) AS second_payment ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B) DEFINE A AS paymentAmount < 100, B AS paymentAmount > 100 ) AS t
2021-06-25 10:45:30,SELECT window_start, window_end, SUM(paymentAmount) FROM TUMBLE(TABLE source_table, DESCRIPTOR(eventTime), INTERVAL '10' SECONDS) WHERE paymentAmount > 10
2021-06-25 10:45:30,SELECT window_start, window_end, SUM(paymentAmount) FROM TABLE(TUMBLE(TABLE source_table, DESCRIPTOR(user_action_time), INTERVAL '10' SECONDS)) GROUP BY window_start, window_end
```

##### Examles of CLI params:
--data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka

