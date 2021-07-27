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
```

##### Examles of CLI params:
--data-source kafka --rules-source kafka --alerts-sink kafka --rules-export-sink kafka

