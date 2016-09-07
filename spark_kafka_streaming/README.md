# Spark Streaming with Kafka Direct API Demo
 
This demo simulates a stream of email metadata.  This example assumes the user has an existing Kafka cluster with email data formatted as "**msg_id::tenant_id::mailbox_id::time_delivered::time_forwarded::time_read::time_replied**". 
It is assumed these fields have the following datatypes: 

* msg_id: String
* tenant_id: UUID
* mailbox_id: UUID
* time_delivered: Long
* time_forwarded: Long
* time_read: Long
* time_replied: Long

### Setup the KS/Table

**Note: You can change RF and compaction settings in this CQL script if needed.**

`cqlsh -f data_model/email_db.cql` 

### Run Spark Streaming

###### Build the streaming jar
`sbt streaming/assembly`

Parameters:

1. kafka broker: Ex. 10.200.185.103:9092 

2. debug flag (limited use): Ex. true or false 

3. checkpoint directory name: Ex. dsefs://[optional-ip-address]/emails_checkpoint

4. [spark.streaming.kafka.maxRatePerPartition](http://spark.apache.org/docs/latest/configuration.html#spark-streaming): Maximum rate (number of records per second) 

5. batch interval (ms) 

6. [auto.offset.reset](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.kafka.KafkaUtils$): Ex. smallest or largest

7. topic name 

8. kafka stream type: ex. direct or receiver

9. number of partitions to consume per topic (controls read parallelism) (receiver approach: you'll want to match whatever used when creating the topic) 

10. processesing parallelism (controls write parallelism) (receiver approach: you'll want to match whatever used when creating the topic) 

11. group.id that id's the consumer processes (receiver approach: you'll want to match whatever used when creating the topic) 

12. zookeeper connect string (e.g localhost:2181) (receiver approach: you'll want to match whatever used when creating the topic) 

###### Running on a server in foreground
`dse spark-submit --driver-memory 2G --class sparkKafkaDemo.StreamingDirectEmails streaming/target/scala-2.10/streaming-assembly-0.1.jar <kafka-broker-ip>:9092 true dsefs://[optional-ip-address]/emails_checkpoint 50000 5000 smallest emails direct 1 100 test-consumer-group localhost:2181`

## Support

The code, examples, and snippets provided in this repository are not "Supported Software" under any DataStax subscriptions or other agreements.

## License

Copyright 2016, DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.