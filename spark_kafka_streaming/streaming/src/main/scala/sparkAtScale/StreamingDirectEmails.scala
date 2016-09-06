package sparkKafkaDemo 

import java.util.UUID
import scala.sys
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.storage.StorageLevel
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.kafka.{KafkaUtils,OffsetRange,HasOffsetRanges}
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.joda.time.DateTime

/** This uses the Kafka Direct API introduced in Spark 1.4
  *
  */
object StreamingDirectEmails {

  def main(args: Array[String]) {

    if (args.length < 3) {
      println("1st paramteter is kafka broker ")
      println("2nd param whether to display debug output (true|false) ")
      println("3rd param is the checkpoint path  ")
      println("4th param is the maxRatePerPartition (records/sec to read from each kafka partition)  ")
      println("5th param is the batch interval in milliseconds")
      println("6th param is the auto.offset.reset type (smallest|largest)") 
      println("7th param is the topic name") 
      println("8th param is the type of kafka stream (direct|receiver)") 
      println("9th param is the number of partitions to consume per topic (used with receiver-based input stream)") 
      println("10th param is the amount of parallelism used for processing data (used with receiver-based input stream)") 
      println("11th param is the group.id that id's the consumer processes (used with receiver-based input stream)") 
      println("12th param is the zookeeper connect string (e.g. localhost:2181) (used with receiver-based input stream)") 
    }

    val brokers = args(0)
    val debugOutput = args(1).toBoolean
    val checkpoint_path = args(2)
    val maxRatePerPartition = args(3)
    val batchIntervalInMillis = args(4).toInt
    val offsetResetType = args(5)
    val topicName = args(6)
    val streamType = args(7)
    val numPartitions = args(8).toInt
    val processingParallelism = args(9).toInt
    val groupId = args(10)
    val zookeeper = args(11)
    val storageLevel = StorageLevel.MEMORY_AND_DISK_SER
    val conf = new SparkConf()
                 .set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
                 .set("spark.locality.wait", "0")
                 .set("spark.cassandra.connection.keep_alive_ms", (batchIntervalInMillis*5).toString)

    if (checkpoint_path == "dont_checkpoint") {
      conf.set("spark.streaming.receiver.writeAheadLog.enable", "false")
    } else {
      conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    }

    val sc = SparkContext.getOrCreate(conf)

    def createStreamingContext(): StreamingContext = {
      // Create a new StreamingContext
      val newSsc = new StreamingContext(sc, Milliseconds(batchIntervalInMillis))

      if (checkpoint_path == "dont_checkpoint") {
        println("dont_checkpoint was provided in checkpoint path, so we're not checkpointing.")
      } else {
        println(s"Creating new StreamingContext $newSsc with checkpoint path of: $checkpoint_path")
        newSsc.checkpoint(checkpoint_path)
      }
         
      // Setup Kafka params
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, 
                                            "auto.offset.reset"-> offsetResetType, 
                                            "group.id"->groupId,
                                            "zookeeper.connect"->zookeeper)

      println(s"connecting to brokers: $brokers")
      println(s"kafkaParams: $kafkaParams")

      // Create the input stream
      val emailsStream = {
        if (streamType == "direct") {
          val topics = Set(topicName)
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](newSsc, kafkaParams, topics)

        } else if (streamType == "receiver") {
          // Changing this number controls the number of consumer threads per input DStream
          val topics = Map(topicName -> 1) 
              
          // Controls the number of input dstreams
          val streams = (1 to numPartitions) map { _ => 
            KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](newSsc, kafkaParams, topics, storageLevel)
          }

          // Below is way to change parallelism for downstream processing, for now we'll stick with numPartitions
          val unifiedStream = newSsc.union(streams)
          unifiedStream.repartition(processingParallelism)

        } else {
          println(s"The streaming type provided is NOT supported: $streamType")
          sys.exit()
        }
      }
      
      emailsStream.foreachRDD {
        (message: RDD[(String, String)], batchTime: Time) => {
          var offsetRanges = Array[OffsetRange]()

          if (streamType == "direct") {
              offsetRanges = message.asInstanceOf[HasOffsetRanges].offsetRanges
              for (o <- offsetRanges) {
                 println(s"\nTopic: ${o.topic} Partition: ${o.partition} FromOffset: ${o.fromOffset} UntilOffset: ${o.untilOffset}")
              }
          }

          // Needs to be here: We have to create a SQLContext using the SparkContext that the StreamingContext is using.
          // We need to lazily instantiate a singelton instance of the SQLContext in order to recover from a checkpoint.
          val sqlContext = SQLContext.getOrCreate(message.sparkContext)
          import sqlContext.implicits._

          // Convert each RDD from the batch into a Email DataFrame
          // email data has the format msg_id:tenant_id:mailbox_id:time_delivered:time_forwarded:time_read:time_replied
          val df = message.map {
            case (key, nxtEmail) => nxtEmail.split("::")
          }.map(email => {
            val time_delivered: Long = email(3).trim.toLong
            val time_forwarded: Long = email(4).trim.toLong
            val time_read: Long = email(5).trim.toLong
            val time_replied: Long = email(6).trim.toLong
            Email(email(0).trim.toString, email(1).trim.toString, email(2).trim.toString, time_delivered, time_forwarded, time_read, time_replied)
          }).toDF("msg_id", "tenant_id", "mailbox_id", "time_delivered", "time_forwarded", "time_read", "time_replied")

          // Save the DataFrame to Cassandra
          // Note: Cassandra has been initialized through dse spark-submit, so we don't have to explicitly set the connection
          df.write.format("org.apache.spark.sql.cassandra")
            .mode(SaveMode.Append)
            .options(Map("keyspace" -> "email_db", "table" -> "email_msg_tracker"))
            .save()

          if (debugOutput) {
            val count = df.count()
            println(s"Successfully saved $count") 
            df.show()
          }
        }
      }
      newSsc
    }

    val ssc = StreamingContext.getActiveOrCreate(checkpoint_path, createStreamingContext)

    ssc.start()
    ssc.awaitTermination()
  }
}
