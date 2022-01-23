package org.inceptez.streaming
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object KafkaStream3 {
def main(args:Array[String])
{
val sparkConf = new SparkConf().setAppName("kafkastream").setMaster("local[*]")
val sparkcontext = new SparkContext(sparkConf)
sparkcontext.setLogLevel("ERROR")
val ssc = new StreamingContext(sparkcontext, Seconds(10))

val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "localhost:9092",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"group.id" -> "grp2",
"auto.offset.reset" -> "earliest"
)
val topics = Array("clickstream")

val stream = KafkaUtils.createDirectStream[String, String](ssc,
PreferConsistent, 
Subscribe[String, String](topics, kafkaParams))

stream.foreachRDD{
  x =>
     x.foreach(x=>println(x.value()))
     println("Printing the offset values")
    val offsetranges=x.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetranges.foreach(println)
    val rdd1=x.flatMap(x => x.value().split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    rdd1.foreach(println)
}

ssc.start()
ssc.awaitTermination()
}}
