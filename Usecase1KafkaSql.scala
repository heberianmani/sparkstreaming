package org.inceptez.streaming
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._

// ES packages
import org.elasticsearch.spark.sql._

// Kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object Usecase1KafkaSql {
def main(args:Array[String])
{
val spark=SparkSession.builder().enableHiveSupport().appName("Usecase1 kafka to sql/nosql")
.master("local[*]")
.config("spark.history.fs.logDirectory", "file:///tmp/spark-events")
.config("spark.eventLog.dir", "file:////tmp/spark-events")
.config("spark.eventLog.enabled", "true") // 3 configs are for storing the events or the logs in some location, so the history can be visible
.config("hive.metastore.uris","thrift://localhost:9083") //hive --service metastore
.config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse") //hive table location
.config("spark.sql.shuffle.partitions",10)
.config("spark.es.nodes","localhost") 
.config("spark.es.port","9200")
.config("es.nodes.wan.only",true)
.enableHiveSupport().getOrCreate();

val hivestaticdf=spark.read.table("retail.txnrecords").select("txnno","custno","amount")

val ssc=new StreamingContext(spark.sparkContext,Seconds(1))

spark.sparkContext.setLogLevel("error")

val topics = Array("hvacsensorpart4rep3");

val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"group.id" -> "we37",
"auto.offset.reset" -> "latest"
,"enable.auto.commit" -> (false:java.lang.Boolean) 
// true - enable auto commit will make kafka to commit the offsets delivered when it read by a consumer automatically - no fault tolerance
// false - disable auto commit will make kafka to not to commit the offsets delivered when it read by a consumer automatically 
// rather the consumer will come back and commit the offset later after processing/writing data to the target- fault tolerance
)

val dstream = KafkaUtils.createDirectStream[String, String](ssc,
PreferConsistent, //preferbrokers or preferfixed
Subscribe[String, String](topics, kafkaParams))
//__consumer_offsets dont do the read time commit (1 -> 6 weekend37, 1 -> 6 bdweekend37) - exactly once semantics

// PreferConsistent / PreferBroker / PreferFixed
/*In most cases, you should use LocationStrategies.PreferConsistent. 
PreferConsistent - This will distribute partitions evenly across available executors started either in the broker or in the non broker nodes. 
drawback - we cant meet Data locality always.
PreferBrokers - will start the executors in the same node where kafka leader brokers of the respective partitions are running  
drawback - though we achive data locality, more number of executors are started by different spark streaming app, 
// we may get broker nodes crowded - BUT WE CAN GO FOR PREFERBROKERS if we want to get some millisecond RT app run.   
Finally, if you have a significant skew in load among partitions, use PreferFixed. 
This allows you to specify an explicit mapping of partitions in the brokers with the spark executors 
*/
// PreferConsistent -> Equal distribution across all executors -> exe1(JVM -CONTAINERS) (200) p0,p1, 2(200) p2,p3 
// PreferBroker -> 10 node cluster (7 nodes spark executors/container can run) -> 
// (3 nodes where kafka brokers (socket/hdd) running) -> 
// PreferFixed -> define your own location strategy (where you want to run the executors) - small clusters

import spark.sqlContext.implicits._
dstream.foreachRDD{
	rddfromdstream =>
	val offsetranges=rddfromdstream.asInstanceOf[HasOffsetRanges].offsetRanges //1->4 (3 messages)
	println("Printing Offsets")
	offsetranges.foreach(println)

	if(!rddfromdstream.isEmpty())
	{ // key,value
		// 1,{"custid": 4000001, "firstname": "Kristina", "city": "California", "transactamt": 230, "createdt": "2021-09-20", "updts": "2021-11-20 19:08:58.0"}
		val rdd1=rddfromdstream.map(x=>x.value())
		//val rdd1={"custid": 4000001, "firstname": "Kristina", "city": "California", "transactamt": 230, "createdt": "2021-09-20", "updts": "2021-11-20 19:08:58.0"}
	  val streamdf1=spark.read.json(rdd1).select("custid","city","firstname","transactamt")
		streamdf1.show
		val dfjoined=streamdf1.join(hivestaticdf, streamdf1("custid")===hivestaticdf("custno"),"inner").
		select("txnno","custid","city","firstname","amount","transactamt")
		dfjoined.show

		println("Writing to DB/NOSQL")
		println(java.time.LocalTime.now)

		val prop=new java.util.Properties();
		prop.put("user", "root")
		prop.put("password", "Root123$")
    
		dfjoined.createOrReplaceTempView("dfjoinedview")
		val aggrdf=spark.sql("""select sum(transactamt) as transactsum,city 
		                        from dfjoinedview 
		                        group by city""")
		                        
		aggrdf.write.mode("append").jdbc("jdbc:mysql://localhost/custdb","cust_trans_aggr_spark",prop)
		println("completed mysql write")
		println(java.time.LocalTime.now)

                   
    //writing into phoenix     
		// create the below table before writing the df output to phoenix
		// create table CUST_TRANS_SPARK_JOINED (txnno integer primary key,custid integer,city varchar(100),firstname varchar(100),amount double,transactamt double);
        
    println("Writing into HBase through phoenix")
    dfjoined.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "cust_trans_spark_joined").option("zkUrl", "localhost:2181").save()

    println("Writing to Elastic Search")    
    dfjoined.saveToEs("sparktxns/txncust",Map("es.mapping.id"->"txnno"))
    
		dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
		println(java.time.LocalTime.now)
		println("commiting offset")
	}
	else
	  println(java.time.LocalTime.now)
	  println("No data in the kafka topic for the given iteration")
}

ssc.start()
ssc.awaitTermination()
}}
