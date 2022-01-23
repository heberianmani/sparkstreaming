package org.inceptez.streaming
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.sql.types._
// ES packages
import org.elasticsearch.spark.sql._

// Kafka packages
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
// HTTPS Rest API -> Nifi -> Kafka -> Spark Streaming (JSON) -> Elastic Search -> Kibana Dashboard

object Usecase2HttpAPIKafkaSparkESKibana {
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

		val ssc=new StreamingContext(spark.sparkContext,Seconds(1))

				spark.sparkContext.setLogLevel("error")


		val weblogschema = StructType(Array(
						StructField("username", StringType, true),
						StructField("ip", StringType, false),
						StructField("dt", StringType, true),
						StructField("day", StringType, true),    
						StructField("month", StringType, true),
						StructField("time1", StringType, true),
						StructField("yr", StringType, true),
						StructField("hr", StringType, true),
						StructField("mt", StringType, true),
						StructField("sec", StringType, true),
						StructField("tz", StringType, true),
						StructField("verb", StringType, true),
						StructField("page", StringType, true),
						StructField("index", StringType, true),
						StructField("fullpage", StringType, true),
						StructField("referrer", StringType, true),
						StructField("referrer2", StringType, true),
						StructField("statuscd", StringType, true)));  


		val topics = Array("hvacsensor1");

		val kafkaParams = Map[String, Object](
				"bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
				"key.deserializer" -> classOf[StringDeserializer],
				"value.deserializer" -> classOf[StringDeserializer],
				"group.id" -> "we37",
				"auto.offset.reset" -> "latest"
				,"enable.auto.commit" -> (false:java.lang.Boolean) )

				val dstream = KafkaUtils.createDirectStream[String, String](ssc,
						PreferConsistent, 
						Subscribe[String, String](topics, kafkaParams))

try {
				import spark.sqlContext.implicits._
				dstream.foreachRDD{
			rddfromdstream =>
			val offsetranges=rddfromdstream.asInstanceOf[HasOffsetRanges].offsetRanges
			println("Printing Offsets")
			offsetranges.foreach(println)
			
			if(!rddfromdstream.isEmpty())
			{
			  
      			val jsonrdd=rddfromdstream.map(x=>x.value())  
						val jsondf =spark.read.option("multiline", "true").option("mode", "DROPMALFORMED").json(jsonrdd)
						
																  
								  //val userwithid= jsondf.withColumn("results",explode($"results")).select("results[0].username")
								  jsondf.printSchema();
								  jsondf.createOrReplaceTempView("apiview")
								  //jsondf.show(5,false)
//spark.sql("select * from apiview").show(10,false)

println("Raw SQL")
spark.sql("""select 
  explode(results) as res,
  info.page as page,res.cell as cell,
  res.name.first as first,
  res.dob.age as age,
  res.email as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
  res.location.coordinates.longitude as longitude,res.location.country as country,
  res.location.state as state,
  res.location.timezone as timezone,res.login.username as username
 from apiview  """).show(10,false)								  

println("Data to write into Elastic search for analysis") 
val esdf=spark.sql("""
  select username as custid,page,cell,first,age,email,uscity,latitude,longitude,country,state 
  from (select 
  explode(results) as res,
  info.page as page,res.cell as cell,
  res.name.first as first,
  res.dob.age as age,
  res.email as email,res.location.city as uscity,res.location.coordinates.latitude as latitude,
  res.location.coordinates.longitude as longitude,res.location.country as country,
  res.location.state as state,
  res.location.timezone as timezone,res.login.username as username
 from apiview  ) as temp1
  """)
  esdf.show(5,false)
//where info.page is not null
				println("Writing to Elastic Search")    
				esdf.saveToEs("sparkjson/custvisit",Map("es.mapping.id"->"custid"))

				dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetranges)
				println(java.time.LocalTime.now)
				println("commiting offset")
			}
			else
				{println(java.time.LocalTime.now)
				println("No data in the kafka topic for the given iteration")
				}
		
			}
			}
			
					catch {
						case ex1: java.lang.IllegalArgumentException => {
							println("Illegal arg exception")
						}

						case ex2: java.lang.ArrayIndexOutOfBoundsException => {
							println("Array index out of bound")
						}

						case ex3: org.apache.spark.SparkException => {
							println("Spark common exception")}
						
            case ex4: java.lang.NullPointerException => {
							println("Values Ignored")}
						    }
					
		ssc.start()
		ssc.awaitTermination()
	}}
