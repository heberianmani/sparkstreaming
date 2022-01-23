package org.inceptez.streaming
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._;

object FileStream1 {
  def main(args:Array[String])=
  {
    val spark=SparkSession.builder().appName("spark streaming").master("local[*]")
    .getOrCreate()
        
    val sc=spark.sparkContext
    val sqlc=spark.sqlContext
    sc.setLogLevel("Error")
    
    val ssc=new StreamingContext(sc,Seconds(10));
    //8.10.00 -> 8.10.10
    //8.10.11 -> 8.10.20
    //dstream -> discretized stream -> micro batch rdds of x seconds
    //val linesrdd=sc.textFile("file:///home/hduser/sparkdata/streaming/")
   
    val lines = ssc.textFileStream("file:///home/hduser/sparkdata/streaming/");
    //val lines =ssc.socketTextStream("localhost",9999)
   //dstream -> Dstream(1 RDD)

    import spark.implicits._;

    lines.foreachRDD(x=>
        {
         val  courses = x.flatMap(_.split(" "))
         val df1=courses.toDF("coursename")
         df1.createOrReplaceTempView("fileview")
         spark.sql("select coursename,count(1) from fileview group by coursename").show(50,false)
        }    
    )
    ssc.start();
    ssc.awaitTerminationOrTimeout(60000)
    
    
  }
}