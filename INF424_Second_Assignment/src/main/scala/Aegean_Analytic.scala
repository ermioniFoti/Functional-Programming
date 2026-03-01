import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Aegean_Analytic extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Aegean_Analytics")
    .getOrCreate()

  //Paths to our hdfs files
  val pathForData = "hdfs://localhost:9000/aegean_data/nmea_aegean.logs"
  //Our output path
  val outputPath1 = "hdfs://localhost:9000/aegean_data/output_files/output_files_Question1"
  val outputPath2 = "hdfs://localhost:9000/aegean_data/output_files/output_files_Question2"
  val outputPath3 = "hdfs://localhost:9000/aegean_data/output_files/output_files_Question3"
  val outputPath4 = "hdfs://localhost:9000/aegean_data/output_files/output_files_Question4"
  val outputPath5 = "hdfs://localhost:9000/aegean_data/output_files/output_files_Question5"
  val df = spark.read.option("header", true).csv(pathForData)

  //Here we sanitize our data, typecasting on columns that actually contain numeric values and filtering outliers (heading,sog > 360)
  val SanitizedDf = df.withColumn("speedoverground", col("speedoverground").cast(DoubleType))
    .withColumn("heading", col("heading").cast(DoubleType))
    .withColumn("courseoverground", col("courseoverground").cast(DoubleType))
    .filter(col("heading") <= 360)
    .filter(col("courseoverground") <= 360)

  //Question_1
  val numberOfTrackedVesselPositions =SanitizedDf
    .groupBy((dayofmonth(col("timestamp"))),col("station")) //Group by day and station
    .count().alias("Number of Tracked Vessels") //Count the number of tracked vessels for each day and station

  numberOfTrackedVesselPositions.show()//Print the result

  numberOfTrackedVesselPositions.rdd.saveAsTextFile(outputPath1)//Convert the dataFrame to an rdd and  save the results in the output path

  //Question_2
  val maximumTrackedPositions = SanitizedDf
    .groupBy(col("mmsi")) // Group by vesselID
    .count().withColumnRenamed("count", "Number of Tracked Vessels in Total") // Count the number of tracked positions for each vessel
    .orderBy(col("Number of Tracked Vessels in Total").desc) // Order by the number of tracked positions in descending order
    .limit(1) // Get the vessel with the most tracked positions

  maximumTrackedPositions.show()//Print the result

  maximumTrackedPositions.rdd.saveAsTextFile(outputPath2)//Convert the dataFrame to an rdd and  save the results in the output path

  //Question_3
  val station10003 = SanitizedDf
    .filter(col("station") === 10003).withColumnRenamed("speedoverground","speedoverground1") //Renaming needed so that we can later calculate the average SOG
    .select(dayofmonth(col("timestamp")).alias("day"),col("mmsi"),col("speedoverground1")) //Retrieving the day, vesselID and SOG for vessels tracked in station 10003

  val station8006 = SanitizedDf
    .filter(col("station") === 8006).withColumnRenamed("speedoverground","speedoverground2") //Renaming needed so that we can later calculate the average SOG
    .select(dayofmonth(col("timestamp")).alias("day"),col("mmsi"),col("speedoverground2")) //Retrieving the day, vesselID and SOG for vessels tracked in station 8006

  val joinedDF = station10003
    .join(station8006, Seq("day","mmsi"),"inner") //Inner join ensures that we only get the vessels (with the same ID) that were tracked in both stations on the same day
    .select("day","mmsi","speedoverground1","speedoverground2") //Selecting the SOG of the vessels tracked in both stations
    .agg(((avg("speedoverground1") + avg("speedoverground2")) / 2).alias("Average_SOG")) //Calculating the average SOG of the vessels tracked in both stations

  joinedDF.show()//Print the result

  joinedDF.rdd.saveAsTextFile(outputPath3)//Convert the dataFrame to an rdd and  save the results in the output path

  //Question_4
  val averageAbs = SanitizedDf
    .groupBy(col("station")) //Group by station
    .agg(avg(abs(col("heading")-col("courseoverground")))).alias("Average Abs") //Calculate the average absolute difference between heading and COG for each station

  averageAbs.show()//Print the result

  averageAbs.rdd.saveAsTextFile(outputPath4)//Convert the dataFrame to an rdd and  save the results in the output path

  //Question_5
  val mostFrequentStatuses = SanitizedDf
    .groupBy(col("status")) //Group by status
    .count().withColumnRenamed("count","Most Frequents Statuses")//Count the number of records for each status
    .orderBy(col("Most Frequents Statuses").desc) //Order by the number of records in descending order
    .limit(3) //Take the top 3

  mostFrequentStatuses.show()//Print the result

  mostFrequentStatuses.rdd.saveAsTextFile(outputPath5)//Convert the dataFrame to an rdd and  save the results in the output path

  spark.stop()
}