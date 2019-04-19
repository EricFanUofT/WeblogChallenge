/**
  * Created by EricF on 2019-04-18.
  */
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WebLogChallenge {
  def main(args: Array[String]):Unit = {
    val conf = new SparkConf()
    conf.set("spark.master","local[4]")
    val spark = SparkSession.builder().appName("PaytmWebLogChallenge").config(conf).getOrCreate()

    val inputFile="C:\\Users\\EricF\\Documents\\Scala\\untitled\\input\\2015_07_22_mktplace_shop_web_log_sample.log"

    //logs delimited by space except when enclosed by quotations
    var df = spark.read.format("csv").option("delimiter","\"").load(inputFile)

    //for this challenge, we only need the timestamp,client ip address, and url fields
    df = df.withColumn("timestamp", split(split(col("_c0"),"\\s+")(0),"\\.")(0))
      .withColumn("ip", split(split(col("_c0"),"\\s+")(2),":")(0))
      .withColumn("url", split(col("_c1"),"\\s+")(1))
      .selectExpr("timestamp","ip","url",
        "unix_timestamp(regexp_replace(timestamp,'T','_'),'yyyy-MM-dd_HH:mm:ss') as unix_time")

    //Q1:  sessionize log
    df = reconstructSession(df, 45)
    df.createOrReplaceTempView("df")

    val df_result = spark.sql("SELECT ip, session_number, max(unix_time)-min(unix_time) session_time_s," +
      "collect_set(url) url_visited, size(collect_set(url)) num_url_visited FROM df GROUP BY ip,session_number")

    //Q2:  average session time
    df_result.groupBy("ip").agg(avg(col("session_time_s"))).show(false) //per visitor
    df_result.agg(avg(col("session_time_s"))).show(false)               //overall average from the current log

    //Q3:  unique url visited per session
    df_result.selectExpr("concat(ip,'_',session_number) as session_id",
      "num_url_visited","url_visited").show(false)
    //average number of url visited per session
    df_result.agg(avg(col("num_url_visited"))).show(false)
    //maximum number of url visited per session
    df_result.agg(max(col("num_url_visited"))).show(false)

    //Q4:  most engaged user
    df_result.orderBy(desc("session_time_s")).show(1,false)

  }


  //A function that reconstucts the sessions from the log file
  //A new session is identified when consecutive logs are further than 'threshold' minutes apart for the same user ip
  //Each session is represented by a combination of 'user ip' and 'session number'
  def reconstructSession(logDF: DataFrame, threshold:Int)={
    val windowSpec = Window.partitionBy("ip").orderBy("timestamp")
    var sessionDF = logDF.withColumn("lag_unix_time",lag(col("unix_time"),1).over(windowSpec))
      .withColumn("diff_session", when((col("unix_time")-col("lag_unix_time")).gt(threshold * 60), 1).otherwise(0))
      .withColumn("session_number", sum(col("diff_session")).over(windowSpec))
    sessionDF
  }
}
