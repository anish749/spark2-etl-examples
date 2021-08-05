/*This is the demo commit*/
package org.anish.hackerearth.mastglobal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.joda.time.format.DateTimeFormat

/**
  * Class to process data to get findings
  *
  * Created by anish on 24/01/17.
  */
object ProcessData {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  /**
    * This function works with the data in local system as well.
    * It thus reads from a folder (which can be HDFS/S3 path as well)
    * This can be modified to read from Hive as well.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    val data_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/allData/")

    // Remove extra space in column header names and cache the source
    val source_df = data_df.toDF(data_df.columns.map(x => x.trim): _*)
      .cache


    val mostFrequentBday: List[Int] = getMostFrequentBday(source_df)
    println("Most birthdays are on: " + mostFrequentBday.mkString(",") + " day(s)")

    val leastFrequentBmonth: List[Int] = getLeastFrequentBmonth(source_df)
    println("Least birthdays are on: " + leastFrequentBmonth.mkString(",") + " month(s)")


    // Work with emails
    val emailCorrected_df: DataFrame = cleanEmails(source_df)
      .cache()

    val eduGovCount_df: Dataset[Row] = findEduGovEmailIds(emailCorrected_df)
    println("Email id from government and educational TLDs : ")
    eduGovCount_df.show(500, truncate = false)

    val moreThan10KTld: Dataset[Row] = findMoreThan10KTld(emailCorrected_df)
    println("Email providers with more than 10K : ")
    moreThan10KTld.show()

    val providerGrp_df: Dataset[Row] = getPostsByProvider(emailCorrected_df)
    println("Posts by email providers: ")
    providerGrp_df.show()

    emailCorrected_df.unpersist

    val year_maxJoined: List[Int] = yearWithMaxSignUps(source_df)
    // List because 2 years can experience exact same number of signups
    println("Year(s) with max sign ups: " + year_maxJoined.mkString(",") + ".")

    // Find class C ip address
    val classCip_df: DataFrame = classCipByFirstOctet(spark, source_df)
    println("Class C IP address frequency by 1st octet:")
    classCip_df.show(50)

    val ip_occurBy3octets: DataFrame = ipAddressFreqBy3Octets(source_df)
    println("Frequency of IP address based on first 3 octets")
    ip_occurBy3octets.show(500)

    val max_referral: DataFrame = maxReferrals(source_df)
    println("Number of referral by members: ")
    max_referral.show(500)

    // Save to Hive
    saveDFsToHive(eduGovCount_df, moreThan10KTld, providerGrp_df, classCip_df, ip_occurBy3octets, max_referral)

    // All done, now Unpersist the sourceDF
    source_df.unpersist
  }


  /**
    * Save output data to Hive tables.
    */
  def saveDFsToHive(eduGovCount_df: Dataset[Row], moreThan10KTld: Dataset[Row], providerGrp_df: Dataset[Row], classCip_df: DataFrame, ip_occurBy3octets: DataFrame, max_referral: DataFrame): Unit = {
    eduGovCount_df
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("eduGovCount_df")
    moreThan10KTld
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("moreThan10KTld")
    providerGrp_df
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("providerGrp_df")
    classCip_df
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("classCip_df")
    ip_occurBy3octets
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("ip_occurBy3octets")
    max_referral
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.avro")
      .saveAsTable("max_referral")
  }

  /**
    * Find ipAdress occurences by first 3 octets
    * @param source_df
    * @return
    */
  def ipAddressFreqBy3Octets(source_df: DataFrame): DataFrame = {
    val ip_occurBy3octets = source_df
      .select("ip_address")
      .withColumn("octet13", concat(substring_index(col("ip_address"), ".", 3), lit(".x")))
      .filter("octet13 <> '.x'")
      .groupBy("octet13")
      .agg(count("*").alias("occurrence"))
      .sort(desc("occurrence"))
      .toDF()
    ip_occurBy3octets
  }

  /**
    * Count hits from class C IP addresses
    * @param spark
    * @param source_df
    * @return
    */
  def classCipByFirstOctet(spark: SparkSession, source_df: DataFrame): DataFrame = {
    import spark.implicits._
    val classCip_df = source_df
      .select("ip_address")
      .withColumn("octet1", split(col("ip_address"), "\\.")(0))
      .filter($"octet1" >= 192 && $"octet1" <= 223)
      .withColumn("ipClassC", concat(col("octet1"), lit(".x.x.x")))
      .groupBy("ipClassC")
      .agg(count("*").alias("count_octet1"))
      .sort(desc("count_octet1"))
      .toDF()
    classCip_df
  }

  /**
    * Count total number of posts by Email Provider
    * @param emailCorrected_df
    * @return
    */
  def getPostsByProvider(emailCorrected_df: DataFrame): Dataset[Row] = {
    val providerGrp_df = emailCorrected_df
      .filter("provider in ('Gmail', 'Yahoo', 'Hotmail')")
      .groupBy("provider")
      .agg(sum("posts").cast(LongType).alias("posts_count"))
      .sort(desc("posts_count"))
    providerGrp_df
  }

  /**
    * Emails of TLDs of more than 10K occurences
    * @param emailCorrected_df
    * @return
    */
  def findMoreThan10KTld(emailCorrected_df: DataFrame): Dataset[Row] = {
    val moreThan10KTld = emailCorrected_df
      .groupBy("tld")
      .agg(count("*").alias("tld_count"))
      .filter("tld_count > 10000")
      .sort(desc("tld_count"))
    moreThan10KTld
  }

  /**
    * Count of members from edu and gov email ids
    * @param emailCorrected_df
    * @return
    */
  def findEduGovEmailIds(emailCorrected_df: DataFrame): Dataset[Row] = {
    val eduGovCount_df = emailCorrected_df
      .filter("tld like '%gov%' OR tld like '%edu%'")
      .filter("provider <> 'Others'") // Filters out edu.*.* OR edubs.ch-> This is not edu
      .groupBy("provider")
      .agg(count("*").alias("eduGov_Count"))
      .sort(desc("eduGov_Count"))
    eduGovCount_df
  }

  /**
    * Count of referrals by members
    * @param source_df
    * @return
    */
  def maxReferrals(source_df: DataFrame): DataFrame = {
    val rootFiltered_df = source_df
      .filter("referred_by <> 0 ")
      .select("member_id", "name", "referred_by")
    val referrers = source_df
      .select("member_id", "name")
      .withColumnRenamed("member_id", "referer_id")
      .withColumnRenamed("name", "referred_by_name")

    val refJoined_df = rootFiltered_df
      .join(referrers, rootFiltered_df("referred_by") === referrers("referer_id"))
      .drop("referer_id")

    val refferedGroups_df = refJoined_df
      .groupBy("referred_by_name")
      .agg(count("*").alias("no_of_people_referred"))
      .select("referred_by_name", "no_of_people_referred")
      .sort(desc("no_of_people_referred"))
      .toDF()

    refferedGroups_df
  }

  /**
    * Year(s) which had max signups
    * @param source_df
    * @return A List of Years which experienced the max signups
    */
  def yearWithMaxSignUps(source_df: DataFrame): List[Int] = {
    val sql_yearFromEpoch = udf((epoch: Long) => {
      DateTimeFormat.forPattern("YYYY").print(epoch * 1000)
    })

    val yrJoinedGrp_df = source_df
      .select("joined")
      .withColumn("year_joined", sql_yearFromEpoch(col("joined")).cast(IntegerType))
      .groupBy("year_joined")
      .agg(count("*").alias("year_joined_count"))
      .select("year_joined", "year_joined_count")
      .cache

    val maxJoined = yrJoinedGrp_df
      .agg(max("year_joined_count").alias("max_year_joined_count"))
      .collect
      .head.get(0).toString

    val year_maxJoined = yrJoinedGrp_df
      .filter("year_joined_count = " + maxJoined)
      .select("year_joined")
      .collect()

    val asScalaList = year_maxJoined.map(x => x.getAs[Int]("year_joined")).toList

    yrJoinedGrp_df.unpersist
    asScalaList
  }

  /**
    * Clean Junk email addresses
    * @param source_df
    * @return
    */
  def cleanEmails(source_df: DataFrame): sql.DataFrame = {
    val sql_emailCorrector = udf((email: String) => {
      email match {
        case r"(.*@)${id}.*@(.*)${dom}" => // Remove everything between multiple @
          id + dom
        case x =>
          if (x.contains("@") && x.split("@").length == 2) {
            val y = x.split("@")(1)
            if (!y.contains("."))
              "invalid"
            else
              x // has 1 @ and dots
          }
          else "invalid"
      }
    })

    val sql_tld = udf((email: String) => {
      if (email.contains("@"))
        email.split("@")(1)
      else
        email
    })

    val sql_provider = udf((tld: String) => {
      if (tld.contains("gmail"))
        "Gmail"
      else if (tld.contains("yahoo"))
        "Yahoo"
      else if (tld.contains("hotmail"))
        "Hotmail"
      else if (tld.endsWith(".edu"))
        ".edu"
      else if (tld.contains(".edu."))
        tld.substring(tld.indexOf(".edu."))
      else if (tld.endsWith(".gov"))
        ".gov"
      else if (tld.contains(".gov."))
        tld.substring(tld.indexOf(".gov."))
      else {
        "Others"
      }
    })

    val emailCorrected_df = source_df
      .select("email", "posts")
      .withColumn("corrected_email", sql_emailCorrector(col("email")))
      .filter("corrected_email <> 'invalid'")
      .withColumn("tld", sql_tld(col("corrected_email")))
      .withColumn("provider", sql_provider(col("tld")))

    emailCorrected_df
  }

  /**
    * Find the least frequent birth month among all users
    * @param source_df
    * @return
    */
  def getLeastFrequentBmonth(source_df: DataFrame): List[Int] = {
    // Find the least frequent birth month
    val bday_monthGrp = source_df
      .filter("bday_day >= 1 AND bday_day <= 31 AND bday_month >= 1 AND bday_month <= 12 AND bday_year > 0") // Filter out invalid values
      .groupBy("bday_month")
      .agg(count("*").alias("count_bday_month"))
      .select("bday_month", "count_bday_month")
      .cache

    val minCountBmonth = bday_monthGrp
      .agg(min("count_bday_month").alias("leastFrequentBmonth"))
      .collect
      .head.get(0).toString

    val leastFrequentBmonth = bday_monthGrp
      .filter("count_bday_month = " + minCountBmonth) // This is because there can be multiple dates having the same count
      .select("bday_month")
      .collect()

    val asScalaList = leastFrequentBmonth.map(x => x.getAs[Int]("bday_month")).toList

    bday_monthGrp.unpersist
    asScalaList
  }

  /**
    * Most frequent Birthdays of members
    * @param source_df
    * @return
    */
  def getMostFrequentBday(source_df: DataFrame): List[Int] = {
    // Find the most frequent birthday
    val bday_dayGrp = source_df
      .filter("bday_day >= 1 AND bday_day <= 31 AND bday_month >= 1 AND bday_month <= 12 AND bday_year > 0") // Filter out invalid values
      .groupBy("bday_day")
      .agg(count("*").alias("count_bday_day"))
      .select("bday_day", "count_bday_day")
      .cache

    val maxCountBday = bday_dayGrp
      .agg(max("count_bday_day").alias("mostFrequentBday"))
      .collect
      .head.get(0).toString

    val mostFrequentBday = bday_dayGrp
      .filter("count_bday_day = " + maxCountBday) // This is because there can be multiple dates having the same count
      .select("bday_day")
      .toDF()
      .collect()
    val asScalaList = mostFrequentBday.map(x => x.getAs[Int]("bday_day")).toList

    bday_dayGrp.unpersist
    asScalaList
  }

  /**
    * Returns the SparkSession object. Manages the configs for the spark session
    * @return
    */
  def getSparkSession: SparkSession = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[3]")
    }
    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName("MastGlobalDataProcessing-" + getClass.getName)
    }
    SparkSession
      .builder()
      .config(sparkConf)
      // .enableHiveSupport()
      .getOrCreate()
  }
}