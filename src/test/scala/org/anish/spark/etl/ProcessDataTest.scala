package org.anish.spark.etl

import java.io.File

import org.anish.hackerearth.mastglobal.ProcessData
import org.anish.hackerearth.mastglobal.hive.LoadToHive
import org.anish.spark.SparkTestUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by anish on 24/01/17.
  */
//@RunWith(classOf[JUnitRunner])
class ProcessDataTest extends FlatSpec with Matchers with BeforeAndAfter {
  var spark: SparkSession = _
  var source_data: DataFrame = _
  before {
    val sparkConf = new SparkConf
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[3]")
    }
    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName("MastGlobalDataProcessing-" + getClass.getName)
    }
    spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(SparkTestUtils.getResourcePath("/input_data/"))
    source_data = data.toDF(data.columns.map(x => x.trim): _*)
  }
  behavior of "Process Data"
  it should "getMostFrequentBday should return the most frequent day" in {
    val mostFreqBday = ProcessData.getMostFrequentBday(source_data)
    mostFreqBday shouldBe List(23, 24)
  }

  it should "getLeastFrequentBmonth should return the least frequent month" in {
    val mostFreqBday = ProcessData.getLeastFrequentBmonth(source_data)
    mostFreqBday shouldBe List(3)
  }

  it should "clean email address" in {
    val cleanEmails = ProcessData.cleanEmails(source_data)
    val tempPath = "tmp_unitTestTemp_" + System.currentTimeMillis()
    cleanEmails
      .select("corrected_email")
      .write.format("com.databricks.spark.avro")
      .save(tempPath)

    val actualData = spark.read.format("com.databricks.spark.avro").load(tempPath)
    val expectedData = spark.read.option("header", "true").csv(SparkTestUtils.getResourcePath("/expectedOutputs/cleanedEmails"))

    // Check if the two DF are equal
    SparkTestUtils.dfEquals(actualData, expectedData)
    // Check and make sure that the output was generated. And then delete it
    val errorFile = new File(tempPath)
    errorFile.exists() shouldBe true
    if (errorFile.isDirectory) {
      FileUtils.deleteDirectory(errorFile)
      errorFile.exists() shouldBe false
    }
  }

  it should "find the year with max signups" in {
    val yearWithMaxSignUps = ProcessData.yearWithMaxSignUps(source_data)
    yearWithMaxSignUps shouldBe List(2016)
  }

  it should "find the max referrals in given set" in {
    val maxReferralsActual = ProcessData.maxReferrals(source_data)
    val maxReferralsExpected = spark.createDataFrame(
      Seq(
        ("wyohl", 3L),
        ("Panda", 2L)
      )).toDF("referred_by_name", "no_of_people_referred")

    SparkTestUtils.dfEquals(maxReferralsActual, maxReferralsExpected)
  }

  it should "get post by provider" in {
    val postsByProviderActual = ProcessData.getPostsByProvider(ProcessData.cleanEmails(source_data))
    val postsByProviderExpected = spark.createDataFrame(
      Seq(
        ("Gmail", 14L),
        ("Hotmail", 12L),
        ("Yahoo", 0L)
      )).toDF("provider", "posts_count")

    SparkTestUtils.dfEquals(postsByProviderActual, postsByProviderExpected)
  }

  it should "list TLD with more than 10K members" in {
    val moreThan10KActual = ProcessData.findMoreThan10KTld(ProcessData.cleanEmails(source_data))
    val moreThan10KExpected = spark.createDataFrame(
      Seq(("", 0L))
    ).toDF("tld", "tld_count")
    SparkTestUtils.dfEquals(moreThan10KActual, moreThan10KExpected, onlySchema = true)
  }

  it should "List edu and gov email ids" in {
    val eduGovIdsActual = ProcessData.findEduGovEmailIds(ProcessData.cleanEmails(source_data))
    val eduGovIdsExpected = spark.createDataFrame(
      Seq(("", 0L))
    ).toDF("provider", "eduGov_Count")
    SparkTestUtils.dfEquals(eduGovIdsActual, eduGovIdsExpected, onlySchema = true)
  }

  it should "count occurence of class C IP" in {
    val classCipActual = ProcessData.classCipByFirstOctet(spark, source_data)
    val classCipExpected = spark.createDataFrame(
      Seq(("193.x.x.x", 1L))
    ).toDF("ipClassC", "count_octet1")
    SparkTestUtils.dfEquals(classCipActual, classCipExpected, onlySchema = true)
  }

  it should "count occurence of IP by first 3 octets" in {
    val ipBy3OctetsActual = ProcessData.ipAddressFreqBy3Octets(source_data)
    val ipBy3OctetsExpected = spark.createDataFrame(
      Seq(("67.71.23.x", 16L),
        ("95.14.204.x", 1L),
        ("108.209.248.x", 1L),
        ("95.14.204.x", 1L),
        ("190.230.223.x", 1L),
        ("95.14.204.x", 1L),
        ("193.92.228.x", 1L),
        ("187.180.176.x", 1L)
      )).toDF("octet13", "occurrence")
    SparkTestUtils.dfEquals(ipBy3OctetsActual, ipBy3OctetsExpected, onlySchema = true)
  }

  behavior of "LoadToHive class"
  it should "Do an upsert, i.e. update instead of only append when new data arrives" in {
    val oldData = spark.createDataFrame(
      Seq(
        ("1", "Data"),
        ("2", "AnotherData"),
        ("3", "JustAnotherOldData")
      )).toDF("id", "data")

    val newData = spark.createDataFrame(
      Seq(
        ("1", "UpdatedData"),
        ("4", "NewData"),
        ("5", "AnotherNewData")
      )).toDF("id", "data")

    val expectedMergedData = spark.createDataFrame(
      Seq(
        ("1", "UpdatedData"),
        ("2", "AnotherData"),
        ("3", "JustAnotherOldData"),
        ("4", "NewData"),
        ("5", "AnotherNewData")
      )).toDF("id", "data")

    val actualMergedData = LoadToHive.upsert(spark, oldData, newData, "id")

    SparkTestUtils.dfEquals(actualMergedData, expectedMergedData)
  }
}
