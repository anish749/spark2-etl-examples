package org.anish.hackerearth.mastglobal.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * This class orchestrates the Hive demo and calls various classes.
  *
  * Created by anish on 24/01/17.
  */
object DemoRunner {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession

    println("Setting up a Hive Metastore and load the data there.")
    HiveSetup.loadAlreadyExistingData(spark)

    // check for files loaded
    spark.catalog.setCurrentDatabase(Constants.hiveDatabaseName)
    val loaded_data = spark.table(Constants.hiveTableName)
    println("Loaded : " + loaded_data.count + " record(s)")
    // Initial files have been loaded


    // Load incremental data now
    println("Loading incremental data from " + Constants.pathOfIncrementalData)
    LoadToHive.loadIncrement(spark)
    val afterIncrementLoad_df = spark.table(Constants.hiveTableName)
    println("Increment load complete. Total " + afterIncrementLoad_df.count + " record(s)")
  }

  /**
    * Get the Spark Session
    *
    * @return SparkSession object
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
      .getOrCreate()
  }
}
