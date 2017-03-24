package org.anish.hackerearth.mastglobal.hive

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Class for loading increment data to Hive tables.
  * This also updates old data while an increment is being loaded.
  *
  * Created by anish on 24/01/17.
  */
object LoadToHive {

  /**
    * This function performs an incremental update to data that is already present
    *
    * @param spark
    */
  def loadIncrement(spark: SparkSession): Unit = {
    // Create a DF out of the increment
    val increment_data = spark.read.option("header", "true").csv(Constants.pathOfIncrementalData)

    // Update the already existing data with the increment data received
    spark.catalog.setCurrentDatabase(Constants.hiveDatabaseName)
    val masterData_df = spark.table(Constants.hiveTableName)

    // Do an upsert - Updates old data with new data, and addes new data if it is not existing.
    // Member_id is used as unique key
    val upsert_df: DataFrame = upsert(spark, masterData_df, increment_data, "member_id")

    // Write upserted data to the same table (overwritten)
    upsert_df
      .write
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .saveAsTable(Constants.hiveTableName)
  }

  /**
    * Update a table with an increment data coming it. It does an update else inserts.
    * @param spark
    * @param masterData_df
    * @param increment_data
    * @param uniqueKey
    * @return
    */
  def upsert(spark: SparkSession, masterData_df: DataFrame, increment_data: DataFrame, uniqueKey: String): DataFrame = {
    import spark.implicits._
    val columns = masterData_df.columns
    val increment_df = increment_data.toDF(increment_data.columns.map(x => x.trim + "_i"): _*)
    val joined_df = masterData_df.as("m").join(increment_df.as("i"), $"m.$uniqueKey" === $"i.${uniqueKey}_i", "outer")
    val upsert_df = columns.foldLeft(joined_df) {
      (acc: DataFrame, colName: String) =>
        acc.withColumn(colName + "_j", coalesce(col(colName + "_i"), col(colName)))
          .drop(colName)
          .drop(colName + "_i")
          .withColumnRenamed(colName + "_j", colName)
    }
    upsert_df
  }
}
