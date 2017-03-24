package org.anish.hackerearth.mastglobal.hive

import org.apache.spark.sql.SparkSession

/**
  * This class creates the hive tables and loads some initial data to begin with.
  *
  * Created by anish on 24/01/17.
  */
object HiveSetup {

  /**
    * Create a table with already existing data. Increments that arrive should get added (updated) to this data.
    * This should be run once to setup the metastore
    *
    * @param spark the SparkSession object
    */
  def loadAlreadyExistingData(spark: SparkSession): Unit = {
    val data = spark.read.option("header", "true").csv(Constants.pathOfAlreadyExistingData)
    val alreadyExistingData_df = data.toDF(data.columns.map(x => x.trim): _*)

    spark.catalog.setCurrentDatabase(Constants.hiveDatabaseName)


    spark.sql("CREATE EXTERNAL TABLE " + Constants.hiveDatabaseName + "." + Constants.hiveTableName +
      "( member_id int" +
      ",name string" +
      ",email string" +
      ",joined long" +
      ",ip_address string" +
      ",posts int" +
      ",bday_day int" +
      ",bday_month int" +
      ",bday_year int" +
      ",members_profile_views int" +
      ",referred_by int" +
      " ) STORED AS AVRO" +
      " LOCATION '" + Constants.hiveWareHouseLocation + "'")

    alreadyExistingData_df
      .write
      .format("com.databricks.spark.avro")
      .mode("overwrite")
      .saveAsTable(Constants.hiveTableName)
  }
}
