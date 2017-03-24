package org.anish.hackerearth.mastglobal.hive

/**
  * Contains configs requred by the Hive component.
  *
  * Created by anish on 24/01/17.
  */
object Constants {
  val pathOfAlreadyExistingData = "data/alreadyExistingData"
  val pathOfIncrementalData = "data/newIncrement"
  val hiveDatabaseName = "default"
  val hiveTableName = "member_details"
  val hiveWareHouseLocation = System.getProperty("user.dir") + "/warehouse/member_details/"
}
