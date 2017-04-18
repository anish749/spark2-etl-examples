# Transformations using Apache Spark 2.0.0
A project with examples of using few commonly used data manipulation/processing/transformation APIs in Apache Spark 2.0.0

### Tech Stack used:
**Framework**: Spark v2.0.0

**Programming Language**: Scala v2.11.6

### About the project
The project can be loaded in IntelliJ IDEA and the class  _org.anish.spark.etc.ProcessData_ can be directly run. This produces all the output.

### Code File descriptions
**org.anish.spark.etc.ProcessData.scala** : Main object along with all transformations and aggregations to process data. Running this object (tested in local system) should produce all the required results.
The input data has the following fields: 
```
member_id, name, email, joined, ip_address, posts, bday_day, bday_month, bday_year, members_profile_views, referred_by
```
A given output is saved in SampleOutput.txt
The output of the occurrence of IP address based on the first 3 octets group has been truncated at 500, to make it more presentable. The complete data frame is however saved in the hive tables.

Build with maven:
```
mvn clean install package
```
To run the main scala object:
Data (for testing) should be in _data/allData/_
```
java -jar target/spark2-etl-examples-1.0-SNAPSHOT-jar-with-dependencies.jar 
```

**org.anish.spark.etl.hive.Constants.scala** : Configurations stored as Strings in a class. Can be made configurable later.

**org.anish.spark.etl.hive.HiveSetup.scala** : Creates Hive tables and loads the initial data.

**org.anish.spark.etl.hive.LoadToHive.scala** : Do incremental loads to Hive. Also has a function to do update else insert option on the whole data set in a Hive table.

**org.anish.spark.etl.hive.DemoRunner.scala** : Run a demo of loading an initial data to Hive and then 1 increment to Hive. All sources are taken from appropriate folders in the data/* directory. This reqires to be run from an edge node with Hive and Spark clients running and connected to a Hive Meta Store and Spark server.


**org.anish.spark.etl.ProcessDataTest.scala** : Test class testing all utility methods defined in the ProcessData and LoadToHive Objects 

### Avro Outputs:
For analysis which gave a single or a list of numbers as output like most birth days day, least birthdays month, years with most signups, the output from the provided sample is in SampleOutput.txt along with data frames truncated at 500 records.

All queries which produced a dataset as output are saved as avro files in the folder _spark-warehouse/_. This can be recreated by executing _java -jar target/spark2-etl-examples-1.0-SNAPSHOT-jar-with-dependencies.jar_ 


### Running the project
1. Run _mvn clean install_ to build the project
2. Scala tests 
3. Build is successful
4. Run _java -jar target/spark2-etl-examples-1.0-SNAPSHOT-jar-with-dependencies.jar_ to produce analysis results. This also shows the following outputs:
    - Most birthdays are on: 1 day(s)                                                 
    - Least birthdays are on: 11 month(s)
5. Continuation of output:
    - Email providers with more than 10K 
    - Posts by email providers
    - Year(s) with max sign ups: 2015.
    - Class C IP address frequency by 1st octet
6. Continuation of output:
    - Frequency of IP address based on first 3 octets (truncated)
7. Continuation of output:
    - Number of referral by members

### Hive related Demo
For loading incremental data to hive tables:
This creates a table in hive with already existing data. Loads the data already present.

Increment Load: Loads an increment data, updating the fields which are already present based on member_id. Appends data which is not already present. (New members will be added. Data for old members will be updated.) For the sample data I have not partitioned and bucketed the data since, frequency of incomming increments, size and query pattern of data is not known.

This assumes that Hive metastore is up and running. Also HiveServer2 should be running and hive client jars present. This should ideally be run from an 'edge node' of a cluster. I've tested it in Spark Local, and not on cluster mode.
```
java -cp target/spark2-etl-examples-1.0-SNAPSHOT-jar-with-dependencies.jar org.anish.spark.etl.hive.DemoRunner
```


### Submitting to Spark Standalone
```
spark-submit --class org.anish.spark.etl.ProcessData --master local[4] \
--jars $(find '<***lib directory with spark jars***>' -name '*.jar' | xargs echo | tr ' ' ',') \
--packages com.databricks:spark-avro_2.11:3.1.0 \
spark2-etl-examples-1.0-SNAPSHOT.jar 
```

Currently the source is coded to take from local as _data/all_data/_
To read from HDFS, the path should be appropriately given. Eg - _hdfs://data/all_data/_
It would automatically take HDFS path if HDFS is running on the same node.

Submitting from "edge nodes" (Yarn Client Mode)
```
spark-submit --class org.anish.spark.etl.ProcessData --master yarn-client \
--jars $(find '<***lib directory with spark jars***>' -name '*.jar' | xargs echo | tr ' ' ',') \
--packages com.databricks:spark-avro_2.11:3.1.0 \
spark2-etl-examples-1.0-SNAPSHOT.jar
```

___