# State of the Data for Spark
A version of the state of the data tools
that is optimized for processing big data from a relational database using a 
spark cluster. This specific version of the tools is designed for use via 
Amazon Web Services (AWS) Elastic MapReduce (EMR) and contains the batch 
scripts to start up the EMR cluster, submit the state of the data scala 
jobs, write the results to a relational database, and shutdown the cluster.

## Table of Contents

* [Overview](#overview)
* [Batch File](#batch-file)
* [Steps.json](#steps-json)
* [Scala](#scala)
* [SQL](#sql)

## Overview
The files in this repo enable the user to run the 
state of the data tools using a Spark cluster. The tools are 
designed specifically for AWS EMR but can be modified
for use with any spark cluster. Below, we describe the 
different components of these tools.

## Batch File
The **emr_full_job_apps_only.bat** file runs the entire process.
This file is executed from the command line on any 
machine that has AWS console access. The user or deployer of 
this script has to have an EMR developer role assigned to him 
or her. 

The script starts by defining the databases that will be read from. 
```
SET db_sotd_tds=@@@db_latest_tds.db_name@@@
SET db_sotd_mgcp=@@@db_latest_mgcp.db_name@@@
```
These databases should be read-only connections. Any 
variable that is between triple @ signs, *@@@* needs to be assigned 
before the tools will work. So, for example, ```@@@db_latest_tds.db_name@@@```
needs to be replaced with the TDS database name.

After setting these variables, the script checks whether certain
tables exist in the database. These tables are needed to complete 
the analysis.

Next, the yarn and [scala files](#scala) are copied into AWS S3. For example, 
```tds_sotd_job.scala``` is the scala file that will process the 
TDS data.

Then, the EMR cluster is stood up using ```aws emr create-cluster```.
There are a lot of cluster parameters that need to be known. Make sure
that you know and understand these parameters. The yarn file is copied 
to the machine.

The [pre-SQL jobs](#sql) are run using ```psql```. These jobs create the 
output database tables and corresponding schemas.

Finally, the EMR steps are submitted to the cluster. These [EMR steps](#steps.json) 
will process state of the data metrics for both TDS and MGCP.

## Steps JSON
The steps JSON files are the steps that are submitted to the EMR cluster.
They are formatted as JSON because EMR translates the JSON into a command.
For example, to tell EMR to copy a file, you submit the following *step*.
```
  {
    "Type": "CUSTOM_JAR",
    "Name": "Copy SOTD TDS Scala",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": ["aws","s3","cp","@@@emr_configuration.s3_path_to_code@@@/tds_sotd_job.scala","///home/hadoop/tds_sotd_job.scala"]
  }
```
To tell EMR to run the scala file, you submit the following step.
```
  {
    "Type": "CUSTOM_JAR",
    "Name": "Run SOTD TDS Scala Job",
    "ActionOnFailure": "CONTINUE",
    "Jar": "command-runner.jar",
    "Args": ["spark-shell","--conf","spark.sql.autoBroadcastJoinThreshold=@@@emr_configuration.autoBroadcastJoinThreshold@@@","--conf","spark.sql.broadcastTimeout=@@@emr_configuration.broadcastTimeout@@@","--jars","@@@emr_configuration.s3_path_to_code@@@/postgresql-42.2.23.jar","-I","///home/hadoop/tds_sotd_job.scala"]
  }
```
These JSON files are the code the EMR will execute. 

## Scala
The scala files in the **inputs** folders run the process to 
create the state of the data metrics from the TDS and MGCP databases.
These tools perform the same function and create the same outputs as the 
Python tools, but use scala and Spark for the processing. They are
much faster than the Python tools and optimized for processing hundreds 
of millions of topographic features.

## SQL
The SQL files in the **inputs\sql** folder are used to set up the 
output databases and populate the output databases with default values.
The **pre-SQL** files create the databases and layout the schema. The
**post-SQL** files populate empty data tables with a default feature
so that ArcGIS recognizes that the database table is a valid 
feature class even though it doesn't have any real features.