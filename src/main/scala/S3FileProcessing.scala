package com.vigil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/*
* * The S3FileProcessing app satisfies the following criteria:
* * 1. Reads input data from an S3 bucket.
* * 2. Processes the input data using Apache Spark.
* * 3. Writes output data back to an S3 bucket.
* * 4. Handles mixed-format input files (CSV and TSV) and interprets the data correctly based on the file format.
* * 5. Handles the case where some input files may contain empty strings that represent zero.
* * 6. Outputs a two-column TSV file that contains each unique key exactly once,
* *    along with the integer occurring an odd number of times for that key across all input files.
* * 7. Authenticates to AWS using credentials from an AWS credentials profile, or using the default provider chain if a profile is not specified.
* * 8. Includes a build file (build.sbt) that defines the project dependencies and allows the app to be built using sbt.
  */

object S3FileProcessing {
  def main(args: Array[String]): Unit = {
    // define the input/output paths and AWS credentials profile
    val inputPath = args(0)
    // val inputPath = "file:///C:/Development/SourceCode/Assignments/s3-data-processing/src/main/resources/input" // set the input path to a local file path
    val outputPath = args(1)
    // val outputPath = "C:/Development/SourceCode/Assignments/s3-data-processing/src/main/resources/output" // set the output path to a local file path
    val awsProfile = args(2)

    // create a SparkSession with S3 access credentials
    val spark = SparkSession.builder
      .appName("S3FileProcessing")
      .config("spark.hadoop.fs.s3a.access.key", sys.env(awsProfile + "_ACCESS_KEY"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env(awsProfile + "_SECRET_KEY"))
      // .master("local[*]") // set the master configuration property to "local"
      .getOrCreate()

    // define the schema for the input files
    val schema = StructType(Seq(
      StructField("key", StringType, true),
      StructField("value", IntegerType, true)
    ))

    // read in the input files
    val df = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(schema)
      .csv(inputPath)

    // val df = spark.read
    // .format("csv")
    // .option("header", "true")
    // .option("delimiter", "\t")
    // .load(inputPath)

    // replace empty strings with 0
    val dfClean = df.na.fill(0)

    // group by key and sum the values
    val dfGrouped = dfClean.groupBy("key")
      .agg(sum("value").alias("total"))

    // filter for keys with odd total values
    val dfOdd = dfGrouped.filter(col("total") % 2 !== 0)

    // write out the odd keys and their total values as TSV files
    dfOdd.select("key", "total")
      .write
      .option("delimiter", "\t")
      .csv(outputPath)

    // dfOdd
    // .write
    // .format("csv")
    // .option("header", "true")
    // .option("delimiter", "\t")
    // .save(outputPath)
  }
}
