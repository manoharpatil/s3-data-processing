package com.vigil

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}

class S3FileProcessingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val inputPath = "src/main/resources/input"
  private val outputPath = "src/main/resources/output"
  private val awsProfile = "default"

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder
      .appName("S3FileProcessingTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "S3FileProcessing" should "process input files and write output files correctly" in {
    val spark = SparkSession.builder
      .appName("S3FileProcessing")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // define test input data
    val inputData = Seq(
      ("foo", 1),
      ("bar", 2),
      ("foo", 3),
      ("baz", 4),
      ("baz", 5),
      ("qux", 6)
    )

    // write test input data to input files
    val inputPathCsv = Paths.get(inputPath, "input.csv")
    val inputPathTsv = Paths.get(inputPath, "input.tsv")
    Files.write(inputPathCsv, inputData.map { case (k, v) => s"$k,$v" }.mkString("\n").getBytes)
    Files.write(inputPathTsv, inputData.map { case (k, v) => s"$k\t$v" }.mkString("\n").getBytes)

    // define expected output data
    val expectedOutputData = Seq(
      ("bar", 2),
      ("baz", 9),
      ("foo", 4)
    )

    // run the S3FileProcessing app on the test input files
    S3FileProcessing.main(Array(inputPath, outputPath, awsProfile))

    // read the output data from the output files
    val outputData = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(s"$outputPath/part*")
      .as[(String, Int)]
      .collect()
      .sortBy(_._1)

    // check that the output data matches the expected output data
    outputData shouldEqual expectedOutputData

    // delete the test input and output files
    Files.delete(inputPathCsv)
    Files.delete(inputPathTsv)
    Files.delete(Paths.get(outputPath))
  }
}
