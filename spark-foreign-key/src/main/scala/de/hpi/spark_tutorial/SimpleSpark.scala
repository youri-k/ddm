package de.hpi.spark_tutorial

import java.io.File

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    //------------------------------------------------------------------------------------------------------------------
    // Handle input parameter
    //------------------------------------------------------------------------------------------------------------------

    val path = if(args.indexOf("--path") == -1) "./TPCH" else args(args.indexOf("--path") + 1)
    val numCores = if(args.indexOf("--cores") == -1) "4" else args(args.indexOf("--cores") + 1)
    val numShufflePartitions = (Integer.valueOf(numCores) * 2).toString

    println("Path: " + path)
    println("Number of cores: " + numCores)

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master(s"local[$numCores]") // local, with numCores worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", numShufflePartitions) //

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Inclusion Dependency Discovery (Homework)
    //------------------------------------------------------------------------------------------------------------------

    time {Sindy.discoverINDs(getFileNames(path), spark)}
  }

  def getFileNames(path: String): List[String] ={
    val dir = new File(path)
      assert(dir.exists() && dir.isDirectory, s"$dir does not exist or is not a directory")
      dir.listFiles().filter(_.isFile).map(_.toString).toList
  }
}
