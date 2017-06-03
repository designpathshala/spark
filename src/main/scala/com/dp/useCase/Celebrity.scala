package com.dp.useCase

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapred.FileAlreadyExistsException

/**
 * @author miraj
 */
object Celebrity {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rddBasics").set("spark.files.overwrite", "true")
    val sc = new SparkContext(conf)

    //1. Find the lines that contains http. Or, Find the twitter profile image link for the accounts in which users uses an uploaded image as profile image not the default image.

    //(i). Data (Lines) which contains http

    val csv = sc.textFile("/mnt/git/spark/data/celebrities_profiles.csv")

    val linesWithHttp = csv.filter(x => x.contains("http"))

    //linesWithHttp.collect

    //(ii) Total number of line content "http"
    println("Lines with Http: " + linesWithHttp.count)

    val lineLength = csv.map(x => x.length)

    //(iii)Twitter profile image link (uploaded image link not default image) & number of such profile:

    var profileIMages = csv.filter(_.contains("http://s3.amazonaws.com/twitter_production/profile_images/")).map(x => x.split(",")).filter(x => x.length == 26).map(x => x(13))
    profileIMages.take(10).foreach(println)
    //res10: Long = 6205

    //-------------------------------------------------------------------------------------------------------------------------------------

    //2. Load the data from HDFS and find the total number of celebrities that have less than 3000 followers.

    //(i)Number of accounts which has less than 3000 followers:

    val followers = csv.map(line => line.split(",").map(_.trim))

    val opt = followers.filter(x => x(4).toInt < 3000)

    println("Celebrity followers: " + opt.count())
    //-------------------------------------------------------------------------------------------------------------------------------------

    //3. Perform the WordCount in the file and store the output into the HDFS.

    //(i) Word Count

    val wc = csv.flatMap(l => l.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    //(ii) Saving the output file

    try {
      wc.saveAsTextFile("/mnt/git/spark/output/wc_output_Twiiter_account")
    } catch {
      case fileExists: FileAlreadyExistsException => println("World count file already exists, skiped wirite operation...")
    }
    //-------------------------------------------------------------------------------------------------------------------------------------

    //4. Find the celebrity profile name whose profile is verified (i.e. true) and location is San Francesco.

    //(i) Celebrity profile name whose profile is verified (i.e. true)

    val cleanedCSV = csv.map(line => line.split(",").map(_.trim))

    val profile = cleanedCSV.filter(x => x(1).contains("TRUE"))

    println("Celebrity profile name whose profile is verified " + profile.count)

    //(ii) Twitter profile, belongs to the location “San Francisco”

    val SF = cleanedCSV.filter(x => x(6).contains("San Francisco"))

    println("Celebrity profile name whose location is San Francesco: " + SF.count)

    //(iii) Number of verified twitter accounts which are from “San Francisco” 

    val verifiedAcc = cleanedCSV.filter(x => (x(1).contains("FALSE") && x(6).contains("San Francisco")))

    println("Celebrity profile name whose profile is verified (i.e. true) and location is San Francesco: " + verifiedAcc.count)
    //res2: Long = 99
    //-------------------------------------------------------------------------------------------------------------------------------------

    //5. Find the celebrity name who has the maximum followers.

    //(i) Loading the dataset using HDFS and creating RDD

    val maxVal = cleanedCSV.map(x => x(4).toInt)

    val mf = maxVal.max

    //(ii) Find the celebrity name who has the maximum followers.

    val MaxFollow = cleanedCSV.filter(x => x(4).toInt == mf)

    MaxFollow.collect

    //(iii) Getting the name
    val userName = MaxFollow.map(x => x(21).toString)
    println("Celebrity name who has the maximum followers: " + userName.first)

    //the above way is not a good way, lets try something else
    val maxfollowed = cleanedCSV.map(x => (x(4).toInt, x(21))).reduce((a, b) => if (a._1 > b._1) a else b)
    println("Using better solution: Celebrity name who has the maximum followers: " + maxfollowed)

  }
}

// /mnt/spark/bin/spark-submit --class useCase.Celebrity --jars /mnt/git/spark/target/scala-2.10/adtech-assembly-0.1.0-deps.jar /mnt/git/spark/target/scala-2.10/adtech_2.10-0.1.0.jar