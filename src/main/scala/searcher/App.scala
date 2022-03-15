package searcher

import org.apache.spark.mllib.rdd.RDDFunctions.fromRDD
import org.apache.spark.sql.functions._

import java.io.File
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.ListMap
import scala.io.StdIn.readLine

/**
 * @author ${user.name}
 */


object App {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      throw new IllegalArgumentException("No directory given to index.")
    }
    var sentence = ""
    val files = args.flatMap { dir => getListOfFiles(dir) }
    println(s"${files.length} files read in directory ${args.toList.mkString(",")}\n")
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("little-search-word-count")
      .getOrCreate();
    val sc = spark.sparkContext

    while (sentence != ":quit") {
      print("search> ")
      sentence = readLine()

      if (sentence != ":quit") {
        val wordSeq = sentence.toLowerCase().split("""[\s,.;:!?]+""").toSeq
        println("concat arguments = " + wordSeq.toString())

        val result = files.map { f =>
          val wordCountRDD = sc.textFile(f.getPath)
          val wordCountArray = wordCountRDD.flatMap(_.split("""[\s,.;:!?]+""")).
            map(_.toLowerCase).
            sliding(1).
            map { case Array(w1) => (w1, 1) }.
            reduceByKey(_ + _).collect()
          val wordCount = wordCountArray.filter(x => wordSeq.contains(x._1)) //Also we could extract count of matching words, from wordCount.

          val rank = wordCount.length / (wordSeq.length).toFloat * 100

          (f.toString() -> rank)
        }.sortBy(_._2)(Ordering[Float].reverse)
        val documentsWithResults = result.filter(x => x._2 > 0.0)
        if (documentsWithResults.length != 0)
          println(documentsWithResults.map(x => x._1 + " " + x._2 + "%\n").mkString)
        else print("no matches found\n")

      }
      // wordCountRDD.takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2)).foreach(println)
    }
  }

}
