package searcher

import java.io.File
import scala.io.Source
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

    while (sentence != ":quit") {
      print("search> ")
      sentence = readLine()

      if (sentence != ":quit") {
        val wordSeq = sentence.toLowerCase().trim().split("""[\s,.;:!?*]+""").toSeq
        println("concat arguments = " + wordSeq.toString())

        val filesMap = files.map { f =>
          val source = Source.fromFile(f.getPath)

          val resultMap = try {
            val wordList = source.mkString.trim().split("""[\s,.;:!?*]+""").map(_.toLowerCase()).distinct //or toSet
            val filteredList = wordList.filter(x => wordSeq.contains(x))
            val rank = filteredList.length / (wordSeq.length).toFloat * 100
            (f.toString() -> rank)
          } finally source.close()
          resultMap
        }.toMap
        val documentsWithResults = filesMap.filter(x => x._2 > 0.0).toSeq.sortBy(_._2)(Ordering[Float].reverse)
        if (documentsWithResults.size != 0)
          println(documentsWithResults.map(x => x._1 + " " + x._2 + "%\n").mkString)
        else print("no matches found\n")
      }
    }
  }
}
