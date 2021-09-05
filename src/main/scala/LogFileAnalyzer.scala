import org.apache.spark.sql.SparkSession

import scala.util.Properties
import scala.collection.Map

object LogFileAnalyzer {

  def main(args:Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LogAnalyzer")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val fileReader = new SeenFileLogReader(sparkSession)

    val resultMessage =
      fileReader
      .tryToCreateSeenFilesRdd("src/main/resources/log.json.gz")
      .map(SeenFilesOperations.countUniqueFileNamePerExtension)
      .fold(errorToString, resultMapToString)

    println(resultMessage)

    sc.stop()
  }

  private def errorToString(error: Throwable): String = {
    s"Failed to obtain result due: ${error.getMessage}"
  }

  private def resultMapToString(extToCount: Map[Option[String], Int]): String = {
    extToCount
      .map{
        case (Some(extension), count) => s"$extension: $count"
        case (None, count) => s"files without extension: $count"
      }
      .mkString(Properties.lineSeparator)
  }
}
