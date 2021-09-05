import SeenFileLogReader._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

import scala.util.Try

/**
 * Class for reading log file with predefined schema and return RDD which contains seen files in form Option(Extension) -> Filename
 * Extension is considered to have at least one alpha-numeric symbol or underscore symbol after last point;
 * File containing point only as a first symbol considered as file without extension
 * @param sparkSession spark seesion for RDD creation
 */
class SeenFileLogReader(sparkSession: SparkSession) {
  def tryToCreateSeenFilesRdd(pathToLog: String): Try[RDD[(Option[String], String)]] = Try {
    val logFileDataFrame =
      sparkSession
        .read
        .option("mode", "DROPMALFORMED")
        .schema(logFileSchema)
        .json(pathToLog)


    logFileDataFrame.select(FileNameColumnName)
      .rdd
      .filter(row => row.size == 1 && row(0) != null)
      .map(row => row(0).toString.trim)
      .filter(fileName => fileName.nonEmpty)
      .map{
        case FileNamePattern(name, extension) => (Option(extension), name)
        case name => (None, name)}
  }
}

object SeenFileLogReader {
  private val FileNameColumnName = "nm"

  private val logFileSchema =
    new StructType()
      .add("ts", TimestampType)
      .add("pt", StringType)
      .add("si", StringType)
      .add("uu", StringType)
      .add("bg", StringType)
      .add("sha", StringType)
      .add(FileNameColumnName, StringType, nullable = false)
      .add("ph", StringType)
      .add("dp", StringType)

  private val FileNamePattern = "(.+)\\.(\\w+)".r
}
