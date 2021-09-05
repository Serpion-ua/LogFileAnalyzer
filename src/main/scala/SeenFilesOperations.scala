import org.apache.spark.rdd.RDD

import scala.collection.Map

/**
 * Object for possible operations over seen files
 */
object SeenFilesOperations {

  /**
   * Return count of unique file names for given RDD in form Map[Extension -> uniques filenames count]
   * @param seenFiles RDD to process
   * @return unique filename count for each extension
   */
  def countUniqueFileNamePerExtension(seenFiles: RDD[(Option[String], String)]): Map[Option[String], Int] = {
    seenFiles
    .groupByKey()
      .mapValues(names => names.toSet.size)
      .collectAsMap()
  }
}
