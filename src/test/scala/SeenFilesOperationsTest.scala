import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SeenFilesOperationsTest extends FunSuite with BeforeAndAfterAll with Matchers {
  var sparkTestSession: SparkSession = _

  override def beforeAll(): Unit = {
    sparkTestSession = SparkSession
      .builder()
      .appName("LogAnalyzerTest")
      .config("spark.master", "local")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkTestSession.stop()
  }

  test("Empty RDD is processed correctly") {
    val rdd: RDD[(Option[String], String)] = sparkTestSession.sparkContext.parallelize(Seq())

    val resultMap = SeenFilesOperations.countUniqueFileNamePerExtension(rdd)

    assert(resultMap.size === 0)
  }

  test("Non-Empty RDD is processed correctly") {
    val testData = Seq(
      (Option("ext1"), "fileName1"),
      (Option("ext1"), "fileName2"),
      (Option("ext1"), "fileName3"),
      (Option("ext2"), "fileName4"),
    )

    val rdd: RDD[(Option[String], String)] = sparkTestSession.sparkContext.parallelize(testData)

    val resultMap = SeenFilesOperations.countUniqueFileNamePerExtension(rdd)

    assert(resultMap.size === 2)
    assert(resultMap(Some("ext1")) === 3)
    assert(resultMap(Some("ext2")) === 1)
  }

  test("the same file name with the same extension is not counted twice") {
    val testData = Seq(
      (Option("ext1"), "fileName1"),
      (Option("ext1"), "fileName1"),
      (Option("ext2"), "fileName2"),
      (Option("ext2"), "fileName2"),
      (Option("ext3"), "fileName2"),
      (Option("ext2"), "fileName2"),
    )

    val rdd: RDD[(Option[String], String)] = sparkTestSession.sparkContext.parallelize(testData)

    val resultMap = SeenFilesOperations.countUniqueFileNamePerExtension(rdd)

    assert(resultMap.size === 3)
    assert(resultMap(Some("ext1")) === 1)
    assert(resultMap(Some("ext2")) === 1)
    assert(resultMap(Some("ext3")) === 1)
  }
}
