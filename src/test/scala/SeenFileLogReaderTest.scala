import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SeenFileLogReaderTest extends FunSuite with BeforeAndAfterAll with Matchers {

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

  lazy val logReader = new SeenFileLogReader(sparkTestSession)

  test("reading non exist file") {
    val tryEmptyRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/incorrst_path.json")
    assert(tryEmptyRDD.isFailure)
  }

  test("reading empty file") {
    val emptyRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/empty_log.json").get
    assert(emptyRDD.count() === 0)
  }

  test("reading big zipped file") {
    val bigRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/big_compressed_file.json.gz")
    assert(bigRDD.isSuccess)
  }

  test("reading file with incorrect schema") {
    val incorrectSchemaRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/incorrect_schema.json").get
    val results = incorrectSchemaRDD.groupByKey().collect().toMap.mapValues(_.toSet)

    assert(results.size === 2)
    assert(results.contains(Some("psp2")))
    assert(results.contains(Some("csv")))
  }

  test("reading file with only one correct entry") {
    val oneCorrectEntryRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/incorrect_entries.json").get
    assert(oneCorrectEntryRDD.count() === 1)
  }

  test("file extensions are parsed correctly") {
    val strangeExtensionsRDD =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/strange_extensions.json").get
    val results = strangeExtensionsRDD.groupByKey().collect().toMap.mapValues(_.toSet)

    assert(results.contains(None))
    assert(results(None).size === 3)
    assert(results.contains(Some("it")))
  }

  test("same entries are readed") {
    val sameEntries =
      logReader.tryToCreateSeenFilesRdd("src/test/resources/same_entries.json").get

    assert(sameEntries.count() === 9)
  }
}
