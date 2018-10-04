import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

trait Helper {
  val MASTER = "local[*]"
  val SPARK_LOCAL_DIR = "sparkLocalDir"
  val SPARK_WAREHOUSE_LOCAL_DIR = "sparkWarehouseLocalDir"

  /**
    * Set the verbosity level for the Log4J logging engine
    *
    * @param sparkSession the spark session
    * @param level        the verbosity level to be set
    */
  def setLog4JVerbosity(sparkSession: SparkSession, level: String = "ERROR"): Unit = {
    sparkSession.sparkContext.setLogLevel(level)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  /**
    * Create a spark session given in input the map of configuration values and the master info to be used.
    *
    * @param args       the map of configuration values for the spark session
    * @param masterInfo the master info to be used
    * @return a spark context created using the given parameters
    */
  def getSparkSession(args: Map[String, String] = HashMap(), masterInfo: String = "local[*]"): SparkSession = {
    val conf = new SparkConf()
      .setMaster(masterInfo)
      .set("spark.sql.warehouse.dir",
        args.getOrElse("spark.sql.warehouse.dir", "sparkWarehouseLocalDir"))
      .set("spark.local.dir",
        args.getOrElse("spark.local.dir", "sparkLocalDir"))
      .set("spark.sql.shuffle.partitions",
        args.getOrElse("spark.sql.shuffle.partitions", "4"))
      .set("spark.driver.memory",
        args.getOrElse("spark.driver.memory", "3g"))
      .set("spark.executor.memory",
        args.getOrElse("spark.executor.memory", "3g"))
      .setAppName(args.getOrElse("spark.app.name", "MyApp"))

    val spark = SparkSession
      .builder()
      .master(masterInfo)
      .config(conf)
      .getOrCreate()

    spark
  }
}

object DruidSpark extends Helper {

  def getSparkSession(): SparkSession = {
    val sparkCtxParamMap =
      HashMap(
        "spark.sql.warehouse.dir" -> SPARK_WAREHOUSE_LOCAL_DIR,
        "spark.local.dir" -> SPARK_LOCAL_DIR,
        "spark.sql.shuffle.partitions" -> "8",
        "spark.driver.memory" -> "1g",
        "spark.executor.memory" -> "1g",
        "spark.driver.maxResultSize" -> "1g",
        "spark.app.name" -> "DruidSpark"
      )

    val spark = getSparkSession(sparkCtxParamMap, MASTER)

    setLog4JVerbosity(spark)
    spark.conf.getAll.foreach(println)
    spark
  }

  def main(args: Array[String]): Unit = {

    val spark = getSparkSession

    val dw = spark.sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:avatica:remote:url=http://127.0.0.1:8082/druid/v2/sql/avatica/",
        "dbtable" -> "wikiticker",
        "driver" -> "org.apache.calcite.avatica.remote.Driver",
        "fetchSize" -> "10000"
      )
    ).load

    dw.show(false)
    /*
        dw.schema(
          StructType(
            Array(
              StructField("__time", TimestampType, true),
              StructField("added", LongType, true),
              StructField("channel", StringType, true),
              StructField("cityname", StringType, true),
              StructField("comment", StringType, true),
              StructField("count", LongType, true),
              StructField("countryisocode", StringType, true),
              StructField("countryname", StringType, true),
              StructField("deleted", LongType, true),
              StructField("delta", LongType, true),
              StructField("isanonymous", StringType, true),
              StructField("isminor", StringType, true),
              StructField("isnew", StringType, true),
              StructField("isrobot", StringType, true),
              StructField("isunpatrolled", StringType, true),
              StructField("metrocode", StringType, true),
              StructField("namespace", StringType, true),
              StructField("page", StringType, true),
              StructField("regionisocode", StringType, true),
              StructField("regionname", StringType, true),
              StructField("user", StringType, true)//,
            //  StructField("user_unique", StringType, true)
            )
          ))
    */

  }
}