import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import Console.{CYAN, YELLOW, RED, WHITE, RESET, GREEN}
import org.apache.spark.sql
import scala.io.StdIn.readLine

object Main {

  def main(args: Array[String]): Unit = {
    
    var method = "default"
    var output = 1
    if(args.length >= 1) {
      if(args(0) == "half") {
        method = args(0)
      }
      else if(args(0) == "small") {
        method = args(0)
      }
    }
    if(args.length >= 2) {
      if(args(1) == "1") {
        output = 1
      }
      else if(args(1) == "0") {
        output = 0
      }
    }

    println(s"$CYAN")
    val session = new SparkInit("Project Sea Otters")
    println(s"$RESET")

    session.logger.info(s"$CYAN Session Created! Attempting to read in information! $RESET")
    //var df = session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv")
    var df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/owid-covid-data.csv")
    if(method == "half") {
      df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid1.csv", "hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid2.csv", "hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid3.csv", "hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid4.csv", "hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid5.csv")
    }
    else if(method == "small") {
      df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid2.csv")
    }
    session.logger.info(s"$CYAN Data read in properly!$RESET")


    //ETL FUNCTIONS (CLEANS AND FORMATS DATA FOR EASE OF ANALYZING)
    session.logger.info(s"$CYAN Attempting to perform ETL operations on the dataset.$RESET")
    val basicCleaning = new BasicCleaning
    df = basicCleaning.basicCleaning(df) // Cleaning Functions with basic casting for ETL
    df.createOrReplaceTempView("raw")
    df = session.spark.sql("SELECT * FROM raw WHERE iso_code NOT LIKE 'OWID_%'")
    df.createOrReplaceTempView("tmp")
    val queries = new Queries
    val tmp = session.spark.sql(queries.cleanQuery())
    tmp.createOrReplaceTempView("rate")
    df = session.spark.sql("SELECT tmp.*, rate.rate FROM tmp JOIN rate ON tmp.location = rate.location")
    df.createOrReplaceTempView("data")
    session.logger.info(s"$CYAN Dataset cleaning completed! $RESET")

    if(output == 1) {
      //queries
      session.spark.sql(queries.query1()).show(false)
      session.spark.sql(queries.query2()).show(false)
      session.spark.sql(queries.query3()).show(false)
      session.spark.sql(queries.query4()).show(false)
      session.spark.sql(queries.query5()).show(false)
      session.spark.sql(queries.query6()).show(false)
      session.spark.sql(queries.query7()).show(false)
      session.spark.sql(queries.query8()).show(false)
      session.spark.sql(queries.query9()).show(false)
      session.spark.sql(queries.query10()).show(false)
      session.spark.sql(queries.query11()).show(false)

    }
    if(output == 0) {
      //queries
      session.spark.sql(queries.query1()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query1/")
      session.spark.sql(queries.query2()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query2/")
      session.spark.sql(queries.query3()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query3/")
      session.spark.sql(queries.query4()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query4/")
      session.spark.sql(queries.query5()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query5/")
      session.spark.sql(queries.query6()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query6/")
      session.spark.sql(queries.query7()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query7/")
      session.spark.sql(queries.query8()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query8/")
      session.spark.sql(queries.query9()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query9/")
      session.spark.sql(queries.query10()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query10/")
      session.spark.sql(queries.query11()).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv("./resultCsv/query11/")

    }
    session.logger.warn(s"$GREEN has finished running! Thanks for your time!$RESET")
  }
}
