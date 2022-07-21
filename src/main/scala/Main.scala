import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import Console.{CYAN, YELLOW, RED, WHITE, RESET, GREEN}
import scala.collection.ArrayOps
import org.apache.spark.sql

import scala.io.StdIn.readLine


class QueryThread(val query: String, val session: SparkInit) extends Thread {
  override def run(): Unit = {
    session.spark.sql(query).show(false)
  }
}

class OutputThread(val query: String, val session: SparkInit, val outputLocation: String) extends Thread {
  override def run(): Unit = {
    session.spark.sql(query).coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputLocation)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    var method = "default"
    if(args.length >= 1) {
      if(args(0) == "half") {
        method = args(0)
      }
      else if(args(0) == "small") {
        method = args(0)
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

    println(s"$YELLOW Would you like to debug(1) or create csvs(2)$RESET")
    val in = readLine()

    if(in == "1") {
      //queries

      /*
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

       */

      println("Starting query 1")
      val Qthread1 = new Thread(new QueryThread(queries.query1(), session))
      Qthread1.start()

      println("Starting query 2")
      val Qthread2 = new Thread(new QueryThread(queries.query2(), session))
      Qthread2.start()

      println("Starting query 3")
      val Qthread3 = new Thread(new QueryThread(queries.query3(), session))
      Qthread3.start()

      println("Starting query 4")
      val Qthread4 = new Thread(new QueryThread(queries.query4(), session))
      Qthread4.start()

      println("Starting query 5")
      val Qthread5 = new Thread(new QueryThread(queries.query5(), session))
      Qthread5.start()

      println("Starting query 6")
      val Qthread6 = new Thread(new QueryThread(queries.query6(), session))
      Qthread6.start()

      println("Starting query 7")
      val Qthread7 = new Thread(new QueryThread(queries.query7(), session))
      Qthread7.start()

      println("Starting query 8")
      val Qthread8 = new Thread(new QueryThread(queries.query8(), session))
      Qthread8.start()

      println("Starting query 9")
      val Qthread9 = new Thread(new QueryThread(queries.query9(), session))
      Qthread9.start()

      println("Starting query 10")
      val Qthread10 = new Thread(new QueryThread(queries.query10(), session))
      Qthread10.start()

      Qthread1.join()
      Qthread2.join()
      Qthread3.join()
      Qthread4.join()
      Qthread5.join()
      Qthread6.join()
      Qthread7.join()
      Qthread8.join()
      Qthread9.join()
      Qthread10.join()




    }
    if(in == "2") {
      //queries

      /*
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

       */

      println("Outputting query 1")
      val outThread1 = new Thread(new OutputThread(queries.query1(), session, "./resultCsv/query1/"))
      outThread1.start()

      println("Outputting query 2")
      val outThread2 = new Thread(new OutputThread(queries.query2(), session, "./resultCsv/query2/"))
      outThread2.start()

      println("Outputting query 3")
      val outThread3 = new Thread(new OutputThread(queries.query3(), session, "./resultCsv/query3/"))
      outThread3.start()

      println("Outputting query 4")
      val outThread4 = new Thread(new OutputThread(queries.query4(), session, "./resultCsv/query4/"))
      outThread4.start()

      println("Outputting query 5")
      val outThread5 = new Thread(new OutputThread(queries.query5(), session, "./resultCsv/query5/"))
      outThread5.start()

      println("Outputting query 6")
      val outThread6 = new Thread(new OutputThread(queries.query6(), session, "./resultCsv/query6/"))
      outThread6.start()

      println("Outputting query 7")
      val outThread7 = new Thread(new OutputThread(queries.query7(), session, "./resultCsv/query7/"))
      outThread7.start()

      println("Outputting query 8")
      val outThread8 = new Thread(new OutputThread(queries.query8(), session, "./resultCsv/query8/"))
      outThread8.start()

      println("Outputting query 9")
      val outThread9 = new Thread(new OutputThread(queries.query9(), session, "./resultCsv/query9/"))
      outThread9.start()

      println("Outputting query 10")
      val outThread10 = new Thread(new OutputThread(queries.query10(), session, "./resultCsv/query10/"))
      outThread10.start()

      outThread1.join()
      outThread2.join()
      outThread3.join()
      outThread4.join()
      outThread5.join()
      outThread6.join()
      outThread7.join()
      outThread8.join()
      outThread9.join()
      outThread10.join()

    }

    session.logger.warn(s"$GREEN has finished running! Thanks for your time!$RESET")
  }


}
