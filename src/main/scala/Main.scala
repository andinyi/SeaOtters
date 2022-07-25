import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DateType, DecimalType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import Console.{CYAN, YELLOW, RED, WHITE, RESET, GREEN}
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
    var output:Boolean = false
    var thread:Boolean = false
    if(args.length >= 1) {
      if(args(0) == "--half") {
        method = "half"
      }
      else if(args(0) == "--small") {
        method = "small"
      }
      else if(args(0) == "--mysql") {
        method = "mysql"
      }
      else if(args(0) == "--help") {
        println(s"$GREEN How to call program\n" +
                        s"$RESET$CYAN(ARGUMENT 1)$RESET$GREEN\n" +
                        " --default (default) to do a run with full dataset\n" +
                        " --small to load a small subset of the dataset\n" +
                        " --half to load a subset half the size of the dataset\n" +
                        " --mysql to load the dataset from MySQL connection\n" +
                        s"$RESET$CYAN(ARGUMENT 2)$RESET$GREEN\n" +
                        " --debug (default) to print out query tables, debug mode\n" +
                        " --output (default) to create files for visualization\n" +
                        s"$RESET$CYAN(ARGUMENT 3)$RESET$GREEN\n" +
                        s" --thread $RESET$YELLOW(EXPERIMENTAL)$RESET$GREEN utilize multithreading for queries$RESET")

        return
      }
    }

    if(args.length >= 2) {
      if(args(1) == "--debug") {
        output = false
      }
      else if(args(1) == "--output") {
        output = true
      }
    }
    if(args.length >= 3) {
      if(args(2) == "--thread") {
        thread = true
      }
    }

    println(s"$CYAN")
    val session = new SparkInit("Project Sea Otters")
    println(s"$RESET")

    session.logger.info(s"$CYAN Session Created! Attempting to read in information! $RESET")
    var df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/owid-covid-data.csv")
    //var df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/owid-covid-data.csv")

    if(method != "default") {
      if(method == "mysql") {
        val mysql = new mySqlConnector()
        df = mysql.run(session)
        session.logger.info(s"$CYAN Attempting queries using MySQL!$RESET")
      }
      else if(method == "half") {
        df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/batchedOwidHalf/owid1.csv")
        session.logger.info(s"$CYAN Attempting queries using half of the dataset!$RESET")
      }
      else if(method == "small") {
        df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/batchedOwid/owid5.csv")
        session.logger.info(s"$CYAN Attempting queries using a small subset of the dataset!$RESET")
      }
    }

    session.logger.info(s"$CYAN Data read in properly!$RESET")

    /*val connect = new mySqlConnector
    connect.run(session)*/

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

    if(!output) {
      //queries
      if(thread) {
        session.logger.info("Starting query 1")
        val Qthread1 = new Thread(new QueryThread(queries.query1(), session))
        Qthread1.start()
        session.logger.info("Starting query 2")
        val Qthread2 = new Thread(new QueryThread(queries.query2(), session))
        Qthread2.start()
        session.logger.info("Starting query 3")
        val Qthread3 = new Thread(new QueryThread(queries.query3(), session))
        Qthread3.start()
        session.logger.info("Starting query 4")
        val Qthread4 = new Thread(new QueryThread(queries.query4(), session))
        Qthread4.start()
        session.logger.info("Starting query 5")
        val Qthread5 = new Thread(new QueryThread(queries.query5(), session))
        Qthread5.start()
        session.logger.info("Starting query 6")
        val Qthread6 = new Thread(new QueryThread(queries.query6(), session))
        Qthread6.start()
        session.logger.info("Starting query 7")
        val Qthread7 = new Thread(new QueryThread(queries.query7(), session))
        Qthread7.start()
        session.logger.info("Starting query 8")
        val Qthread8 = new Thread(new QueryThread(queries.query8(), session))
        Qthread8.start()
        session.logger.info("Starting query 9")
        val Qthread9 = new Thread(new QueryThread(queries.query9(), session))
        Qthread9.start()
        session.logger.info("Starting query 10")
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
      else {
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

      }
    }
    if(output) {
      //queries
      if(thread) {
        session.logger.info("Outputting query 1")
        val outThread1 = new Thread(new OutputThread(queries.query1(), session, "./resultCsv/query1/"))
        outThread1.start()
        session.logger.info("Outputting query 2")
        val outThread2 = new Thread(new OutputThread(queries.query2(), session, "./resultCsv/query2/"))
        outThread2.start()
        session.logger.info("Outputting query 3")
        val outThread3 = new Thread(new OutputThread(queries.query3(), session, "./resultCsv/query3/"))
        outThread3.start()
        session.logger.info("Outputting query 4")
        val outThread4 = new Thread(new OutputThread(queries.query4(), session, "./resultCsv/query4/"))
        outThread4.start()
        session.logger.info("Outputting query 5")
        val outThread5 = new Thread(new OutputThread(queries.query5(), session, "./resultCsv/query5/"))
        outThread5.start()
        session.logger.info("Outputting query 6")
        val outThread6 = new Thread(new OutputThread(queries.query6(), session, "./resultCsv/query6/"))
        outThread6.start()
        session.logger.info("Outputting query 7")
        val outThread7 = new Thread(new OutputThread(queries.query7(), session, "./resultCsv/query7/"))
        outThread7.start()
        session.logger.info("Outputting query 8")
        val outThread8 = new Thread(new OutputThread(queries.query8(), session, "./resultCsv/query8/"))
        outThread8.start()
        session.logger.info("Outputting query 9")
        val outThread9 = new Thread(new OutputThread(queries.query9(), session, "./resultCsv/query9/"))
        outThread9.start()
        session.logger.info("Outputting query 10")
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
      else {
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
      }
    }
    session.logger.warn(s"$GREEN Program has finished running! Thanks for your time!$RESET")
  }


}
