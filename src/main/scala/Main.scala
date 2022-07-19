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

    val session = new SparkInit("Project Sea Otters")

    var df = session.spark.read.option("header", "true").csv("hdfs://localhost:9000/tmp/project2/datasets/owid-covid-data.csv")
    df.createOrReplaceTempView("data")
    val queries = new Queries()
    val q1Df =  session.spark.sql(queries.query11())
    q1Df.show()

    q1Df.write.option("header",true).csv("C:\\proj2\\Query11.csv")

    session.spark
  }
}
