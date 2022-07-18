import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DecimalType, DateType}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    var df = session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv")
    val basicCleaning = new BasicCleaning
    df = basicCleaning.basicCleaning(df) // Cleaning Functions with basic casting for ETL
    df.createOrReplaceTempView("raw")
    df = session.spark.sql("SELECT * FROM raw WHERE iso_code NOT LIKE 'OWID_%'")
    df.createOrReplaceTempView("data")

    val queries = new Queries

    val tmp = session.spark.sql(queries.query3())
    tmp.createOrReplaceTempView("rate")
    val query1 = session.spark.sql(queries.query1()).toDF()
    query1.write.option("header", "true").csv("C:\\proj2\\Query1.csv")


    //session.spark.sql("SELECT * FROM raw WHERE location = 'China'").show(1000)

    session.logger.error("Program has finished running! Thanks for your time!")
  }
}
