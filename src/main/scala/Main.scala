import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.DecimalType
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    var df = session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv")
    df = df.withColumn("total_cases", col("total_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("total_deaths", col("total_deaths").cast(DecimalType(18, 1)))
    df = df.withColumn("population", col("population").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("query1")
    session.spark.sql(Query1.query1).show(false)
    df.createOrReplaceTempView("dataView")
    session.spark.sql(Q2.query2).show(false)
    df.createOrReplaceTempView("owid")
    session.spark.sql(Query4.query4()).show(1000000)
    df.createOrReplaceTempView("owid")
    session.spark.sql(Query7.query7()).show()
  }
}

