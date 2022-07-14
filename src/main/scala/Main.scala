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
    df = df.withColumn("total_cases", col("total_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("total_deaths", col("total_deaths").cast(DecimalType(18, 1)))
    df = df.withColumn("new_cases", col("new_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("population", col("population").cast(DecimalType(18, 1)))
    df = df.withColumn("date", col("date").cast(DateType))
    df.createOrReplaceTempView("data")
    //session.spark.sql(Query1.query1).show(false)
    //df.createOrReplaceTempView("dataView") //query 2
    //session.spark.sql(Q2.query2).show(false)
    //session.spark.sql(Query3.query3).show()
    //session.spark.sql("SELECT location, MAX(new_cases) FROM data GROUP BY location").show()

    //df.createOrReplaceTempView("owid") //query 4
    //session.spark.sql(Query4.query4()).show(1000000)
    //session.spark.sql(Query5.query5).show(false)
    //session.spark.sql(Query6.query6).show(false)
    //session.spark.sql(Query7.query7()).show()
  }
}
