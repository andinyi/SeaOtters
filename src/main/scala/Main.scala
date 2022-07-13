import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SaveMode, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val session = new SparkInit("Project Sea Otters")
    var df = session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv")
    df = df.withColumn("total_cases", col("total_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("total_deaths", col("total_deaths").cast(DecimalType(18, 1)))
    df.createOrReplaceTempView("query3")
    session.spark.sql(Query3.query3)


    val max_cases:DataFrame = session.spark.sql(Query3.query3)
    max_cases.createOrReplaceTempView("t1")
    max_cases.show()
    //session.spark.sql("SELECT query3.location, MIN(query3.date), MAX(t1.peak) FROM query3 JOIN t1 ON query3.location = t1.location GROUP BY query3.location").show()
    session.spark.sql("SELECT query3.location, MIN(query3.date) AS normal, MAX(t1.peak), MIN(t1.date) FROM query3 JOIN t1 ON int(query3.new_cases) = t1.peak GROUP BY query3.location").show()

  }
}

