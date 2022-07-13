import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.IntegerType

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    //session.spark.read.option("header", "true").csv("datasets/covid_19_data.csv").show()
    val df1 = session.spark.sql("Select * from owid_tb").toDF()
    import session.spark.implicits._
    val q1 = df1.select($"total_deaths".cast(IntegerType), $"population".cast(IntegerType), $"location", $"date").toDF()
    q1.printSchema()
    q1.show()
    q1.createOrReplaceTempView("owid")
    session.spark.sql(Query4.query4()).show(1000000)

  }
}

