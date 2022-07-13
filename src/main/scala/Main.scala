import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.IntegerType

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    //session.spark.read.option("header", "true").csv("datasets/covid_19_data.csv").show()
    val df1 = session.spark.sql("Select * from owid_tb").toDF()
    df1.show()
    import session.spark.implicits._
    val q1 = df1.select($"people_vaccinated".cast(IntegerType), $"population".cast(IntegerType), $"location", $"date").toDF()
    q1.createOrReplaceTempView("owid")
    session.spark.sql("Select location, cast((people_vaccinated/population)*100 as decimal(8,5)) As Vaccinated from owid where date='2022-07-10' order by Vaccinated desc").show()



  }
}

