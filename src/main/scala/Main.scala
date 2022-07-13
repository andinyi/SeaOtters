import org.apache.spark
import org.apache.spark.sql

object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    //session.spark.read.csv("datasets/covid_19_data.csv").show()

    val dataDf = session.spark.read.format("csv").option("header","true").load("datasets/owid-covid-data.csv")
    dataDf.createOrReplaceTempView("dataView")

    session.spark.sql(Q2.query2).show(false)

  }
}

