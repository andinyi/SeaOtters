object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    //session.spark.read.option("header", "true").csv("datasets/covid_19_data.csv").show()
    session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv").show()
  }
}

