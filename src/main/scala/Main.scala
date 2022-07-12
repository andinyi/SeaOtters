object Main {
  def main(args: Array[String]): Unit = {

    val session = new SparkInit("Project Sea Otters")
    session.spark.read.csv("datasets/covid_19_data.csv").show()
  }
}

