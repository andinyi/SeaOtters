object Main {
  def main(args: Array[String]): Unit = {
    val session = new SparkInit("Project Sea Otters")
    val df = session.spark.read.option("header", "true").csv("datasets/owid-covid-data.csv").show()
  }
}

