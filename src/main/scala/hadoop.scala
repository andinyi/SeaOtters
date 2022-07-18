object hadoop extends App{
  //System.setProperty("hadoop.home.dir", "C:\\hadoop3")
  val session = new SparkInit("Project Sea Otters")
  val df1 = session.spark.read.option("header", "true").csv(path = "hdfs://localhost:9000/user/proj2/owid-covid-data.csv")
  df1.printSchema()
  df1.createOrReplaceTempView("owid")

  val dfs1 = session.spark.sql("Select * from owid")
  dfs1.write.mode("overwrite").csv("hdfs://localhost:9000/user/proj2/owid.csv")
}
