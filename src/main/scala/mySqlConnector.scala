import org.apache.spark.sql.DataFrame

class mySqlConnector {
  def run(session: SparkInit): DataFrame = {
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "InsertUSER"
    val password = "InsertPSW"
    Class.forName(driver)
    val df2 = session.spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "`owid-covid-data`")
      .option("url", "jdbc:mysql://localhost:3306/project2")
      .option("user", username)
      .option("password", password)
      .load()
    df2
  }
}
