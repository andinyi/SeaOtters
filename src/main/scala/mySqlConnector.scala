class mySqlConnector {
  def run(session : SparkInit) {
    val driver = "com.mysql.cj.jdbc.Driver"
    val username = "InsertUSERNSME"
    val password = "insertPSW"

    try {
      Class.forName(driver)


      val df2 = session.spark.read.format("jdbc")
        .option("driver","com.mysql.cj.jdbc.Driver")
        .option("dbtable","covid_data")
        .option("url","jdbc:mysql://localhost:3306/project2")
        .option("user",username)
        .option("password",password)
        .load()
      df2.createOrReplaceTempView("mysql")
    }

  }

}