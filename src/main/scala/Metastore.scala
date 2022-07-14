import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Metastore extends App {
  System.setProperty("hadoop.home.dir", "C:\\hadoop3")
  //calling the spark "session"
  val session = new SparkInit("Project Sea Otters")

  //creating the dataframe for owid
  val df1 = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\SeaOtters\\datasets\\owid-covid-data.csv").toDF()
  df1.printSchema()
  df1.createOrReplaceTempView("owidCsv")
  //df1.show()

  //creating the table for owid
  session.spark.sql("Drop table if exists owid_tb")
  session.spark.sql("create table if not exists owid_tb(continent string, location string, date string, total_cases string, total_deaths string, total_tests string, total_vaccinations string, people_vaccinated string, total_boosters string, population string, population_density string, median_age string)")
  session.spark.sql("Insert Into Table owid_tb(Select continent, location, date, total_cases, total_deaths, total_tests, total_vaccinations, people_vaccinated , total_boosters, population, population_density, median_age from owidCsv)")
  session.spark.sql("Select * from owid_tb").show(1000)

  //creating the dataframe for covid (william) data
  val df2 = session.spark.read.option("header", "true").csv("C:\\Users\\Fenix Xia\\Documents\\GitHub\\SeaOtters\\datasets\\covid_19_data.csv").toDF()
  df2.printSchema()
  df2.show()
  df2.createOrReplaceTempView("covidCsv")

  //creating the table for covid (william) data
  session.spark.sql("Drop table if exists covid_tb")
  session.spark.sql("create table if not exists covid_tb(SNo string, ObservationDate string, Province_State string, Country_Region string, LastUpdate string, Confirmed string, Deaths string, Recovered string)")
  session.spark.sql("Insert Into Table covid_tb(Select SNo, ObservationDate, Province_State, Country_Region , LastUpdate, Confirmed, Deaths, Recovered from covidCsv)")
  session.spark.sql("Select * from covid_tb").show(1000)


}
