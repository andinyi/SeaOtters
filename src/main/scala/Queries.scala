class Queries {
  def query1():String = {
    "SELECT total_cases, total_deaths, (total_cases - total_deaths) as total_recovered, population FROM raw WHERE date = '2022-07-10' AND location = 'World'"
  }
  def query2():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived FROM data WHERE continent IS NOT NULL GROUP BY location"
  }
  def query3():String = {
    "SELECT data.location, data.new_cases as peak, t5.First_Date AS testDate, data.date AS Peak_Date, datediff(data.date, t5.First_Date) AS DIFFDATES, peak/(datediff(data.date, t5.First_Date)) AS rate FROM data JOIN (SELECT location, MAX(new_cases) AS peak FROM data GROUP BY location) AS t1 ON (data.location = t1.location AND data.new_cases = t1.peak)"+
      " INNER JOIN (SELECT location, to_date(MIN(date)) AS First_Date FROM (SELECT location, date FROM data WHERE new_cases >= 1) GROUP BY location) AS t5 ON t1.location = t5.location"
  }
  def query4():String = {
    "Select location, cast((total_deaths/population)*100 as decimal(8,5)) As death from data where date='2022-07-10' order by death desc"
  }
  def query5():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(population)), MAX(INT(total_cases))/MAX(INT(population)) AS cases_per_person FROM data WHERE continent IS NOT NULL GROUP BY location"
  }
  def query6():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_tests)), (MAX(INT(total_cases))/MAX(INT(total_tests)))*100 as percent_positive FROM data WHERE continent IS NOT NULL GROUP BY location"
  }
  def query7():String = {
    "Select location, cast((people_vaccinated/population)*100 as decimal(8,5)) As Vaccinated from data where date='2022-07-10' order by Vaccinated desc"
  }
  def query8():String = {
    "SELECT data.location, AVG(data.population_density) AS density, AVG(rate.rate) AS rate, AVG(data.population_density)/AVG(rate.rate) AS density_by_rate FROM data JOIN rate ON data.location = rate.location GROUP BY data.location"
  }
  def query9():String = {
    "SELECT data.location, AVG(data.gdp_per_capita) AS gdp, AVG(rate.rate) AS rate, AVG(data.gdp_per_capita)/AVG(rate.rate) AS gdp_by_rate FROM data JOIN rate ON data.location = rate.location GROUP BY data.location"
  }
  def query10():String = {
    "Select location, median_age,Round(((total_cases - total_deaths)/population)*100, 4) as recovered from data where date = '2022-07-10'"
  }
  def query11():String = {


    "SELECT * FROM "+
    "(SELECT location, percent_survived_with_vaccine FROM "+
    "(SELECT location,  MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived_with_vaccine "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS NOT null GROUP BY location ORDER BY location)) AS t1 "+
    "INNER JOIN " +
      "(SELECT location, percent_survived_without_vaccine FROM "+
      "(SELECT location,  MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived_without_vaccine "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS null GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location"

    "SELECT * FROM "+
      "(SELECT location, New_cases_after_vacc FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_after_vacc "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS NOT null GROUP BY location ORDER BY location)) AS t1 "+
      "INNER JOIN " +
      "(SELECT location, (New_cases_before_vacc / yo) AS newcases_over_time FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_before_vacc, COUNT(INT(new_cases)) AS yo "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS null GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location"

/***************************************************************************************************************/
    "SELECT * FROM "+
      "(SELECT location, (New_cases_after_vacc / yo) AS With_Vaccine_newcases_over_time FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_after_vacc, COUNT(location) AS yo "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS NOT null GROUP BY location ORDER BY location)) AS t1 "+
      "INNER JOIN " +
      "(SELECT location, (New_cases_before_vacc / total) AS Without_Vaccine_newcases_over_time FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_before_vacc, COUNT(location) AS total "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS null GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location"
/*****************************************************************************************************************/

    "SELECT * FROM "+
      "(SELECT location, (New_cases_after_vacc / total2) AS With_Vaccine_newcases_over_time FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_after_vacc, COUNT(location) AS total2 "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS NOT null AND date < '2022-06-01' GROUP BY location ORDER BY location)) AS t1 "+
      "INNER JOIN " +
      "(SELECT location, (New_cases_before_vacc / total) AS Without_Vaccine_newcases_over_time FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_before_vacc, COUNT(location) AS total "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS null AND date < '2022-06-01' GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location"


    /*"SELECT * FROM "+
      "(SELECT location, percent_survived_with_vaccine FROM "+
      "(SELECT location,  MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived_with_vaccine "+
      "FROM data WHERE continent IS NOT NULL AND (SELECT MIN(date) FROM data GROUP BY location) > (SELECT to_date(first_vac_date) FROM (SELECT location, MIN(date) AS first_vac_date FROM data WHERE total_vaccinations > 1 GROUP BY location)) GROUP BY location)) AS t1 "+
      "INNER JOIN " +
      "(SELECT location, percent_survived_without_vaccine FROM "+
      "(SELECT location,  MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived_without_vaccine "+
      "FROM data WHERE continent IS NOT NULL AND data.date < (SELECT to_date(MIN(first_vac_date)) FROM (SELECT location, date AS first_vac_date FROM data WHERE total_vaccinations > 1)) GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location"

    "SELECT to_date(first_vac_date) FROM "+
      "(SELECT location, AVG(date) AS first_vac_date FROM data WHERE new_vaccinations_smoothed > 0)"*/

    /*"(SELECT MIN(date) FROM data GROUP BY location)"*/








  }
}
