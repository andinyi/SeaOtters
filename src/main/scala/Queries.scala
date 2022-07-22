class Queries {
  def query1():String = {
    "SELECT total_cases, total_deaths, (total_cases - total_deaths) as total_recovered, population FROM raw WHERE date = '2022-07-10' AND location = 'World'"
  }
  def query2():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived FROM data WHERE continent IS NOT NULL AND location != 'North Korea' GROUP BY location ORDER BY percent_survived DESC"
  }
  def query3():String = {
    "SELECT data.location, data.new_cases as peak, t5.First_Date AS testDate, data.date AS Peak_Date, datediff(data.date, t5.First_Date) AS DIFFDATES, rate FROM data JOIN (SELECT location, MAX(new_cases) AS peak FROM data GROUP BY location) AS t1 ON (data.location = t1.location AND data.new_cases = t1.peak)"+
      " INNER JOIN (SELECT location, to_date(MIN(date)) AS First_Date FROM (SELECT location, date FROM data WHERE new_cases >= 1) GROUP BY location) AS t5 ON t1.location = t5.location ORDER BY rate DESC"
  }

  def query4():String = {
    "SELECT location, MAX(INT(total_deaths))/MAX(INT(population))*100 AS death FROM data GROUP BY location ORDER BY death DESC"
  }

  def query5():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(population)), MAX(INT(total_cases))/MAX(INT(population))*100 AS cases_per_person FROM data WHERE continent IS NOT NULL GROUP BY location ORDER BY cases_per_person DESC"
  }
  def query6():String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_tests)), (MAX(INT(total_cases))/MAX(INT(total_tests)))*100 as percent_positive FROM data WHERE continent IS NOT NULL GROUP BY location HAVING percent_positive < 95 ORDER BY percent_positive DESC"
  }
  
  def query7():String = {
    "SELECT location, MAX(INT(people_vaccinated)), MAX(INT(population)), AVG(data.gdp_per_capita) AS gdp, (MAX(INT(people_vaccinated))/MAX(INT(population)))*100 AS vaccinated FROM data GROUP BY location order by vaccinated desc"
  }


  def query8():String = {
    "SELECT data.location, AVG(data.population_density) AS density, AVG(data.gdp_per_capita) AS gdp, AVG(rate.rate) AS rate FROM data JOIN rate ON data.location = rate.location GROUP BY data.location ORDER BY rate DESC"
  }

  def query9():String = {
    "SELECT location, cast(MAX(median_age) AS decimal(8,5)) AS age, MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS survival FROM data WHERE location != 'North Korea' GROUP BY location ORDER BY survival DESC"
  }

  def cleanQuery():String = {
    "SELECT tmp.location, peak/(datediff(tmp.date, t5.First_Date)) AS rate FROM tmp JOIN (SELECT location, MAX(new_cases) AS peak FROM tmp GROUP BY location) AS t1 ON (tmp.location = t1.location AND tmp.new_cases = t1.peak)"+
      " INNER JOIN (SELECT location, to_date(MIN(date)) AS First_Date FROM (SELECT location, date FROM tmp WHERE new_cases >= 1) GROUP BY location) AS t5 ON t1.location = t5.location"
  }

  def query10():String = {
    "SELECT t1.location, Pre_Vaccine, Post_Vaccine, (Post_Vaccine-Pre_Vaccine) AS diff FROM "+
      "(SELECT location, (New_cases_after_vacc / total2) AS Post_Vaccine FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_after_vacc, COUNT(location) AS total2 "+
      "FROM data WHERE continent IS NOT NULL AND NOT new_vaccinations_smoothed = 'tests performed' AND new_vaccinations_smoothed IS NOT null AND date < '2022-06-01' GROUP BY location)) AS t1 "+
      "INNER JOIN " +
      "(SELECT location, (New_cases_before_vacc / total) AS Pre_Vaccine FROM "+
      "(SELECT location,  SUM(INT(new_cases)) AS New_cases_before_vacc, COUNT(location) AS total "+
      "FROM data WHERE continent IS NOT NULL AND new_vaccinations_smoothed IS null AND date < '2022-06-01' GROUP BY location)) AS t2 "+
      "ON t1.location = t2.location ORDER BY diff DESC LIMIT 20"
  }

}
