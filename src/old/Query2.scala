
object Query2 {
  def query2:String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_deaths)), (MAX(INT(total_cases))-MAX(INT(total_deaths)))/MAX(INT(total_cases))*100 AS percent_survived FROM dataView WHERE continent IS NOT NULL GROUP BY location"
  }


}
