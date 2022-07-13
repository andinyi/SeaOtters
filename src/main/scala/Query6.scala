object Query6 {
  def query3:String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(total_tests)), (MAX(INT(total_cases))/MAX(INT(total_tests)))*100 as percent_positive FROM dataView WHERE continent IS NOT NULL GROUP BY location"
  }
}
