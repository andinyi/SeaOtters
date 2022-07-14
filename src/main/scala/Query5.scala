object Query5 {
  def query5:String = {
    "SELECT location, MAX(INT(total_cases)), MAX(INT(population)), MAX(INT(total_cases))/MAX(INT(population)) AS cases_per_person FROM dataView WHERE continent IS NOT NULL GROUP BY location"
  }
}
