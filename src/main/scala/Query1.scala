object Query1 {
  def query1:String = {
    "SELECT total_cases, total_deaths, (total_cases - total_deaths) as total_recovered, population FROM query1 WHERE date = '2022-07-10' AND location = 'World'"
  }
}
