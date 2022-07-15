object Query1 {
  def query1:String = {
    "SELECT total_cases, total_deaths, (total_cases - total_deaths) as total_recovered, population FROM raw WHERE date = '2022-07-10' AND location = 'World'"
  }
}
