object Query9 {
  def query9:String = {
    "SELECT data.location, AVG(data.gdp_per_capita) AS gdp, AVG(rate.rate) AS rate, AVG(data.gdp_per_capita)/AVG(rate.rate) AS gdp_by_rate FROM data JOIN rate ON data.location = rate.location GROUP BY data.location"
  }
}
