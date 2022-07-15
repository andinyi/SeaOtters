object Query8 {
  def query8:String = {
    "SELECT data.location, AVG(data.population_density) AS density, AVG(rate.rate) AS rate, AVG(data.population_density)/AVG(rate.rate) AS density_by_rate FROM data JOIN rate ON data.location = rate.location GROUP BY data.location"
  }
}
