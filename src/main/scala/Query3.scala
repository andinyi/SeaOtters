object Query3 {
  def query3:String = {
    "SELECT data.location, data.new_cases as peak, data.date FROM data JOIN (SELECT location, MAX(new_cases) AS peak FROM data GROUP BY location) AS t1 ON (data.location = t1.location AND data.new_cases = t1.peak)"
  }
}
