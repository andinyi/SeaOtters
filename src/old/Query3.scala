object Query3 {
  def query3:String = {
    "SELECT data.location, data.new_cases as peak, t5.First_Date AS testDate, data.date AS Peak_Date, datediff(data.date, t5.First_Date) AS DIFFDATES, peak/(datediff(data.date, t5.First_Date)) AS rate FROM data JOIN (SELECT location, MAX(new_cases) AS peak FROM data GROUP BY location) AS t1 ON (data.location = t1.location AND data.new_cases = t1.peak)"+
      " INNER JOIN (SELECT location, to_date(MIN(date)) AS First_Date FROM (SELECT location, date FROM data WHERE new_cases >= 1) GROUP BY location) AS t5 ON t1.location = t5.location"
  }
}
