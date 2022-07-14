object Query10 {
  def query10():String = {
    "Select location, median_age,Round(((total_cases - total_deaths)/population)*100, 4) as recovered from owid_tb where date = '2022-07-10'"
  }
}
