object Query7 {
  def query7():String = {
    "Select location, cast((people_vaccinated/population)*100 as decimal(8,5)) As Vaccinated from owid where date='2022-07-10' order by Vaccinated desc"
  }
}
