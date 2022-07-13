object Query4{
  def query4():String = {
    "Select location, cast((total_deaths/population)*100 as decimal(8,5)) As death from owid where date='2022-07-10' order by death desc"
  }
}
