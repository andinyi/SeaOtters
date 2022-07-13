object Query3 {
  def query3:String = {
    //org.apache inter type
    /**How much time does it take between the first confirmed case and the day with the highest confirmed cases for each country?**/

    "SELECT location, MIN(total_cases) FROM query3 WHERE total_cases = 1 GROUP BY location"
    /**Gets the date of first coivd**/
    "SELECT location, MIN(date) AS First_Date FROM (SELECT location, date FROM query3 WHERE new_cases >= 1) GROUP BY location"
    /****/
    //"SELECT location, maxy, datey FROM (SELECT MAX(total_cases) AS maxy, location, MIN(date) AS datey FROM query3 GROUP BY location)"
    //"SELECT location, MIN(date) AS First_Date FROM (SELECT location, date FROM query3 WHERE new_cases = (SELECT MAX(new_cases) FROM query3 GROUP BY location) GROUP BY location"
    //"SELECT location, MIN(date) AS First_Date FROM (SELECT location, date FROM query3 WHERE new_cases = 1) GROUP BY location"
    //"SELECT location, MIN(date), MAX(new_cases) FROM query3 GROUP BY location"
    //"SELECT location, MIN(date) FROM query3 WHERE new_cases = (SELECT MAX(new_cases) FROM query3 GROUP BY location)"
   // "SELECT MAX(new_cases) FROM query3 GROUP BY location"
    //"SELECT location, MAX(total_cases) FROM query3 GROUP BY location"
    //"SELECT location, MIN(date) FROM query3 WHERE total_cases = 6 GROUP BY location"
/*
    "SELECT MAX(new_cases) AS MC FROM query3 GROUP BY location) AS t1 "+
    "LEFT JOIN "+
      "SELECT location, MIN(date) FROM query3 AS t2 "+
    "ON (t1.MC = t2.new_cases)"
*/
    "SELECT location, MIN(new_cases), MIN(date) FROM query3 GROUP BY location"
    "SELECT location, MIN(date) AS First_Date FROM (SELECT location, date FROM query3 WHERE new_cases >= 1) GROUP BY location"
    "(SELECT location, MIN(date) FROM query3 GROUP BY location) UNION ALL (SELECT location, MAX(new_cases) FROM query3 GROUP BY location)"
    "SELECT location, MAX(int(new_cases)) AS peak, MIN(date) as date FROM query3 GROUP BY location"


    //"SELECT location, min(date), CASE WHEN new_cases >= 1 THEN 'test1' END FROM query3 GROUP BY location"


  }
}
