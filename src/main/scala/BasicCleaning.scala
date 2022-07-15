import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DecimalType}

class BasicCleaning {
  def basicCleaning(in:DataFrame):DataFrame = {
    var df:DataFrame = in
    df = df.withColumn("total_cases", col("total_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("total_deaths", col("total_deaths").cast(DecimalType(18, 1)))
    df = df.withColumn("new_cases", col("new_cases").cast(DecimalType(18, 1)))
    df = df.withColumn("population", col("population").cast(DecimalType(18, 1)))
    df = df.withColumn("date", col("date").cast(DateType))
    df
  }
}
