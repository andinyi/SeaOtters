
import org.apache.spark.sql.{DataFrame, SaveMode}

class owidSplitting {
  def owidSplit(df: DataFrame): Unit = {
    val df1 = df.repartition(2)
    df1.write.mode(SaveMode.Overwrite).option("header", "true").csv("./datasets/batchedOwidHalf/")
  }
}
