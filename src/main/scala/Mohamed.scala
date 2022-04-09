import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Mohamed {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()


  def colToUpper(path: String, col: String) = {
    val df: DataFrame = spark.read.options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true")).csv(path)
    val listCols = col.split("\\W+").filter(df.schema(_).dataType.typeName == "string")
    df.select(df("*") +: listCols.map(col => upper(df(col)).as(s"upper_$col")) : _*)
  }

}
