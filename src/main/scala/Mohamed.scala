import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


object Mohamed extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val path = if (args.length > 0) args(0) else "cities.csv"
  val df = loadDF(path)

  def colToUpper(df: DataFrame, cols: Seq[String]) = {
    val listCols = cols.filter(df.schema(_).dataType == StringType)
    listCols.foldLeft(df) { (result, s) =>
       result.withColumn(s"upper_$s", upper(df(s)))
    }
//    df.select(df("*") +: listCols.map(col => upper(df(col)).
//      as(s"upper_$col")): _*)
  }

  def loadDF(path: String): DataFrame = {
    spark.read.options(
      Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .csv(path)
  }
  colToUpper(df, Seq("country", "city")).show
}
