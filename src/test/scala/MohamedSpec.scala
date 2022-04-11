import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class MohamedSpec extends AnyFlatSpec with should.Matchers {

  "colToUpper" should "read a csv file, turn it into a data frame, " +
    "create a list of column names from an input but ignore int type columns, " +
    "add upper case columns to the existing data frame named 'upper_colname'" in {
    import Mohamed._

    import spark.implicits._

    val df = loadDF("cities.csv")
    val cols = Seq("country", "city")

    val actual = colToUpper(df, cols)
    val expected = Seq(
      (0, "Warsaw", "Poland", "POLAND", "WARSAW"),
      (1, "Villeneuve-Loubet", "France", "FRANCE", "VILLENEUVE-LOUBET"),
      (2, "Vranje", "Serbia", "SERBIA", "VRANJE"),
      (3, "Pittsburgh", "US", "US", "PITTSBURGH")
    ).toDF("id", "city", "country", "upper_city", "upper_country")

    actual shouldBe expected
  }
}