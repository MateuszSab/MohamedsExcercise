import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import scala.collection.mutable._
class MohamedSpec extends AnyFlatSpec with should.Matchers {

  "colToUpper" should "read a csv file, turn it into a data frame, " +
    "create a list of column names from an input but ignore int type columns, " +
    "add upper case columns to the existing data frame named 'upper_colname'" in {
    import Mohamed._

    colToUpper("cities.csv", "country,city").collect.map(_.toSeq) shouldBe ???
//      Array(WrappedArray(0, "Warsaw", "Poland", "POLAND", "WARSAW"),
//      WrappedArray(1, "Villeneuve-Loubet", "France", "FRANCE", "VILLENEUVE-LOUBET"),
//      WrappedArray(2, "Vranje", "Serbia", "SERBIA", "VRANJE"),
//      WrappedArray(3, "Pittsburgh", "US", "US", "PITTSBURGH"))


  }
}