import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Vehicle {
  def main(args: Array[String]): Unit = {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val vehicles = List(
      ("CarA", 30),
      ("CarB", 22),
      ("CarC", 18),
      ("CarD", 15),
      ("CarE", 10),
      ("CarF", 28),
      ("CarG", 12),
      ("CarH", 35),
      ("CarI", 25),
      ("CarJ", 16)
    ).toDF("vehicle_name", "mileage")

    vehicles.select(col("vehicle_name"),
    when(col("mileage") > 25, "High Efficiency").when(col("mileage") >= 15 and col("mileage") <= 25, "Moderate")
      .when(col("mileage")<15,"Low Efficiency").alias("analysis")).show()

    spark.stop()

  }
}