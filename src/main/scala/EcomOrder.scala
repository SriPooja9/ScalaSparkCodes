import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, when}

object EcomOrder {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      ("Order1", "Laptop", "Domestic", 2),
      ("Order2", "Shoes", "International", 8),
      ("Order3", "Smartphone", "Domestic", 3),
      ("Order4", "Tablet", "International", 5),
      ("Order5", "Watch", "Domestic", 7),
      ("Order6", "Headphones", "International", 10),
      ("Order7", "Camera", "Domestic", 1),
      ("Order8", "Shoes", "International", 9),
      ("Order9", "Laptop", "Domestic", 6),
      ("Order10", "Tablet", "International", 4)
    ).toDF("order_id", "product_type", "origin", "delivery_days")

    val df=orders.select(col("order_id"),col("product_type"),
      when(col("delivery_days") > 7 and col("origin") === "International", "Delayed")
        .when(col("delivery_days").between(3,7), "On time")
        .when(col("delivery_days")<3, "Fast")
        .alias("order"))

      df.groupBy(col("product_type"),col("order")).agg(countDistinct(col("order"))).alias("cnt").show()



  }
}