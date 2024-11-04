import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, when}

object ProductInven {
  def main(args:Array[String]):Unit= {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inventory = List(
      ("ProductA", 120),
      ("ProductB", 95),
      ("ProductC", 45),
      ("ProductD", 200),
      ("ProductE", 75),
      ("ProductF", 30),
      ("ProductG", 85),
      ("ProductH", 100),
      ("ProductI", 60),
      ("ProductJ", 20)
    ).toDF("product_name", "stock_quantity")

    val df=inventory.select(col("product_name"),col("stock_quantity"),
      when(col("stock_quantity")>100,"Overstocked")
    .when(col("stock_quantity")>=50 and col("stock_quantity")<=100,"Normal")
    .when(col("stock_quantity")<50,"Low Stock").alias("class_inven"))

    df.groupBy(col("class_inven")).agg(sum(col("stock_quantity")).alias("total_quantity")).show()
  }

}
