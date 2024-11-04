import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, countDistinct, when}

object Product18 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.name", "Sri").set("spark.master", "local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val productSales = List(
      ("Product1", 250000, 5),
      ("Product2", 150000, 8),
      ("Product3", 50000, 20),
      ("Product4", 120000, 10),
      ("Product5", 300000, 7),
      ("Product6", 60000, 18),
      ("Product7", 180000, 9),
      ("Product8", 45000, 25),
      ("Product9", 70000, 15),
      ("Product10", 10000, 30)
    ).toDF("product_name", "total_sales", "discount")

    val df=productSales.select($"total_sales",$"product_name",$"discount",
      when($"total_sales">200000 and $"discount"<10,"Top Seller")
        .when($"total_sales".between(100000,200000),"Moderate Seller")
        .otherwise("Low Seller").alias("category")
    )

    val df1=df.groupBy($"category").count().select($"category",$"count")



    val df2=df.filter($"category"==="Top Seller" or $"category"==="Moderate Seller").groupBy($"category")
      .agg(functions.max($"total_sales").alias("max_sales"),functions.min($"discount").alias("min_discount"))
      .select($"category",when($"category"==="Top Seller",$"max_sales")
        .when($"category"==="Moderate Seller","min_discount").alias("new"))


    val df3=df.filter($"category"==="Low Seller" and $"total_sales"<50000 and $"discount">15)
      .select($"category",$"total_sales",$"discount")

    df1.show()
    df2.show()
    df3.show()
  }

}