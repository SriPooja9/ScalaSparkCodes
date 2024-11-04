import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, countDistinct, when}

object EcomReturn20 {

  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
    conf.set("spark.app.name","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()


    import spark.implicits._

    val ecommerceReturn = List(
      ("Product1", 75, 25),
      ("Product2", 40, 15),
      ("Product3", 30, 5),
      ("Product4", 60, 18),
      ("Product5", 100, 30),
      ("Product6", 45, 10),
      ("Product7", 80, 22),
      ("Product8", 35, 8),
      ("Product9", 25, 3),
      ("Product10", 90, 12)
    ).toDF("product_name", "sale_price", "return_rate")

    val df=ecommerceReturn.select($"product_name",$"sale_price",$"return_rate",
      when($"return_rate">20,"High Return").when($"return_rate".between(10,20),"Medium Return")
        .otherwise("Low Return").alias("category"))

    val df1=df.groupBy(col("category")).agg(countDistinct($"product_name").alias("cnt"))

    val df2=df.filter($"category"==="High Return" or $"category"==="Medium Return")
      .groupBy($"category").agg(avg($"sale_price").alias("avg"),functions.max($"return_rate").alias("max"))

    val df3=df2.select($"category",when($"category"==="High Return",col("avg")).otherwise(col("max")).alias("new"))

    val df4=df.filter($"category"==="Low Return" and $"sale_price"<50 and $"return_rate"<5)
      .select($"category",$"return_rate",$"sale_price")

    df1.show()
    df3.show()
    df4.show()

  }

}
