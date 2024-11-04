import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, countDistinct, when}

object CusLoyalty19 {

  def main(args:Array[String]):Unit={

    val conf=new SparkConf()
    conf.set("spark.app.name","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val customerLoyalty = List(
      ("Customer1", 25, 700),
      ("Customer2", 15, 400),
      ("Customer3", 5, 50),
      ("Customer4", 18, 450),
      ("Customer5", 22, 600),
      ("Customer6", 2, 80),
      ("Customer7", 12, 300),
      ("Customer8", 6, 150),
      ("Customer9", 10, 200),
      ("Customer10", 1, 90)
    ).toDF("customer_name", "purchase_frequency", "average_spending")

    val df=customerLoyalty.select(col("customer_name"),$"average_spending",$"purchase_frequency",
      when($"purchase_frequency">20 and $"average_spending">500,"High Loyal")
    .when($"purchase_frequency".between(10,20),"Moderately Loyal").otherwise("Low Loyal").alias("loyal"))

    val df1=df.groupBy($"loyal").count()
val df2=df.filter($"loyal"==="High Loyal" or $"loyal"==="Moderately Loyal").select($"loyal",$"average_spending").
groupBy($"loyal").agg(avg($"average_spending").alias("avg"),functions.min($"average_spending").alias("min"))
  .select($"loyal",when($"loyal"==="High Loyal",$"avg").otherwise($"min").alias("category"))

val df3=df.filter($"loyal"==="Low Loyal" and $"average_spending"<100 and $"purchase_frequency"<5)
  .select($"loyal",$"average_spending",$"purchase_frequency")

  df2.show()
    df3.show()

  }


}
