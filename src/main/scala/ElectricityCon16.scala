import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, countDistinct, when}

object ElectricityCon16 {

  def main(args:Array[String]):Unit={

    val conf=new SparkConf()
    conf.set("spark.app.name","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val electricityUsage = List(
      ("House1", 550, 250),
      ("House2", 400, 180),
      ("House3", 150, 50),
      ("House4", 500, 200),
      ("House5", 600, 220),
      ("House6", 350, 120),
      ("House7", 100, 30),
      ("House8", 480, 190),
      ("House9", 220, 105),
      ("House10", 150, 60)
    ).toDF("household", "kwh_usage", "total_bill")

    val df=electricityUsage.select($"kwh_usage",$"household",$"total_bill",
      when($"kwh_usage">500 and $"total_bill">200,"High Usage")
        .when($"kwh_usage".between(200,500) and $"total_bill".between(100,200),"Medium Usage")
        .otherwise("Low Usage").alias("category")
    )

    val df1=df.groupBy($"category").agg(countDistinct($"household").alias("total"))

val df2=df.filter($"category"==="High Usage" or $"category"==="Medium Usage")
  .groupBy($"category").agg(functions.max($"total_bill").alias("max"),avg($"kwh_usage").alias("avg"))
  .select($"category",when($"category"==="High Usage",$"max").when($"category"==="Medium Usage",$"avg").alias("new")
  )

    val df3=df.filter($"category"==="Low Usage" and $"kwh_usage">300).groupBy($"category").count().select($"category")

    df.show()
    df1.show()
    df2.show()
    df3.show()

    spark.stop()

  }

}
