import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, initcap, sum, when}

object SalesPerformance {
def main(args:Array[String]):Unit={

  val conf=new SparkConf()
  conf.set("spark.app.name","Sri").set("spark.master","local[*]")

val spark=SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  val sales = List(
    ("karthik", 60000),
    ("neha", 48000),
    ("priya", 30000),
    ("mohan", 24000),
    ("ajay", 52000),
    ("vijay", 45000),
    ("veer", 70000),
    ("aatish", 23000),
    ("animesh", 15000),
    ("nishad", 8000),
    ("varun", 29000),
    ("aadil", 32000)
  ).toDF("name", "total_sales")

val df=sales.select(initcap(col("name")).alias("name"),col("total_sales"),
  when(col("total_sales")>50000,"Excellent")
    .when(col("total_sales")>=25000 and col("total_sales")<=50000,"Good")
    .when(col("total_sales")<25000,"Needs Improvement").alias("performance_status"))

  val df1=df.groupBy(col("performance_status")).agg(sum(col("total_sales")).alias("tot"))

  df1.show()



}
}
