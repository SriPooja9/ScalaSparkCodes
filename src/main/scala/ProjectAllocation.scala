import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, initcap, sum, when}

object ProjectAllocation {
def main(args:Array[String]):Unit= {

  val conf=new SparkConf()
  conf.set("spark.app.name","Sri").set("spark.master","local[*]")

  val spark=SparkSession.builder().config(conf).getOrCreate()

  import spark.implicits._

  val workload = List(
    ("karthik", "ProjectA", 120),
    ("karthik", "ProjectB", 100),
    ("neha", "ProjectC", 80),
    ("neha", "ProjectD", 30),
    ("priya", "ProjectE", 110),
    ("mohan", "ProjectF", 40),
    ("ajay", "ProjectG", 70),
    ("vijay", "ProjectH", 150),
    ("veer", "ProjectI", 190),
    ("aatish", "ProjectJ", 60),
    ("animesh", "ProjectK", 95),
    ("nishad", "ProjectL", 210),
    ("varun", "ProjectM", 50),
    ("aadil", "ProjectN", 90)
  ).toDF("name", "project", "hours")

  val df=workload.groupBy(col("name").alias("name")).agg(sum(col("hours")).alias("hours"))

  val df1=df.select(initcap(col("name")),
    when(col("hours")>200,"Overloaded")
      .when(col("hours")>=100 and col("hours")<=200,"Balanced").otherwise("Underutilized").alias("workload_level"))

  val df2=df1.groupBy(col("workload_level")).count()

  df2.show()
}
}
