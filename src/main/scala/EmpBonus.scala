import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, when}

object EmpBonus {

  def main(args:Array[String]):Unit= {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()


import spark.implicits._

  val employees1= List(
    ("karthik", "Sales", 85),
    ("neha", "Marketing", 78),
    ("priya", "IT", 90),
    ("mohan", "Finance", 65),
    ("ajay", "Sales", 55),
    ("vijay", "Marketing", 82),
    ("veer", "HR", 72),
    ("aatish", "Sales", 88),
    ("animesh", "Finance", 95),
    ("nishad", "IT", 60)
    ).toDF("name","department","performance_score")

   val df=employees1.select(col("name"),col("department"),col("performance_score"),
      when((col("department")==="Sales" || col("department")==="Marketing") &&
        col("performance_score")>80,col("performance_score")*0.2).
        when((col("department")=!="Sales" || col("department")=!="Marketing")
          && col("performance_score")>70,col("performance_score")*0.15).
        otherwise(0).alias("bonus_cal"))


    df.groupBy(col("department")).agg(sum("bonus_cal")).alias("total_cal").show()



}
}