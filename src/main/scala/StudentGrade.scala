import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, when}

object StudentGrade {
  def main(args:Array[String]):Unit= {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val students = List(
      ("karthik", 95),
      ("neha", 82),
      ("priya", 74),
      ("mohan", 91),
      ("ajay", 67),
      ("vijay", 80),
      ("veer", 85),
      ("aatish", 72),
      ("animesh", 90),
      ("nishad", 60)
    ).toDF("name", "score")

    val df=students.select(col("name"),
      when(col("score")>=90,"Excellent").when(col("score")>=75 and col("score")<=89,"Good")
        .when(col("score")<75,"Needs Improvement").alias("grade"))

    df.groupBy(col("grade")).agg(countDistinct(col("name")).alias("count")).show()

  }
}
