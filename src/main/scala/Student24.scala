import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Student24 {

  def main(args: Array[String]): Unit = {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

import spark.implicits._

    val students = List(
      ("Student1", 70, 45, 60, 65, 75),
      ("Student2", 80, 55, 58, 62, 67),
      ("Student3", 65, 30, 45, 70, 55),
      ("Student4", 90, 85, 80, 78, 76),
      ("Student5", 72, 40, 50, 48, 52),
      ("Student6", 88, 60, 72, 70, 68),
      ("Student7", 74, 48, 62, 66, 70),
      ("Student8", 82, 56, 64, 60, 66),
      ("Student9", 78, 50, 48, 58, 55),
      ("Student10", 68, 35, 42, 52, 45)
    ).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score",
      "history_score")


    val df=students.select(col("student_id"),col("attendance_percentage"),
      ((col("math_score")+col("science_score")+col("english_score")+ col("history_score"))/4).alias("average_testscore"))

    val df1=df.select($"average_testscore",col("student_id"),when($"attendance_percentage"<75 and $"average_testscore"<50,"At Risk")
    .when($"attendance_percentage".between(75,85),"Moderate Risk")
      .otherwise("Low Risk").alias("risk_level"))

    df1.groupBy($"risk_level").agg(countDistinct($"student_id"))

    val df2=df1.filter($"risk_level"==="At Risk").groupBy($"risk_level").agg(avg($"average_testscore")).show()

  }


}