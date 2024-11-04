
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when, _}

object EmpProduct21 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sri").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val employeeProductivity = List(
      ("Emp1", 85, 6),
      ("Emp2", 75, 4),
      ("Emp3", 40, 1),
      ("Emp4", 78, 5),
      ("Emp5", 90, 7),
      ("Emp6", 55, 3),
      ("Emp7", 80, 5),
      ("Emp8", 42, 2),
      ("Emp9", 30, 1),
      ("Emp10", 68, 4)
    ).toDF("employee_id", "productivity_score", "project_count")

    val df=employeeProductivity.select(col("productivity_score"),col("employee_id"),col("project_count"),
      when(col("productivity_score")>80 and col("project_count")>5,"High Performer")
        .when(col("productivity_score").between(60,80),"Average Performer")
        .otherwise("Low Performer").alias("performance"))

    val df1=df.groupBy(col("performance")).agg(countDistinct(col("employee_id")).alias("cnt"))
    val df2=df.filter(col("performance")==="High Performer" or col("performance")==="Average Performer")
      .groupBy(col("performance"))
      .agg(avg(col("productivity_score")).alias("avg"),min(col("productivity_score")).alias("min"))

    val df3=df2.select(col("performance"),
      when(col("performance")==="High Performer",col("avg")).otherwise(col("min")).alias("new"))

    val df4=df.filter(col("performance")==="Low Performer" and col("productivity_score")<50 and col("project_count")>2)
      .select(col("performance"),col("productivity_score"),
      col("project_count"))


    df.show()
    df2.show()
    df3.show()
    df4.show()

  }

}