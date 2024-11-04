import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, initcap, when}

object EmployeeStaus {
def main(args:Array[String]):Unit=
  {

    //Creating sparksession
    val spark=SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    //Data
    val employees = List(
      ("karthik", "2024-11-01"),
      ("neha", "2024-10-20"),
      ("priya", "2024-10-28"),
      ("mohan", "2024-11-02"),
      ("ajay", "2024-09-15"),
      ("vijay", "2024-10-30"),
      ("veer", "2024-10-25"),
      ("aatish", "2024-10-10"),
      ("animesh", "2024-10-15"),
      ("nishad", "2024-11-01"),
      ("varun", "2024-10-05"),
      ("aadil", "2024-09-30")
    ).toDF("name", "last_checkin")


    val df=employees.select(initcap(col("name")),
    when(datediff(current_date(),col("last_checkin"))<=7,"Active").otherwise("Inactive").alias("WorkStatus")
    )
    df.show()

   spark.stop()

  }
}
