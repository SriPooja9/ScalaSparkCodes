import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, initcap, when}

object CustomerAge {
  def main(args: Array[String]): Unit = {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import  spark.implicits._

    val customers = List(
      ("karthik", 22),
      ("neha", 28),
      ("priya", 40),
      ("mohan", 55),
      ("ajay", 32),
      ("vijay", 18),
      ("veer", 47),
      ("aatish", 38),
      ("animesh", 60),
      ("nishad", 25)
    ).toDF("name", "age")

    val df=customers.select(initcap(col("name")).alias("name"),
      when(col("age")<24,"Youth").when(col("age")>=25 and col("age")<=45,"Adult")
        .when(col("age")>45,"Senior").alias("cust"))

    //df.groupBy(col("cust")).agg(countDistinct(col("name"))).show()
    val df2=df.groupBy(col("cust")).count()
    df2.show()


  }
}