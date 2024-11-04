import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustSpend {


  def main(args: Array[String]): Unit = {

    //Creating sparksession
    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val customers = List(
      ("karthik", "Premium", 1050, 32),
      ("neha", "Standard", 800, 28),
      ("priya", "Premium", 1200, 40),
      ("mohan", "Basic", 300, 35),
      ("ajay", "Standard", 700, 25),
      ("vijay", "Premium", 500, 45),
      ("veer", "Basic", 450, 33),
      ("aatish", "Standard", 600, 29),
      ("animesh", "Premium", 1500, 60),
      ("nishad", "Basic", 200, 21)
    ).toDF("name", "membership", "spending", "age")

    val df=customers.select(col("name"),col("membership"),col("spending"),
      when(col("spending")>1000 and col("membership")==="Premium","High Spender")
    .when(col("spending").between(500,1000) and col("membership")==="Standard","Average Spender")
        .otherwise("Low Spender").alias("cust"))

    df.groupBy(col("membership")).agg(avg(col("spending"))).alias("avg").show()
  }

}