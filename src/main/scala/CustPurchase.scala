import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustPurchase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val customerPurchases = List(
      ("karthik", "Premium", 50, 5000),
      ("neha", "Standard", 10, 2000),
      ("priya", "Premium", 65, 8000),
      ("mohan", "Basic", 90, 1200),
      ("ajay", "Standard", 25, 3500),
      ("vijay", "Premium", 15, 7000),
      ("veer", "Basic", 75, 1500),
      ("aatish", "Standard", 45, 3000),
      ("animesh", "Premium", 20, 9000),
      ("nishad", "Basic", 80, 1100)
    ).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")

    val df=customerPurchases.select(col("name"),col("membership"),col("total_purchase_amount"),
      when(col("days_since_last_purchase")<30,"Frequent").
        when(col("days_since_last_purchase")<60,"Occasional").
        when(col("days_since_last_purchase")>60,"Rare").alias("frequency"))

    val df1=df.groupBy(col("frequency"),col("membership")).count()

    val df2=df.filter(col("frequency")==="Frequent" && col("membership")==="Premium")
      .groupBy(col("frequency"),col("membership")).agg(avg(col("total_purchase_amount")).alias("total_amt"))

    val df3=df.filter(col("frequency")==="Rare").groupBy(col("membership")).agg(min(col("total_purchase_amount")).alias("min"))

    df.show()
    df2.show()
    df3.show()

  }
}