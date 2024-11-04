import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, when}

object Finance {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val loanApplicants = List(
      ("karthik", 60000, 120000, 590),
      ("neha", 90000, 180000, 610),
      ("priya", 50000, 75000, 680),
      ("mohan", 120000, 240000, 560),
      ("ajay", 45000, 60000, 620),
      ("vijay", 100000, 100000, 700),
      ("veer", 30000, 90000, 580),
      ("aatish", 85000, 85000, 710),
      ("animesh", 50000, 100000, 650),
      ("nishad", 75000, 200000, 540)
    ).toDF("name", "income", "loan_amount", "credit_score")

    val df=loanApplicants.select(col("loan_amount"),col("loan_amount"),col("credit_score"),
      when(col("loan_amount")>col("income")*2 and col("credit_score")<600,"High Risk")
        .when(col("loan_amount")>=col("income")*1 and col("loan_amount")<col("income")*2 and col("credit_score").between(600,700),"Moderate Risk")
        .otherwise("Low Risk").alias("Risk_Level"),
      when(col("income")<50000,"Low")
    .when(col("income").between(50000,100000),"Moderate")
    .otherwise("High").alias("Income_Range"))

    val df1=df.groupBy(col("Risk_Level")).count()

    val df2=df.groupBy(col("income_range"),col("risk_level")).agg(avg(col("loan_amount"))).filter(col("Risk_Level")==="High Risk")

    val df3=df.groupBy(col("income_range"),col("risk_level")).agg(avg(col("credit_score")).alias("avg"))

    df3.filter(col("avg")<650)

    df1.show()
    df2.show()
    df3.show()




  }
}