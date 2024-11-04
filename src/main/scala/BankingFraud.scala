import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BankingFraud {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sri")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val transactions = List(
      ("Account1", "2024-11-01", 12000, 6, "Savings"),
      ("Account2", "2024-11-01", 8000, 3, "Current"),
      ("Account3", "2024-11-02", 2000, 1, "Savings"),
      ("Account4", "2024-11-02", 15000, 7, "Savings"),
      ("Account5", "2024-11-03", 9000, 4, "Current"),
      ("Account6", "2024-11-03", 3000, 1, "Current"),
      ("Account7", "2024-11-04", 13000, 5, "Savings"),
      ("Account8", "2024-11-04", 6000, 2, "Current"),
      ("Account9", "2024-11-05", 20000, 8, "Savings"),
      ("Account10", "2024-11-05", 7000, 3, "Savings")
    ).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")

    val df=transactions.select(col("account_id"),col("frequency"),col("amount"),col("account_type"),
      when(col("amount")>10000 and col("frequency")>5,"High Risk")
        .when(col("amount").between(5000,10000) and col("frequency").between(2,5),"Moderate Risk")
        .otherwise("Low Risk").alias("level"))

      val df1=df.groupBy(col("level")).agg(sum("frequency"))

    val df2=df.filter(col("level")==="High Risk").groupBy(col("level"),col("account_id")).agg(sum(col("amount")).alias("total"))

    val df3=df.filter(col("level")==="Moderate Risk" and col("account_type")==="Savings" and col("amount")>7500)

      df1.show()
      df2.show()
      df3.show()
  }

}