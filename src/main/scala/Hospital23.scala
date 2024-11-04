import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{when, _}

object Hospital23 {

  def main(args:Array[String]):Unit={

    val spark=SparkSession.builder().appName("Sri").master("local[*]").getOrCreate()

    import spark.implicits._

    val patients = List(
      ("Patient1", 62, 10, 3, "ICU"),
      ("Patient2", 45, 25, 1, "General"),
      ("Patient3", 70, 8, 2, "ICU"),
      ("Patient4", 55, 18, 3, "ICU"),
      ("Patient5", 65, 30, 1, "General"),
      ("Patient6", 80, 12, 4, "ICU"),
      ("Patient7", 50, 40, 1, "General"),
      ("Patient8", 78, 15, 2, "ICU"),
      ("Patient9", 40, 35, 1, "General"),
      ("Patient10", 73, 14, 3, "ICU")
    ).toDF("patient_id", "age", "readmission_interval", "icu_admissions", "admission_type")

    val df=patients.select(col("patient_id"),col("readmission_interval"),col("admission_type"),col("icu_admissions"),
      when(col("readmission_interval")<15 and col("age")>60,"High Readmission Risk")
        .when(col("readmission_interval").between(15,30),"Moderate Risk")
        .otherwise("Low Risk").alias("patient_read"))

val df1=df.groupBy(col("patient_read")).count()

    val df2=df.filter(col("patient_read")==="High Readmission Risk").groupBy(col("patient_read"))
      .agg(avg(col("readmission_interval")).alias("avg"))

    val df3=df.filter(col("patient_read")==="Moderate Risk" and col("admission_type")==="ICU")
      .groupBy(col("patient_id")).agg(sum(col("icu_admissions")).alias("sum")).filter(col("sum")>2)

    df.show()
    df1.show()
    df2.show()
    df3.show()
  }


}
