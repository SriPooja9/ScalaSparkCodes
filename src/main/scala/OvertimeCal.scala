import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, initcap, when}

object OvertimeCal {

  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
    conf.set("spark.app.name","Sri").set("spark.master","local[*]")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    val employees = List(
      ("karthik", 62),
      ("neha", 50),
      ("priya", 30),
      ("mohan", 65),
      ("ajay", 40),
      ("vijay", 47),
      ("veer", 55),
      ("aatish", 30),
      ("animesh", 75),
      ("nishad", 60)
    ).toDF("name", "hours_worked")

    val df=employees.select(initcap(col("name")).alias("name"),
      when(col("hours_worked")>60,"Excessive Overtime")
        .when(col("hours_worked")>=45 and col("hours_worked")<=60,"Standard Overtime")
        .when(col("hours_worked")<45,"No Overtime").alias("cal"))

    val df2=df.groupBy(col("cal")).agg(collect_list($"name"))

    df2.show()



  }

}
