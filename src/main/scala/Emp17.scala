  import org.apache.spark.SparkConf
  import org.apache.spark.sql.{SparkSession, functions}
  import org.apache.spark.sql.functions.{avg, countDistinct, when}

  object Emp17
  {

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf()
      conf.set("spark.app.name", "Sri").set("spark.master", "local[*]")

      val spark = SparkSession.builder().config(conf).getOrCreate()

      import spark.implicits._
      val employees = List(
        ("karthik", "IT", 110000, 12, 88),
        ("neha", "Finance", 75000, 8, 70),
        ("priya", "IT", 50000, 5, 65),
        ("mohan", "HR", 120000, 15, 92),
        ("ajay", "IT", 45000, 3, 50),
        ("vijay", "Finance", 80000, 7, 78),
        ("veer", "Marketing", 95000, 6, 85),
        ("aatish", "HR", 100000, 9, 82),
        ("animesh", "Finance", 105000, 11, 88),
        ("nishad", "IT", 30000, 2, 55)
      ).toDF("name", "department", "salary", "experience", "performance_score")

      val df=employees.select($"department",$"performance_score",$"experience",
        when($"salary">100000 and $"experience">10,"Senior")
          .when($"salary".between(50000,100000) and $"experience".between(5,10),"Mid-Level")
          .otherwise("Junior").alias("salary_band")
      )

      val df1=df.groupBy($"department",$"salary_band").count().select($"department",$"salary_band",$"count")

      val df2=df.groupBy($"salary_band").agg(avg($"performance_score").alias("avg")).filter($"avg">80)
        .select($"salary_band",$"avg")

      val df3=df.filter($"salary_band"==="Mid-Level" and $"performance_score">85 and $"experience">7)
        .select($"salary_band",$"experience",$"performance_score")

      df.show()
      df1.show()
      df2.show()
      df3.show()
    }
  }