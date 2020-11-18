import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{sum, desc}

object WorkingWithParquet {
  def computeEachGroupPercentage (DF : DataFrame, totalNumber : Long)  = {
    DF.foreach ((row) => {

      var groupName = ""
      for (i <- 0 to row.length - 2) {
        val name = row.get (i).toString ()

        if (name == "true") {
          groupName += "passed "
        } else if (name == "false") {
          groupName += "failed "
        } else {
          groupName += name + " "
        }
      }

      println ("Percentage of " + groupName + "is: " + (row.getLong (row.length - 1) * 1.0 / totalNumber))
    })
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("SecondTask")
      .config("spark.sql.warehouse.dir","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel ("ERROR")

    val studentsPerformanceSchema = new StructType ()
      .add ("gender", StringType, true)
      .add ("race", StringType, true)
      .add ("parental_level_of_education", StringType, true)
      .add ("lunch", StringType, true)
      .add ("test_preparation_course", StringType, true)
      .add ("math_score", IntegerType, true)
      .add ("reading_score", IntegerType, true)
      .add ("writing_score", IntegerType, true)

    val studentsPerformanceCSV = spark.read
      .option ("header", "true")
      .option ("inferSchema", "false")
      .option ("nullValue", "null")
      .schema (studentsPerformanceSchema)
      .csv ("StudentsPerformance.csv")

    studentsPerformanceCSV.show ()

    // Saving loaded csv file to a Hive Parquet Table
    studentsPerformanceCSV.write.mode ("overwrite").format ("parquet").saveAsTable ("hive_test_db.students_performance_table")

    // Reading Table as a DataFrame
    val studentsPerformanceDF = spark.sql ("SELECT * FROM hive_test_db.students_performance_table")
    studentsPerformanceDF.printSchema()
    studentsPerformanceDF.show ()
    studentsPerformanceDF.cache ()

    val totalNumberOfStudents = studentsPerformanceDF.count ()

    // Relation between Gender and Math Performance
    val studentsGroupedByGender = studentsPerformanceDF
      .select (studentsPerformanceDF ("math_score") >= 50, studentsPerformanceDF ("gender"))
      .groupBy ("gender", "(math_score >= 50)")
      .count ()

    studentsGroupedByGender.show ()
    computeEachGroupPercentage (studentsGroupedByGender, totalNumberOfStudents)

    // Relation between Race and Math Performance
    val studentsGroupedByRace = studentsPerformanceDF
      .select (studentsPerformanceDF ("math_score") >= 50, studentsPerformanceDF ("race"))
      .groupBy ("race", "(math_score >= 50)")
      .count ()

    studentsGroupedByRace.show ()
    computeEachGroupPercentage (studentsGroupedByRace, totalNumberOfStudents)

    // Relation between Lunch and Math Performance
    val studentsGroupedByLunch = studentsPerformanceDF
      .select (studentsPerformanceDF ("math_score") >= 50, studentsPerformanceDF ("lunch"))
      .groupBy ("lunch", "(math_score >= 50)")
      .count ()

    studentsGroupedByLunch.show ()
    computeEachGroupPercentage (studentsGroupedByLunch, totalNumberOfStudents)

    // Relation between Parental Education Level and Math Performance
    val studentsGroupedByParentalEducation = studentsPerformanceDF
      .select (studentsPerformanceDF ("parental_level_of_education"), studentsPerformanceDF ("math_score") >= 50)
      .groupBy ("parental_level_of_education", "(math_score >= 50)")
      .count ()

    studentsGroupedByParentalEducation.show ()
    computeEachGroupPercentage (studentsGroupedByParentalEducation, totalNumberOfStudents)

    // Relation between test_preparation_course and Math Performance
    val studentsGroupedByTestPrep = studentsPerformanceDF
      .select (studentsPerformanceDF ("test_preparation_course"), studentsPerformanceDF ("math_score") >= 50)
      .groupBy ("test_preparation_course", "(math_score >= 50)")
      .count ()

    studentsGroupedByTestPrep.show ()
    computeEachGroupPercentage (studentsGroupedByTestPrep, totalNumberOfStudents)

    // Relation between Having Lunch with Test Preparation and Math Performance
    studentsPerformanceDF
      .select (studentsPerformanceDF ("lunch"), studentsPerformanceDF ("test_preparation_course"), studentsPerformanceDF ("math_score") >= 50)
      .groupBy ("lunch", "test_preparation_course", "(math_score >= 50)")
      .count ()
      .show ()

    // Retrieve top 10 students and last 10 students
    studentsPerformanceDF
      .orderBy ("math_score")
      .limit (10)
      .show ()

    studentsPerformanceDF
      .orderBy (desc ("math_score"))
      .limit (10)
      .show ()

    // Retrieve average of math_scores
    val studentsMarksSum = studentsPerformanceDF
      .agg (sum ("math_score"))
      .first ()
      .getLong (0)

    println ("Average of math_score between students: " + (studentsMarksSum * 1.0 / totalNumberOfStudents))

    studentsPerformanceDF
      .orderBy (desc ("math_score") , desc ("reading_score"), desc ("writing_score"))
      .limit (1)
      .show ()


    studentsPerformanceDF.createOrReplaceTempView ("students")

    // Top 3 female students from each parental level of education using OVER () and PARTITION BY Clause divided to groups but ordered
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM ( " +
      "SELECT gender, parental_level_of_education, math_score, row_number () OVER (PARTITION BY parental_level_of_education ORDER BY math_score DESC) row_num " +
      "FROM students " +
      "WHERE gender=\"female\" ) " +
      "WHERE row_num <= 3 " +
      "ORDER BY math_score DESC").show ()

    // Top 3 female students from each parental level of education using OVER () and PARTITION BY Clause divided to groups but not ordered
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM ( " +
      "SELECT gender, parental_level_of_education, math_score, row_number () OVER (PARTITION BY parental_level_of_education ORDER BY math_score DESC) row_num " +
      "FROM students " +
      "WHERE gender=\"female\" ) " +
      "WHERE row_num <= 3 ").show ()

    // Top 3 students who scored in math more than in reading and less than in writing
    spark.sql ("SELECT * FROM students WHERE math_score > reading_score AND math_score < writing_score ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 students in math, reading, and writing
    spark.sql ("SELECT * FROM students ORDER BY math_score DESC, reading_score DESC, writing_score DESC LIMIT 3").show ()

    // Top 3 students in math, reading, and writing
    spark.sql ("SELECT *, (math_score + reading_score + writing_score) as total_score_sum FROM students ORDER BY total_score_sum DESC LIMIT 3").show ()

    spark.stop ()
  }
}