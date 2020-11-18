import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructType}

object WorkingWithCSV {
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

  def main (args : Array[String]) = {
    val spark = SparkSession
      .builder ()
      .master ("local")
      .appName ("FirstTask")
      .getOrCreate ()

    spark.sparkContext.setLogLevel ("ERROR")

    val studentsPerformanceDF = spark.read
      .option ("header", "true")
      .option ("inferSchema", "true")
      .csv ("StudentsPerformance.csv")
      .cache ()

    studentsPerformanceDF.printSchema ()
    studentsPerformanceDF.show ()

    val totalNumberOfStudents = studentsPerformanceDF.count ()
    println ("Total number of student records is: " + totalNumberOfStudents)

    // I'm going to extract just the students who passed their Math Exams to make my analysis on them
    val passedStudents = studentsPerformanceDF.select (studentsPerformanceDF ("math score") >= 50).groupBy ("(math score >= 50)").count ()
    passedStudents.show ()
    computeEachGroupPercentage (passedStudents, totalNumberOfStudents)

    // Relation between Gender and Math Performance
    val studentsGroupedByGender = studentsPerformanceDF
      .select (studentsPerformanceDF ("math score") >= 50, studentsPerformanceDF ("gender"))
      .groupBy ("gender", "(math score >= 50)")
      .count ()

    //    studentsGroupedByGender.write.csv ("StudentsGroupedByGender.csv")
    studentsGroupedByGender.show ()
    computeEachGroupPercentage (studentsGroupedByGender, totalNumberOfStudents)

    // Relation between Race and Math Performance
    val studentsGroupedByRace = studentsPerformanceDF
      .select (studentsPerformanceDF ("math score") >= 50, studentsPerformanceDF ("race/ethnicity"))
      .groupBy ("race/ethnicity", "(math score >= 50)")
      .count ()

    //    studentsGroupedByRace.write.csv ("StudentsGroupedByRace.csv")
    studentsGroupedByRace.show ()
    computeEachGroupPercentage (studentsGroupedByRace, totalNumberOfStudents)

    // Relation between Lunch and Math Performance
    val studentsGroupedByLunch = studentsPerformanceDF
      .select (studentsPerformanceDF ("math score") >= 50, studentsPerformanceDF ("lunch"))
      .groupBy ("lunch", "(math score >= 50)")
      .count ()

    //    studentsGroupedByLunch.write.csv ("StudentsGroupedByLunch.csv")
    studentsGroupedByLunch.show ()
    computeEachGroupPercentage (studentsGroupedByLunch, totalNumberOfStudents)

    // Relation between Parental Education Level and Math Performance
    val studentsGroupedByParentalEducation = studentsPerformanceDF
      .select (studentsPerformanceDF ("parental level of education"), studentsPerformanceDF ("math score") >= 50)
      .groupBy ("parental level of education", "(math score >= 50)")
      .count ()

    //    studentsGroupedByParentalEducation.write.csv ("StudentsGroupedByParentalEducation.csv")
    studentsGroupedByParentalEducation.show ()
    computeEachGroupPercentage (studentsGroupedByParentalEducation, totalNumberOfStudents)


    val studentsGroupedByTestPrep = studentsPerformanceDF
      .select (studentsPerformanceDF ("test preparation course"), studentsPerformanceDF ("math score") >= 50)
      .groupBy ("test preparation course", "(math score >= 50)")
      .count ()

    //    studentsGroupedByTestPrep.write.csv ("StudentsGroupedByTestPrep.csv")
    studentsGroupedByTestPrep.show ()
    computeEachGroupPercentage (studentsGroupedByTestPrep, totalNumberOfStudents)

    // Relation between Having Lunch with Test Preparation and Math Performance
    studentsPerformanceDF
      .select (studentsPerformanceDF ("lunch"), studentsPerformanceDF ("test preparation course"), studentsPerformanceDF ("math score") >= 50)
      .groupBy ("lunch", "test preparation course", "(math score >= 50)")
      .count ()
      .show ()

    // Retrieve top 10 students and last 10 students
    studentsPerformanceDF
      .orderBy ("math score")
      .limit (10)
      .show ()

    studentsPerformanceDF
      .orderBy (desc ("math score"))
      .limit (10)
      .show ()

    // Retrieve average of math scores
    val studentsMarksSum = studentsPerformanceDF
      .agg (sum ("math score"))
      .first ()
      .getLong (0)

    println ("Average of math score between students: " + (studentsMarksSum * 1.0 / totalNumberOfStudents))

    studentsPerformanceDF
      .orderBy (desc ("math score") , desc ("reading score"), desc ("writing score"))
      .limit (1)
      .show ()

    // Query to select only students who got excellent
    // and analyzing their race, and lunch attributes
    val studentsGotExcellentGrouped = studentsPerformanceDF
      .filter (studentsPerformanceDF ("math score") >= 85)
      .select (studentsPerformanceDF ("race/ethnicity"), studentsPerformanceDF ("lunch"))
      .groupBy ("race/ethnicity", "lunch")
      .count ()

    // We need to count the sum of the counts outputted from
    // the previous query dataframe
    val studentsGotExcellentCount = studentsGotExcellentGrouped
      .agg (sum ("count"))
      .first ()
      .getLong (0)

    studentsGotExcellentGrouped.show ()
    computeEachGroupPercentage (studentsGotExcellentGrouped, studentsGotExcellentCount)

    studentsPerformanceDF.createOrReplaceTempView ("students")

    println ("Trying out surrounding columns with backticks")
    spark.sql ("SELECT gender, `parental level of education`, `math score` FROM students WHERE `math score` >= 50 ORDER BY `math score` DESC LIMIT 10").show ()

    val studentsPerformanceDFRenamed = studentsPerformanceDF
      .withColumnRenamed ("parental level of education", "parental_level_of_education")
      .withColumnRenamed ("math score", "math_score")
      .withColumnRenamed ("reading score", "reading_score")
      .withColumnRenamed ("writing score", "writing_score")
    //
    studentsPerformanceDFRenamed.createOrReplaceTempView ("students")

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

    // Top 3 female students from associate's degree
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"associate's degree\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 female students from bachelor's degree
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"bachelor's degree\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 female students from some college
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"some college\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 female students from high school
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"high school\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 female students from some high school
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"some high school\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 female students from master's degree
    spark.sql ("SELECT gender, parental_level_of_education, math_score FROM students WHERE gender=\"female\" AND parental_level_of_education=\"master's degree\" ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 students who scored in math more than in reading and less than in writing
    spark.sql ("SELECT * FROM students WHERE math_score > reading_score AND math_score < writing_score ORDER BY math_score DESC LIMIT 3").show ()

    // Top 3 students in math, reading, and writing
    spark.sql ("SELECT * FROM students ORDER BY math_score DESC, reading_score DESC, writing_score DESC LIMIT 3").show ()

    // Top 3 students in math, reading, and writing
    spark.sql ("SELECT *, (math_score + reading_score + writing_score) as total_score_sum FROM students ORDER BY total_score_sum DESC LIMIT 3").show ()


    // Loading StudentsPerformance.csv with custom schema
    //    val customSchema = new StructType ()
    //      .add ("gender", StringType, true)
    //      .add ("race", StringType, true)
    //      .add ("parental_level_of_education", StringType, true)
    //      .add ("lunch", StringType, true)
    //      .add ("test_preparation_course", StringType, true)
    //      .add ("math_score", IntegerType, true)
    //      .add ("reading_score", IntegerType, true)
    //      .add ("writing_score", IntegerType, true)
    //
    //    val studentsPerformanceDFWithCustomSchema = spark.read
    //      .schema (customSchema)
    //      .option ("header", "true")
    //      .option ("inferSchema", "false")
    //      .option ("nullValue", "null")
    //      .csv ("StudentsPerformance.csv")
    //
    //    studentsPerformanceDFWithCustomSchema.printSchema ()
    //    studentsPerformanceDFWithCustomSchema.show ()

    spark.stop ()
  }
}
