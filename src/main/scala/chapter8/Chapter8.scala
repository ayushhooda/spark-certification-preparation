package chapter8

import org.apache.spark.sql.SparkSession

object Chapter8 extends App {

  val spark = SparkSession.builder().appName("Chapter-8").master("local[*]").getOrCreate()

  import spark.implicits._

  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 3, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")

  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")

  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
    .toDF("id", "status")

  // Let's register them as tables
  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

  // 1. Inner Joins
  /**
    * Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together) only
    * the rows that evaluate to true.
    */
  person.join(graduateProgram, joinExpression, "inner").show()

  // 2. Outer Joins or Full Outer Joins
  /**
    * Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the
    * rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame,
    * Spark will insert null
    */
  person.join(graduateProgram, joinExpression, "outer").show()

  // 3. Left outer Joins
  /**
    * Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    * left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If
    * there is no equivalent row in the right DataFrame, Spark will insert null
    */
  person.join(graduateProgram, joinExpression, joinType = "left_outer").show

  // 4. Right outer Joins
  /**
    * Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the
    * right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame.
    * If there is no equivalent row in the left DataFrame, Spark will insert null
    */
  person.join(graduateProgram, joinExpression, joinType = "right_outer").show

  // 5. Left semi Joins
  /**
    * Semi joins are a bit of a departure from the other joins. They do not actually include any values from
    * the right DataFrame. They only compare values to see if the value exists in the second DataFrame. If
    * the value does exist, those rows will be kept in the result, even if there are duplicate keys in the left
    * DataFrame. Think of left semi joins as filters on a DataFrame, as opposed to the function of a
    * conventional join
    */
  graduateProgram.join(person, joinExpression, joinType = "left_semi").show

  // 6. Left anti Joins
  /**
    * Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually include anyvalues from the right DataFrame. They only compare values to see if the value exists in the second
    * DataFrame. However, rather than keeping the values that exist in the second DataFrame, they keep
    * only the values that do not have a corresponding key in the second DataFrame. Think of anti joins as a
    * NOT IN SQL-style filter
    */
  graduateProgram.join(person, joinExpression, joinType = "left_anti").show

  // 7. Natural Joins
  /**
    * Natural joins make implicit guesses at the columns on which you would like to join. It finds matching
    * columns and returns the results. Left, right, and outer natural joins are all supported.
    */

  // 8. Cross Joins
  /**
    * Cross-joins in simplest terms are inner
    * joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame to
    * ever single row in the right DataFrame.
    */
  graduateProgram.join(person, joinExpression, joinType = "cross").show
  val x = person.crossJoin(graduateProgram)
  print(x("school")(0))

  // Joins on Complex Types


}
