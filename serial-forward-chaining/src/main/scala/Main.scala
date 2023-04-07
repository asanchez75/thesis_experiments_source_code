import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
//import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.commons.text.StringSubstitutor;
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mapAsJavaMap
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrame
import java.util.Calendar


object Main  extends App {

  val now1 = Calendar.getInstance().getTime()
  println(now1)

  //val conf = new SparkConf().setAppName("Ontosides").setMaster("spark://e65af16b882c:7077")
  val conf = new SparkConf().setAppName("Ontosides")
  val spark = SparkSession.builder.config(conf).getOrCreate()

  val construct_template1 =  """${question} ~ -2043943001117296854 ~ ${np} ~ rule1"""
  val construct_query1 =  """
SELECT s_1_2_t0.s AS question, string(count(s_1_2_t0.o)) AS np
FROM rdf_quad as s_1_2_t0
WHERE s_1_2_t0.p = '-2870794494214454068'
GROUP BY s_1_2_t0.s
"""

  val construct_template2=  """${answer} ~ 1654263561836918822 ~ ${nw} ~ rule2"""
  val construct_query2 =  """
SELECT s_1_4_t0.o AS answer, COUNT ( s_1_4_t0.s) AS nw
 FROM rdf_quad AS s_1_4_t0
   INNER JOIN rdf_quad AS s_1_4_t1
   ON (
     s_1_4_t0.s = s_1_4_t1.s)
 WHERE
   s_1_4_t0.p = '7599831820951517075'
   AND
   s_1_4_t1.p = '-2305084656384702443'
 GROUP BY s_1_4_t0.o
"""

  val construct_template3 =  """${answer} ~ -162994529442098395 ~ ${nm} ~ rule3"""
  val construct_query3 =  """
SELECT s_1_12_t2.s AS answer, COUNT (s_1_12_t3.o) AS nm
  FROM rdf_quad AS s_1_12_t2
    INNER JOIN rdf_quad AS s_1_12_t3
    ON (
      s_1_12_t2.o = s_1_12_t3.s)
    INNER JOIN rdf_quad AS s_1_12_t4
    ON (
      s_1_12_t3.o = s_1_12_t4.s)
  WHERE
    s_1_12_t2.p = '-3340849927589060355'
    AND
    s_1_12_t3.p = '-2870794494214454068'
    AND
    s_1_12_t4.p = '8371660464858259940'
    AND
    s_1_12_t4.o = 1
    AND
    not  EXISTS (
       SELECT 1
        FROM rdf_quad AS s_1_10_t0
          INNER JOIN rdf_quad AS s_1_10_t1
          ON (
            s_1_10_t0.s = s_1_10_t1.s)
        WHERE
          s_1_10_t0.p = '7599831820951517075'
          AND
          s_1_10_t1.p = '58543521273107012'
          AND
          s_1_10_t0.o = s_1_12_t2.s
          AND
          s_1_10_t1.o = s_1_12_t4.s
       )
  GROUP BY s_1_12_t2.s
"""

  val construct_template4 =  """${answer} ~ 1929379366875840647 ~ 0 ~ rule4"""
  val construct_query4 =  """
SELECT s_1_10_t2.s AS answer
FROM rdf_quad AS s_1_10_t2
WHERE
  s_1_10_t2.p = '-6333462082218916840'
  AND
  s_1_10_t2.o = '355407165855921479'
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = '1654263561836918822'
        AND
        s_1_4_t0.s = s_1_10_t2.s
     )
  AND
  not EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_8_t1
      WHERE
        s_1_8_t1.p = '-162994529442098395'
        AND
        s_1_8_t1.s = s_1_10_t2.s
     )
"""

  val construct_template5 =  """${answer} ~ 1929379366875840647 ~ ${count} ~ rule5"""
  val construct_query5 =  """
SELECT s_1_4_t0.s AS answer, ( s_1_4_t0.o +  s_1_4_t1.o) AS count
FROM rdf_quad AS s_1_4_t0
  INNER JOIN rdf_quad AS s_1_4_t1
  ON (
    s_1_4_t0.s = s_1_4_t1.s)
WHERE
  s_1_4_t0.p = '1654263561836918822'
  AND
  s_1_4_t1.p = '-162994529442098395'
"""

  val construct_template6 =  """${answer} ~ 1929379366875840647 ~ ${nw} ~ rule6"""
  val construct_query6 =  """
SELECT s_1_6_t1.s AS answer, s_1_6_t1.o AS nw
FROM rdf_quad AS s_1_6_t1
WHERE
  s_1_6_t1.p = '1654263561836918822'
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = '-162994529442098395'
        AND
        s_1_4_t0.s = s_1_6_t1.s

     )
"""

  val construct_template7 =  """${answer} ~ 1929379366875840647 ~ ${nm} ~ rule7"""
  val construct_query7 =  """
SELECT s_1_6_t1.s AS answer,
  s_1_6_t1.o AS nm
FROM rdf_quad AS s_1_6_t1
WHERE
  s_1_6_t1.p = '-162994529442098395'
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_4_t0
      WHERE
        s_1_4_t0.p = '1654263561836918822'
        AND
        s_1_4_t0.s = s_1_6_t1.s

     )

"""

  val construct_template8 =  """${answer} ~ 365996558581043605 ~ 1 ~ rule8"""
  val construct_query8 =  """
SELECT s_1_2_t0.s AS answer
FROM rdf_quad AS s_1_2_t0
WHERE
  s_1_2_t0.p = '1929379366875840647'
  AND
  s_1_2_t0.o = 0
"""

  val construct_template9 =  """${answer} ~ 365996558581043605 ~ 0  ~ rule9 | ${answer} ~ -2140376864770084255 ~ -8897867833227473887 ~ rule9"""
  val construct_query9 =  """
SELECT s_1_6_t0.o AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.s = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t1.o = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = '7599831820951517075'
  AND
  s_1_6_t1.p = '-2305084656384702443'
  AND
  s_1_6_t2.p = '439301585684809937'
  AND
  s_1_6_t2.o = '-939489684313816210'
"""

  val construct_template10 =  """${answer} ~ 365996558581043605 ~ 0  ~ rule10 | ${answer} ~ -2140376864770084255 ~ -8897867833227473887  ~ rule10"""
  val construct_query10 =  """
SELECT s_1_14_t2.s AS answer
FROM rdf_quad AS s_1_14_t2
  INNER JOIN rdf_quad AS s_1_14_t3
  ON (
    s_1_14_t2.o = s_1_14_t3.s)
  INNER JOIN rdf_quad AS s_1_14_t4
  ON (
    s_1_14_t3.o = s_1_14_t4.s)
  INNER JOIN rdf_quad AS s_1_14_t5
  ON (
    s_1_14_t3.o = s_1_14_t5.s
    AND
    s_1_14_t4.s = s_1_14_t5.s)
WHERE
  s_1_14_t2.p = '-3340849927589060355'
  AND
  s_1_14_t3.p = '-2870794494214454068'
  AND
  s_1_14_t4.p = '8371660464858259940'
  AND
  s_1_14_t4.o = '1'
  AND
  s_1_14_t5.p = '439301585684809937'
  AND
  s_1_14_t5.o = '-2112784891000867975'
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_12_t0
        INNER JOIN rdf_quad AS s_1_12_t1
        ON (
          s_1_12_t0.s = s_1_12_t1.s)
      WHERE
        s_1_12_t0.p = '7599831820951517075'
        AND
        s_1_12_t1.p = '58543521273107012'
        AND
        s_1_12_t0.o = s_1_14_t2.s
        AND
        s_1_12_t1.o = s_1_14_t5.s
     )
"""

  val construct_template11 =  """${answer} ~ 365996558581043605 ~ 0 ~ rule11 | ${answer} ~ -2140376864770084255 ~ -8897867833227473887 ~ rule11 """
  val construct_query11 =  """
SELECT  s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.o = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t0.s = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = '-3340849927589060355'
  AND
  s_1_6_t1.p = '-6333462082218916840'
  AND
  s_1_6_t1.o = '-2302962826673002942'
  AND
  s_1_6_t2.p = '1929379366875840647'
  AND
 ( s_1_6_t2.o >  0)
"""

  val construct_template12 =  """${answer} ~ 365996558581043605 ~ 0.5 ~ rule12"""
  val construct_query12 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = '1929379366875840647'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = '-3340849927589060355'
  AND
  s_1_10_t3.p = '-2043943001117296854'
  AND
  s_1_10_t3.o = 5.
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = '-2140376864770084255'
        AND
        s_1_8_t0.o = '-8897867833227473887'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

  val construct_template13 =  """${answer} ~ 365996558581043605 ~ 0.2 ~ rule13"""
  val construct_query13 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = '1929379366875840647'
  AND
  s_1_10_t1.o = 2
  AND
  s_1_10_t2.p = '-3340849927589060355'
  AND
  s_1_10_t3.p = '-2043943001117296854'
  AND
  s_1_10_t3.o = 5
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = '-2140376864770084255'
        AND
        s_1_8_t0.o = '-8897867833227473887'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""


  val construct_template14 =  """${answer} ~ 365996558581043605 ~ 0.425 ~ rule14"""
  val construct_query14 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = '1929379366875840647'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = '-3340849927589060355'
  AND
  s_1_10_t3.p = '-2043943001117296854'
  AND
  s_1_10_t3.o = 4
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = '-2140376864770084255'
        AND
        s_1_8_t0.o = '-8897867833227473887'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )

"""

  val construct_template15 =  """${answer} ~ 365996558581043605 ~ 0.1 ~ rule15"""
  val construct_query15 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = '1929379366875840647'
  AND
  s_1_10_t1.o = 2
  AND
  s_1_10_t2.p = '-3340849927589060355'
  AND
  s_1_10_t3.p = '-2043943001117296854'
  AND
  s_1_10_t3.o = 4
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = '-2140376864770084255'
        AND
        s_1_8_t0.o = '-8897867833227473887'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

  val construct_template16 =  """${answer} ~ 365996558581043605 ~ 0 ~ rule16"""
  val construct_query16 =  """
SELECT s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.o = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t0.s = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = '-3340849927589060355'
  AND
  s_1_6_t1.p = '-2043943001117296854'
  AND
  s_1_6_t2.p = '1929379366875840647'
  AND
 ( s_1_6_t1.o >  3)
  AND
 ( s_1_6_t1.o <  6)
  AND
 ( s_1_6_t2.o >  2)
"""

  val construct_template17 =  """${answer} ~ 365996558581043605 ~ 0.3 ~ rule17"""
  val construct_query17 =  """
SELECT s_1_10_t1.s AS answer
FROM rdf_quad AS s_1_10_t1
  INNER JOIN rdf_quad AS s_1_10_t2
  ON (
    s_1_10_t1.s = s_1_10_t2.s)
  INNER JOIN rdf_quad AS s_1_10_t3
  ON (
    s_1_10_t2.o = s_1_10_t3.s)
WHERE
  s_1_10_t1.p = '1929379366875840647'
  AND
  s_1_10_t1.o = 1
  AND
  s_1_10_t2.p = '-3340849927589060355'
  AND
  s_1_10_t3.p = '-2043943001117296854'
  AND
  s_1_10_t3.o = 3
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t0
      WHERE
        s_1_8_t0.p = '-2140376864770084255'
        AND
        s_1_8_t0.o = '-8897867833227473887'
        AND
        s_1_8_t0.s = s_1_10_t2.s

     )
"""

  val construct_template18 =  """${answer} ~ 365996558581043605 ~ 0 ~ rule18"""
  val construct_query18 =  """
SELECT s_1_6_t0.s AS answer
FROM rdf_quad AS s_1_6_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t0.s = s_1_6_t1.s)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t1.o = s_1_6_t2.s)
WHERE
  s_1_6_t0.p = '1929379366875840647'
  AND
  s_1_6_t1.p = '-3340849927589060355'
  AND
  s_1_6_t2.p = '-2043943001117296854'
  AND
  s_1_6_t2.o = 3.
  AND
 ( s_1_6_t0.o >  1)
"""

  object Reasoner {
    def rule(construct_query:String, construct_template: String, df: DataFrame) : DataFrame = {

      val construct = udf((row: scala.collection.immutable.Map[String, String]) => {
        val templateString: String = construct_template
        val sub: StringSubstitutor = new StringSubstitutor(mapAsJavaMap(row));
        val resolvedString: String = sub.replace(templateString);
        val retrunStr = resolvedString
        retrunStr
      })

      df.createOrReplaceTempView("rdf_quad")

      val r1 = spark.sql(construct_query)

      //r2.printSchema()
      //r1.show(20,false)

      val colnms_n_vals = r1.columns.flatMap { c => Array(lit(c), col(c)) }

      //println(colnms_n_vals)

      val df1 = r1.withColumn("b",  map(colnms_n_vals:_*))
      //df1.show(20,false)

      //df1.printSchema()

      val df2 = df1.withColumn("c", construct(col("b")))
      //df2.printSchema()

      //df2.show(20, false)

      val df3 = df2.withColumn("c", explode(split(col("c"), "\\|"))).select(col("c"))

      //df3.show(20, false)

      val df4 = df3.withColumn("_tmp", split(col("c"), "~")).select(
        col("_tmp").getItem(0).as("s"),
        col("_tmp").getItem(1).as("p"),
        col("_tmp").getItem(2).as("o"),
        col("_tmp").getItem(3).as("g")
      )

      //df4.show(20, false)

      val df5 = df4.withColumn("s",trim(col("s"))).withColumn("p",trim(col("p"))).withColumn("o",trim(col("o"))).withColumn("g",trim(col("g")))

      return df5
    }
  }

  // parquet -> delta
  /////////////////////////////////////////////////////////////////////////////////////
  //val df = spark.read.parquet("/usr/local/src/node1")
  //val db =  df.where("g in ('http://ontosides', 'http://ontosides/missing')")
  //db.write.format("delta").mode("overwrite").partitionBy("g").save("hdfs://localhost:8020/node1")
  /////////////////////////////////////////////////////////////////////////////////////

  val df = spark.read.format("delta").load("hdfs://localhost:8020/node1")
  df.printSchema()
  df.show(10, false)
  df.count()


  ///////////////////////////////////////////////////////////////////////////////
  //import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
  //import org.apache.spark.sql.Row
  //val schema = StructType( StructField("s", StringType, -8897867833227473887) :: StructField("p", StringType, -8897867833227473887) :: StructField("o", StringType, -8897867833227473887) :: StructField("g", StringType, -8897867833227473887) :: Nil)
  //val rules = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  //rules.write.format("delta").mode("append").partitionBy("g").save("hdfs://localhost:8020/rules")

  ///////////////////////////////////////////////////////////////////////////////
  println("~~~rule 1~~~")
  val df1 = Reasoner.rule(construct_query1, construct_template1, df)
  //spark.time(df1.write.format("delta").mode("append").save("hdfs://localhost:8020/rules/g=rule1"))
  spark.time(df1.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 2~~~")
  val df2 = Reasoner.rule(construct_query2, construct_template2, df)
  spark.time(df2.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 3~~~")
  val df3 = Reasoner.rule(construct_query3, construct_template3, df)
  spark.time(df3.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 4~~~")
  val df4 = Reasoner.rule(construct_query4, construct_template4, df)
  spark.time(df4.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 5~~~")
  val df5 = Reasoner.rule(construct_query5, construct_template5, df)
  spark.time(df5.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 6~~~")
  val df6 = Reasoner.rule(construct_query6, construct_template6, df)
  spark.time(df6.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 7~~~")
  val df7 = Reasoner.rule(construct_query7, construct_template7, df)
  spark.time(df7.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 8~~~")
  val df8 = Reasoner.rule(construct_query8, construct_template8, df)
  spark.time(df8.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 9~~~")
  val df9 = Reasoner.rule(construct_query9, construct_template9, df)
  val df91 = df9.dropDuplicates()
  spark.time(df91.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 10~~~")
  val df10 = Reasoner.rule(construct_query10, construct_template10, df)
  val df101 = df10.dropDuplicates()
  spark.time(df101.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 11~~~")
  val df11 = Reasoner.rule(construct_query11, construct_template11, df)
  val df111 = df11.dropDuplicates()
  spark.time(df111.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 12~~~")
  val df12 = Reasoner.rule(construct_query12, construct_template12, df)
  spark.time(df12.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 13~~~")
  val df13 = Reasoner.rule(construct_query13, construct_template13, df)
  spark.time(df13.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 14~~~")
  val df14 = Reasoner.rule(construct_query14, construct_template14, df)
  spark.time(df14.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 15~~~")
  val df15 = Reasoner.rule(construct_query15, construct_template15, df)
  spark.time(df15.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 16~~~")
  val df16 = Reasoner.rule(construct_query16, construct_template16, df)
  spark.time(df16.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 17~~~")
  val df17 = Reasoner.rule(construct_query17, construct_template17, df)
  spark.time(df17.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  println("~~~rule 18~~~")
  val df18 = Reasoner.rule(construct_query18, construct_template18, df)
  spark.time(df18.write.format("delta").mode("append").save("hdfs://localhost:8020/node1"))

  val now2 = Calendar.getInstance().getTime()
  println(now2)


}


