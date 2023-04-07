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

//var now1 = Calendar.getInstance().getTime()
//println("start: " + now1)

//val conf = new SparkConf().setAppName("Ontosides").setMaster("spark://e65af16b882c:7077")
val conf = new SparkConf().setAppName("Ontosides")
val spark = SparkSession.builder.config(conf).getOrCreate()


  val construct_template1 = """${question} ~ -2043943001117296854 ~ ${np} ~ http://delta"""
  val construct_query1 =
    """
SELECT s_1_1_t0.s AS question, COUNT ( s_1_1_t0.o) AS np
  FROM rdf_quad AS s_1_1_t0
  WHERE
    s_1_1_t0.g =  'http://delta' 
    AND 
    s_1_1_t0.p =  '-2870794494214454068' 
    AND 
    not EXISTS (
       SELECT 1 
        FROM rdf_quad AS s_1_4_t1
        WHERE
          s_1_4_t1.g =  'http://ontosides' 
          AND 
          s_1_4_t1.p =  '-2043943001117296854' 
          AND 
          s_1_4_t1.s = s_1_1_t0.s

       )
  GROUP BY s_1_1_t0.s
"""


  val construct_template2 = """${answer} ~ 1654263561836918822 ~ ${nw} ~ http://delta"""
  val construct_query2 =
    """
 SELECT  s_1_2_t0.o AS answer, COUNT ( s_1_2_t1.s) AS nw FROM rdf_quad AS s_1_2_t0
    INNER JOIN rdf_quad AS s_1_2_t1
    ON (
      s_1_2_t1.s = s_1_2_t0.s)
  WHERE
    s_1_2_t0.g =  'http://delta'
    AND
    s_1_2_t0.p =  '7599831820951517075'
    AND
    s_1_2_t1.g =  'http://delta'
    AND
    s_1_2_t1.p =  '-2305084656384702443'
  GROUP BY s_1_2_t0.o
"""

  val construct_template3 = """${answer} ~ -162994529442098395 ~ ${nm} ~ http://delta"""
  val construct_query3 =
    """
SELECT  s_1_1_t0.s AS answer, COUNT ( s_1_5_t2.s) AS nm
  FROM rdf_quad AS s_1_1_t0
    INNER JOIN rdf_quad AS s_1_5_t1
    ON (
      s_1_5_t1.s = s_1_1_t0.o)
    INNER JOIN rdf_quad AS s_1_5_t2
    ON (
      s_1_5_t2.s = s_1_5_t1.o)
  WHERE
    s_1_1_t0.g =  'http://delta'
    AND
    s_1_1_t0.p =  '-3340849927589060355'
    AND
    s_1_5_t1.g in ('http://ontosides','http://delta')
    AND
    s_1_5_t1.p =  '-2870794494214454068'
    AND
    s_1_5_t2.g in ('http://ontosides','http://delta')
    AND
    s_1_5_t2.p =  '8371660464858259940'
    AND
    s_1_5_t2.o =  1
    AND
    not EXISTS (
       SELECT 1
        FROM rdf_quad AS s_1_9_t3
          INNER JOIN rdf_quad AS s_1_9_t4
          ON (
            s_1_9_t4.s = s_1_9_t3.s)
        WHERE
          s_1_9_t3.g =  'http://delta'
          AND
          s_1_9_t3.p =  '7599831820951517075'
          AND
          s_1_9_t4.g =  'http://delta'
          AND
          s_1_9_t4.p =  '58543521273107012'
          AND
          s_1_9_t4.o = s_1_5_t1.o
          AND
          s_1_9_t3.o = s_1_1_t0.s

       )
  GROUP BY s_1_1_t0.s
"""

  val construct_template4 = """${answer} ~ 1929379366875840647 ~ 0 ~ http://delta"""
  val construct_query4 =
    """
SELECT  s_1_7_t2.s AS answer FROM rdf_quad AS s_1_7_t2
WHERE
  s_1_7_t2.g =  'http://delta'
  AND
  s_1_7_t2.p =  '-6333462082218916840'
  AND
  s_1_7_t2.o =  '355407165855921479'
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_2_t0
      WHERE
        s_1_2_t0.g =  'http://delta'
        AND
        s_1_2_t0.p =  '1654263561836918822'
        AND
        s_1_2_t0.s = s_1_7_t2.s

     )
  AND
  not  EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_5_t1
      WHERE
        s_1_5_t1.g =  'http://delta'
        AND
        s_1_5_t1.p =  '-162994529442098395'
        AND
        s_1_5_t1.s = s_1_7_t2.s

     )
"""

  val construct_template5 = """${answer} ~ 1929379366875840647 ~ ${count} ~ http://delta"""
  val construct_query5 =
    """
SELECT  s_1_2_t1.s AS answer, ( s_1_2_t0.o +  s_1_2_t1.o) AS count FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1654263561836918822'
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-162994529442098395'
"""

  val construct_template6 = """${answer} ~ 1929379366875840647 ~ ${nw} ~ http://delta"""
  val construct_query6 =
    """
SELECT  s_1_4_t1.s AS answer, s_1_4_t1.o AS nw FROM rdf_quad AS s_1_4_t1
WHERE
  s_1_4_t1.g =  'http://delta'
  AND
  s_1_4_t1.p =  '1654263561836918822'
  AND
  not EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_2_t0
      WHERE
        s_1_2_t0.g =  'http://delta'
        AND
        s_1_2_t0.p =  '-162994529442098395'
        AND
        s_1_2_t0.s = s_1_4_t1.s

     )
"""

  val construct_template7 = """${answer} ~ 1929379366875840647 ~ ${nm} ~ http://delta"""
  val construct_query7 =
    """
SELECT  s_1_4_t1.s AS answer, s_1_4_t1.o AS nm FROM rdf_quad AS s_1_4_t1
WHERE
  s_1_4_t1.g =  'http://delta'
  AND
  s_1_4_t1.p =  '-162994529442098395'
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_2_t0
      WHERE
        s_1_2_t0.g =  'http://delta'
        AND
        s_1_2_t0.p =  '1654263561836918822'
        AND
        s_1_2_t0.s = s_1_4_t1.s

     )
"""

  val construct_template8 = """${answer} ~ 365996558581043605 ~ 1 ~ http://delta"""
  val construct_query8 =
    """
SELECT  s_1_1_t0.s AS answer
FROM rdf_quad AS s_1_1_t0
WHERE
  s_1_1_t0.g =  'http://delta'
  AND
  s_1_1_t0.p =  '1929379366875840647'
  AND
  s_1_1_t0.o = 0
"""

  val construct_template9 = """${answer} ~ 365996558581043605 ~ 0  ~ http://delta | ${answer} ~ -2140376864770084255 ~ 6088520763082456498 ~ http://delta"""
  val construct_query9 =
    """
SELECT  s_1_2_t0.o AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '7599831820951517075'
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-2305084656384702443'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '439301585684809937'
  AND
  s_1_5_t2.o = '-939489684313816210'
"""

  val construct_template10 = """${answer} ~ 365996558581043605 ~ 0  ~ http://delta | ${answer} ~ -2140376864770084255 ~ 6088520763082456498  ~ http://delta"""
  val construct_query10 =
    """
SELECT  s_1_1_t0.s AS answer
FROM rdf_quad AS s_1_1_t0
  INNER JOIN rdf_quad AS s_1_6_t1
  ON (
    s_1_6_t1.s = s_1_1_t0.o)
  INNER JOIN rdf_quad AS s_1_6_t2
  ON (
    s_1_6_t2.s = s_1_6_t1.o)
  INNER JOIN rdf_quad AS s_1_6_t3
  ON (
    s_1_6_t3.s = s_1_6_t2.s
    AND
    s_1_6_t3.s = s_1_6_t1.o)
WHERE
  s_1_1_t0.g =  'http://delta'
  AND
  s_1_1_t0.p =  '-3340849927589060355'
  AND
  s_1_6_t1.g in ('http://ontosides','http://delta')
  AND
  s_1_6_t1.p =  '-2870794494214454068'
  AND
  s_1_6_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_6_t2.p =  '8371660464858259940'
  AND
  s_1_6_t2.o =  1
  AND
  s_1_6_t3.g in ('http://ontosides','http://delta')
  AND
  s_1_6_t3.p =  '439301585684809937'
  AND
  s_1_6_t3.o = '-2112784891000867975'
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_10_t4
        INNER JOIN rdf_quad AS s_1_10_t5
        ON (
          s_1_10_t5.s = s_1_10_t4.s)
      WHERE
        s_1_10_t4.g =  'http://delta'
        AND
        s_1_10_t4.p =  '7599831820951517075'
        AND
        s_1_10_t5.g =  'http://delta'
        AND
        s_1_10_t5.p =  '58543521273107012'
        AND
        s_1_10_t5.o = s_1_6_t1.o
        AND
        s_1_10_t4.o = s_1_1_t0.s

     )
"""

  val construct_template11 = """${answer} ~ 365996558581043605 ~ 0 ~ http://delta | ${answer} ~ -2140376864770084255 ~ 6088520763082456498 ~ http://delta """
  val construct_query11 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t0.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '-3340849927589060355'
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '1929379366875840647'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-6333462082218916840'
  AND
  s_1_5_t2.o =  '-2302962826673002942'
  AND
 ( s_1_2_t1.o >  0)
"""

  val construct_template12 = """${answer} ~ 365996558581043605 ~ 0.5 ~ http://delta"""
  val construct_query12 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t0.o = 1
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 5.
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_8_t3
      WHERE
        s_1_8_t3.g =  'http://delta'
        AND
        s_1_8_t3.p =  '-2140376864770084255'
        AND
        s_1_8_t3.o =  '6088520763082456498'
        AND
        s_1_8_t3.s = s_1_2_t0.s

     )
"""

  val construct_template13 = """${answer} ~ 365996558581043605 ~ 0.2 ~ http://delta"""
  val construct_query13 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t0.o = 2
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 5
  AND
  not EXISTS (
     SELECT 1
      FROM rdf_quad AS s_1_8_t3
      WHERE
        s_1_8_t3.g =  'http://delta'
        AND
        s_1_8_t3.p =  '-2140376864770084255'
        AND
        s_1_8_t3.o =  '6088520763082456498'
        AND
        s_1_8_t3.s = s_1_2_t0.s

     )
"""


  val construct_template14 = """${answer} ~ 365996558581043605 ~ 0.425 ~ http://delta"""
  val construct_query14 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t0.o = 1
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 4
  AND
  not ( EXISTS ( (
     SELECT 1
      FROM rdf_quad AS s_1_8_t3
      WHERE
        s_1_8_t3.g =  'http://delta'
        AND
        s_1_8_t3.p =  '-2140376864770084255'
        AND
        s_1_8_t3.o = '6088520763082456498'
        AND
        s_1_8_t3.s = s_1_2_t0.s

     )))
"""

  val construct_template15 = """${answer} ~ 365996558581043605 ~ 0.1 ~ http://delta"""
  val construct_query15 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t0.o = 2
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 4
  AND
  not  EXISTS  (
     SELECT 1
      FROM rdf_quad AS s_1_8_t3
      WHERE
        s_1_8_t3.g =  'http://delta'
        AND
        s_1_8_t3.p =  '-2140376864770084255'
        AND
        s_1_8_t3.o =  '6088520763082456498'
        AND
        s_1_8_t3.s = s_1_2_t0.s

     )
"""

  val construct_template16 = """${answer} ~ 365996558581043605 ~ 0 ~ http://delta"""
  val construct_query16 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t0.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '-3340849927589060355'
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '1929379366875840647'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
 ( s_1_5_t2.o >  3)
  AND
 ( s_1_5_t2.o <  6)
  AND
 ( s_1_2_t1.o >  2)
"""

  val construct_template17 = """${answer} ~ 365996558581043605 ~ 0.3 ~ http://delta"""
  val construct_query17 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t0.o = 1
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 3
  AND
  not ( EXISTS ( (
     SELECT 1
      FROM rdf_quad AS s_1_8_t3
      WHERE
        s_1_8_t3.g =  'http://delta'
        AND
        s_1_8_t3.p =  '-2140376864770084255'
        AND
        s_1_8_t3.o =  '6088520763082456498'
        AND
        s_1_8_t3.s = s_1_2_t0.s

     )))
"""

  val construct_template18 = """${answer} ~ 365996558581043605 ~ 0 ~ http://delta"""
  val construct_query18 =
    """
SELECT  s_1_2_t1.s AS answer
FROM rdf_quad AS s_1_2_t0
  INNER JOIN rdf_quad AS s_1_2_t1
  ON (
    s_1_2_t1.s = s_1_2_t0.s)
  INNER JOIN rdf_quad AS s_1_5_t2
  ON (
    s_1_5_t2.s = s_1_2_t1.o)
WHERE
  s_1_2_t0.g =  'http://delta'
  AND
  s_1_2_t0.p =  '1929379366875840647'
  AND
  s_1_2_t1.g =  'http://delta'
  AND
  s_1_2_t1.p =  '-3340849927589060355'
  AND
  s_1_5_t2.g in ('http://ontosides','http://delta')
  AND
  s_1_5_t2.p =  '-2043943001117296854'
  AND
  s_1_5_t2.o = 3
  AND
 ( s_1_2_t0.o >  1)
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
//   val is_digit = udf((r:String) => {if(r.forall(_.isDigit)) {true} else {false} })
//   val df1 = spark.read.parquet("/mnt/data/usr/local/src/node1/")
//   //val df2 =  df1.where("g in ('http://ontosides', 'http://ontosides/missing')")
//   val df2 =  df1.where("g like 'http://ontosides%'")
//   //df2.groupBy("g").agg(count("g")).show(20, false)
//   val df3 = (df2.
//     withColumn("s", when(is_digit($"s") === true, $"s").otherwise(xxhash64($"s"))).
//     withColumn("p", when(is_digit($"p") === true, $"p").otherwise(xxhash64($"p"))).
//     withColumn("o", when(is_digit($"o") === true, $"o").otherwise(xxhash64($"o")))
//     )
//   val df4 = df3.withColumn("g", lit("http://ontosides"))
//   //df4.groupBy("g").agg(count("g")).show(20, false)
//   //df4.show(20,false)
//   //df4.count
//   
//   
//   val diff = spark.read.parquet("/mnt/data/usr/local/src/diff/diff1-hashed.parquet/")
//   //diff.show(20,false)
//   val dfsat = df4.union(diff) 
//   dfsat.write.format("delta").mode("overwrite").partitionBy("g").save("hdfs://hadoop-namenode/incremental/node1saturated")
/////////////////////////////////////////////////////////////////////////////////////


//var df = spark.read.format("delta").load("hdfs://hadoop-namenode/incremental/node1saturated/")
////df.groupBy("g").agg(count("g")).show(20, false)
//df.printSchema()
//df.show(10, false)
////df.groupBy("g").agg(count("g")).show(20, false)
//df.count()


///////////////////////////////////////////////////////////////////////////////
//import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
//import org.apache.spark.sql.Row
//val schema = StructType( StructField("s", StringType, -8897867833227473887) :: StructField("p", StringType, -8897867833227473887) :: StructField("o", StringType, -8897867833227473887) :: StructField("g", StringType, -8897867833227473887) :: Nil)
//val rules = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
//rules.write.format("delta").mode("append").partitionBy("g").save("hdfs://hadoop-namenode/rules")

///////////////////////////////////////////////////////////////////////////////

object Execution {
def layers_execution(df : DataFrame, SaturationNumber : String) {
//
//Allowing a maximum of 5 threads to run
val executorService = Executors.newFixedThreadPool(6)
implicit val executionContext = ExecutionContext.fromExecutorService(executorService)

//
// layer 1

val fq10 = Future {
    println("~~~rule 10~~~")
    val df10 = Reasoner.rule(construct_query10, construct_template10, df)
    val df101 = df10.dropDuplicates()
   // df101.show(20,false)
   // df101.count
    spark.time(df101.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/rule10"))
}

val fq1 = Future {
    println("~~~rule 1~~~")
    val df1 = Reasoner.rule(construct_query1, construct_template1, df)
   // df1.show(20,false)
   // df1.count
    spark.time(df1.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/rule1"))
}

val fq2 = Future {
    println("~~~rule 2~~~")
    val df2 = Reasoner.rule(construct_query2, construct_template2, df)
   // df2.show(20,false)
   // df2.count
    spark.time(df2.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/rule2"))
}

val fq3 = Future {
    println("~~~rule 3~~~")
    val df3 = Reasoner.rule(construct_query3, construct_template3, df)
   // df3.show(20,false)
   // df3.count
    spark.time(df3.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/rule3"))
}

val fq9 = Future {
    println("~~~rule 9~~~")
    val df9 = Reasoner.rule(construct_query9, construct_template9, df)
    val df91 = df9.dropDuplicates()
   // df91.show(20,false)
   // df91.count
    spark.time(df91.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/rule9"))
}

  Await.result(fq10, Duration.Inf)
  Await.result(fq1, Duration.Inf)
  Await.result(fq2, Duration.Inf)
  Await.result(fq3, Duration.Inf)
  Await.result(fq9, Duration.Inf)

// data from layer1
var dfs = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer1/*/*.parquet")
   // dfs.show(20,false)
// append data from layer1 to node1saturated
dfs.write.format("delta").mode("append").partitionBy("g").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber +  "saturated")


// layer 2


val fq4 = Future {
    println("~~~rule 4~~~")
    val df4 = Reasoner.rule(construct_query4, construct_template4, df)
    spark.time(df4.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer2/rule4"))
}

val fq5 = Future {
    println("~~~rule 5~~~")
    val df5 = Reasoner.rule(construct_query5, construct_template5, df)
    spark.time(df5.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer2/rule5"))
}

val fq6 = Future {
    println("~~~rule 6~~~")
    val df6 = Reasoner.rule(construct_query6, construct_template6, df)
    spark.time(df6.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer2/rule6"))
}

val fq7 = Future {
    println("~~~rule 7~~~")
    val df7 = Reasoner.rule(construct_query7, construct_template7, df)
    spark.time(df7.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer2/rule7"))
}


  Await.result(fq4, Duration.Inf)
  Await.result(fq5, Duration.Inf)
  Await.result(fq6, Duration.Inf)
  Await.result(fq7, Duration.Inf)


// data from layer2
dfs = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer2/*/*.parquet")
   // dfs.show(20,false)
// append data from layer2 to node1saturated
dfs.write.format("delta").mode("append").partitionBy("g").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber +  "saturated")
//
// layer 3


val fq8 = Future {
    println("~~~rule 8~~~")
    val df8 = Reasoner.rule(construct_query8, construct_template8, df)
    spark.time(df8.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer3/rule8"))
}

val fq12 = Future {
    println("~~~rule 12~~~")
    val df12 = Reasoner.rule(construct_query12, construct_template12, df)
    spark.time(df12.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer3/rule12"))
}

val fq11 = Future {
    println("~~~rule 11~~~")
    val df11 = Reasoner.rule(construct_query11, construct_template11, df)
    val df111 = df11.dropDuplicates()
    spark.time(df111.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer3/rule11"))
}


  Await.result(fq8, Duration.Inf)
  Await.result(fq12, Duration.Inf)
  Await.result(fq11, Duration.Inf)

// data from layer3
dfs = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer3/*/*.parquet")
   // dfs.show(20,false)
// append data from layer3 to node1saturated
dfs.write.format("delta").mode("append").partitionBy("g").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber +  "saturated")

// layer 4

val fq13 = Future {
    println("~~~rule 13~~~")
    val df13 = Reasoner.rule(construct_query13, construct_template13, df)
    spark.time(df13.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule13"))
}

val fq14 = Future {
    println("~~~rule 14~~~")
    val df14 = Reasoner.rule(construct_query14, construct_template14, df)
    spark.time(df14.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule14"))
}

val fq15 = Future {
    println("~~~rule 15~~~")
    val df15 = Reasoner.rule(construct_query15, construct_template15, df)
    spark.time(df15.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule15"))
}

val fq16 = Future {
    println("~~~rule 16~~~")
    val df16 = Reasoner.rule(construct_query16, construct_template16, df)
    spark.time(df16.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule16"))
}

val fq17 = Future {
    println("~~~rule 17~~~")
    val df17 = Reasoner.rule(construct_query17, construct_template17, df)
    spark.time(df17.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule17"))
}

val fq18 = Future {
    println("~~~rule 18~~~")
    val df18 = Reasoner.rule(construct_query18, construct_template18, df)
    spark.time(df18.write.format("parquet").mode("overwrite").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/rule18"))
}

  Await.result(fq13, Duration.Inf)
  Await.result(fq14, Duration.Inf)
  Await.result(fq15, Duration.Inf)
  Await.result(fq16, Duration.Inf)
  Await.result(fq17, Duration.Inf)
  Await.result(fq18, Duration.Inf)


// data from layer4
dfs = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node" + SaturationNumber  +  "/layer4/*/*.parquet")
   // dfs.show(20,false)
// append data from layer4 to node1saturated
dfs.write.format("delta").mode("append").partitionBy("g").save("hdfs://hadoop-namenode/incremental/node" + SaturationNumber +  "saturated")

}
}


//spark.time(Execution.layers_execution(df, "1"))
//
//var now2 = Calendar.getInstance().getTime()
//println("end: " + now2)

var df1 : DataFrame = null
var diff : DataFrame = null
var dfsat : DataFrame = null

/********************************************************************************/

println("start diff 1")

var now1 = Calendar.getInstance().getTime()
println("start: " + now1)

var df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node1/")
spark.time(Execution.layers_execution(df, "1"))

var now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/

println("start diff 2")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node2/")
spark.time(Execution.layers_execution(df, "2"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)

/********************************************************************************/
println("start diff 3")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node3/")
spark.time(Execution.layers_execution(df, "3"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/

println("start diff 4")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node4/")
spark.time(Execution.layers_execution(df, "4"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)

/********************************************************************************/
println("start diff 5")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node5/")
spark.time(Execution.layers_execution(df, "5"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/
println("start diff 6")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node6/")
spark.time(Execution.layers_execution(df, "6"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/
println("start diff 7")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node7/")
spark.time(Execution.layers_execution(df, "7"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/
println("start diff 8")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node8/")
spark.time(Execution.layers_execution(df, "8"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)
/********************************************************************************/

println("start diff 9")

now1 = Calendar.getInstance().getTime()
println("start: " + now1)

df = spark.read.format("parquet").load("hdfs://hadoop-namenode/incremental/node9/")
spark.time(Execution.layers_execution(df, "9"))

now2 = Calendar.getInstance().getTime()
println("end: " + now2)


}



