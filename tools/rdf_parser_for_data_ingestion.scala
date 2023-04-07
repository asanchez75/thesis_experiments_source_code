import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import java.io.{File, FileOutputStream, FileInputStream, InputStream, IOException}
import scala.collection.JavaConverters._
import java.io.ByteArrayInputStream
import org.apache.spark.sql.functions.udf
import spark.sqlContext.implicits._
import scala.util.{Try,Success,Failure}

def parser = udf((triple: String) => {
  def parse(triple: String): Try[String] = Try {
    val is : InputStream = new ByteArrayInputStream(triple.getBytes())
    val baseURI : String  = "";
    val format : RDFFormat = RDFFormat.NQUADS;
    val res : GraphQueryResult = QueryResults.parseGraphBackground(is, baseURI, format)
    val st : Statement  = res.next()
    val nquad_string : String = st.getSubject().toString() + "," + st.getPredicate().toString() + ","  + st.getObject().toString() + "," + st.getContext().toString()
    nquad_string
  }

  parse(triple) match { 
    case Success(line) => line.toString
    case _ => "malformed,malformed,malformed,malformed"
  }

})

val df = spark.read.format("csv").load("/mnt/data/home/docker/docker-virtuoso-ontosides111/data/dumps/*.nq.gz")
df.show(10, false)
val df1 = df.withColumn("_c1", parser($"_c0")).drop($"_c0")
val df2 = df1.select(split($"_c1", ",").getItem(0).as("s"), split($"_c1", ",").getItem(1).as("p"), split($"_c1", ",").getItem(2).as("o"),  split($"_c1", ",").getItem(3).as("g") )
df2.write.format("delta").mode("append").save("hdfs://localhost:8020/ontosides11")
df2.show(10, false)

