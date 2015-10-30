package edu.umkc.sparkGraphX.property

/**
 * Created by hastimal on 10/26/2015.
 * http://www.snee.com/bobdc.blog/2015/03/spark-and-sparql-rdf-graphs-an.html
 *
 * Scala program that output some GraphX data as RDF and then showed some SPARQL queries to run on that RDF.
 */
/** Example Property Graph mentioned above, creates an RDD called users of nodes about people at a
  * university and an RDD called relationships that stores information about edges that connect the
  * nodes. RDDs use long integers such as the 3L and 7L values shown below as identifiers for the nodes,
  * and you'll see that it can store additional information about nodes—for example, that node 3L is named
  * "rxin" and has the title "student"—as well as additional information about edges—for example, that the
  * user represented by 5L has an "advisor" relationship to user 3L. I added a few extra nodes and edges to
  * give the eventual SPARQL queries a little more to work with.
//Once the node and edge RDDs are defined, the program creates a graph from them. After that, I added code to
//output RDF triples about node relationships to other nodes (or, in RDF parlance, object property triples)
//using a base URI that I defined at the top of the program to convert identifiers to URIs when necessary.
//This produced triples such as
//<http://snee.com/xpropgraph#istoica> <http://snee.com/xpropgraph#colleague> <http://snee.com/xpropgraph#franklin>
//in the output. Finally, the program outputs non-relationship values (literal properties), producing triples such as
//<http://snee.com/xpropgraph#rxin> <http://snee.com/xpropgraph#role> "student".*/

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.io.Path

object ExamplePropertyGraph {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sc = new SparkContext(new SparkConf().setAppName("ExamplePropertyGraph").setMaster("local"))
    val baseURI = "http://snee.com/xpropgraph#"


    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rakesh", "student")),
        (7L, ("john", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("ishwar", "prof")),
        // Following lines are new data
        (8L, ("biswash", "student")),
        (9L, ("nitin", "student")),
        (10L, ("amit", "student")),
        (11L, ("roshan", "student")),
        (12L, ("nilesh", "student"))
      ))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        // Following lines are new data
        Edge(5L, 8L, "advisor"),
        Edge(2L, 9L, "advisor"),
        Edge(5L, 10L, "advisor"),
        Edge(2L, 11L, "advisor")
      ))
    // Build the initial Graph
    val graph = Graph(users, relationships)

    // Output object property triples
    graph.triplets.foreach( t => println(
      s"<$baseURI${t.srcAttr._1}> <$baseURI${t.attr}> <$baseURI${t.dstAttr._1}> ."
    )
    )
    //Deleting output files recursively if exists
    val dir1 = Path("src/main/resources/outputData/rdf1.txt")
    if (dir1.exists) {
      dir1.deleteRecursively()
      println("Successfully existing output rdf1.tx deleted!!")
    }
    println("Writing in new files as output.......")
    graph.triplets.map(t=>s"<$baseURI${t.srcAttr._1}> <$baseURI${t.attr}> <$baseURI${t.dstAttr._1}>.").saveAsTextFile("src/main/resources/outputData/rdf1.txt")
   // saveAsTextFile("src/main/resources/outputData/rdf.txt")

    // Output literal property triples
    users.foreach(t => println(
      s"""<$baseURI${t._2._1}> <${baseURI}role> \"${t._2._2}\" ."""
    ))
    //Deleting output files recursively if exists
    val dir2 = Path("src/main/resources/outputData/rdf2.txt")
    if (dir2.exists) {
      dir2.deleteRecursively()
      println("Successfully existing output rdf2.tx deleted!!")
    }
    println("Writing in new files as output.......")
    users.map(t=>s"""<$baseURI${t._2._1}> <${baseURI}role> \"${t._2._2}\" .""").saveAsTextFile("src/main/resources/outputData/rdf2.txt")
    sc.stop

  }
}