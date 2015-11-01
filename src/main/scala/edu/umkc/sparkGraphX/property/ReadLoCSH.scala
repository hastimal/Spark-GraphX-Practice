package edu.umkc.sparkGraphX.property

/**
 * Created by hastimal on 10/26/2015.
 * http://www.snee.com/bobdc.blog/2015/04/running-spark-graphx-algorithm.html
 */

// readLoCSH.scala: read Library of Congress Subject Headings into
// Spark GraphX graph and apply connectedComponents algorithm to those
// connected by skos:related property.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

object ReadLoCSH {

  val componentLists = HashMap[VertexId, ListBuffer[VertexId]]()
  val prefLabelMap = HashMap[VertexId, String]()

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sc = new SparkContext(new SparkConf().setAppName("ReadLoCSH").setMaster("local[*]").set("spark.executor.memory", "3g"))

    // regex pattern for end of triple
    val tripleEndingPattern = """\s*\.\s*$""".r
    // regex pattern for language tag
    val languageTagPattern = "@[\\w-]+".r

    // Parameters of GraphX Edge are subject, object, and predicate
    // identifiers. RDF traditionally does (s, p, o) order but in GraphX
    // it's (edge start node, edge end node, edge description).

    // Scala beginner hack: I couldn't figure out how to declare an empty
    // array of Edges and then append Edges to it (or how to declare it
    // as a mutable ArrayBuffer, which would have been even better), but I
    // can append to an array started like the following, and will remove
    // the first Edge when creating the RDD.

    var edgeArray = Array(Edge(0L, 0L, "http://dummy/URI"))
    var literalPropsTriplesArray = new Array[(Long, Long, String)](0)
    var vertexArray = new Array[(Long, String)](0)

    // Read the Library of Congress n-triples file
    //val source = Source.fromFile("sampleSubjects.nt","UTF-8")  // shorter for testing
  //  val source = Source.fromFile("PrefLabelAndRelatedMinusBlankNodes.nt", "UTF-8")
    val source = Source.fromFile("src/main/resources/inputRDF/samplecongress.nt", "iso-8859-1")
    val lines = source.getLines.toArray

    // When parsing the data we read, use this map to check whether each
    // URI has come up before.
    val vertexURIMap = new HashMap[String, Long];

    // Parse the data into triples.
    var triple = new Array[String](2)
    var nextVertexNum = 0L
    for (i <- 0 until lines.length) {
      // Space in next line needed for line after that.
      lines(i) = tripleEndingPattern.replaceFirstIn(lines(i), " ")
      triple = lines(i).mkString.split(">\\s+") // split on "> "
      // Variables have the word "triple" in them because "object"
      // by itself is a Scala keyword.
      val tripleSubject = triple(0).substring(1) // substring() call
      val triplePredicate = triple(1).substring(1) // to remove "<"
      if (!(vertexURIMap.contains(tripleSubject))) {
        vertexURIMap(tripleSubject) = nextVertexNum
        nextVertexNum += 1

      }
      if (!(vertexURIMap.contains(triplePredicate))) {
        vertexURIMap(triplePredicate) = nextVertexNum
        nextVertexNum += 1

      }
      val subjectVertexNumber = vertexURIMap(tripleSubject)
      val predicateVertexNumber = vertexURIMap(triplePredicate)

      // If the first character of the third part is a <, it's a URI;
      // otherwise, a literal value. (Needs more code to account for
      // blank nodes.)
      if (triple(2)(0) == '<') {
        val tripleObject = triple(2).substring(1) // Lose that <.
        if (!(vertexURIMap.contains(tripleObject))) {
          vertexURIMap(tripleObject) = nextVertexNum
          nextVertexNum += 1
        }
        val objectVertexNumber = vertexURIMap(tripleObject)
        edgeArray = edgeArray :+
          Edge(subjectVertexNumber, objectVertexNumber, triplePredicate)
      }
      else {
        literalPropsTriplesArray = literalPropsTriplesArray :+
          (subjectVertexNumber, predicateVertexNumber, triple(2))
      }
      //tripleSubject
    }


    // Switch value and key for vertexArray that we'll use to create the
    // GraphX graph.
    for ((k, v) <- vertexURIMap) vertexArray = vertexArray :+(v, k)

    // We'll be looking up a lot of prefLabels, so create a hashmap for them.
    for (i <- 0 until literalPropsTriplesArray.length) {
      if (literalPropsTriplesArray(i)._2 ==
        vertexURIMap("http://www.w3.org/2004/02/skos/core#prefLabel")) {
        // Lose the language tag.
        val prefLabel =
          languageTagPattern.replaceFirstIn(literalPropsTriplesArray(i)._3, "")
        prefLabelMap(literalPropsTriplesArray(i)._1) = prefLabel;
      }
    }

    // Create RDDs and Graph from the parsed data.

    // vertexRDD Long: the GraphX longint identifier. String: the URI.
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)

    // edgeRDD String: the URI of the triple predicate. Trimming off the
    // first Edge in the array because it was only used to initialize it.
    val edgeRDD: RDD[Edge[(String)]] =
      sc.parallelize(edgeArray.slice(1, edgeArray.length))

    // literalPropsTriples Long, Long, and String: the subject and predicate
    // vertex numbers and the the literal value that the predicate is
    // associating with the subject.
    val literalPropsTriplesRDD: RDD[(Long, Long, String)] =
      sc.parallelize(literalPropsTriplesArray)

    val graph: Graph[String, String] = Graph(vertexRDD, edgeRDD)
    graph.triplets.collect().foreach(println)//Added by HM

    // Create a subgraph based on the vertices connected by SKOS "related"
    // property.
    val skosRelatedSubgraph =
      graph.subgraph(t => t.attr ==
        "http://www.w3.org/2004/02/skos/core#related")

    // Find connected components  of skosRelatedSubgraph.
    val ccGraph = skosRelatedSubgraph.connectedComponents()

    // Fill the componentLists hashmap.
    skosRelatedSubgraph.vertices.leftJoin(ccGraph.vertices) {
      case (id, u, comp) => comp.get
    }.foreach { case (id, startingNode) => {
      // Add id to the list of components with a key of comp.get
      if (!(componentLists.contains(startingNode))) {
        componentLists(startingNode) = new ListBuffer[VertexId]
      }
      componentLists(startingNode) += id
    }
    }

    // Output a report on the connected components.
//    println("------  connected components in SKOS \"related\" triples ------\n")
//    for ((component, componentList) <- componentLists) {
//      if (componentList.size > 1) {
//        // don't bother with lists of only 1
//        for (c <- componentList) {
//          println(prefLabelMap(c));
//        }
//        println("--------------------------")
//      }
//    }

   // sc.stop
  }
}