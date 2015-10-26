package edu.umkc.sparkGraphX.hastimal

import org.apache.spark._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD


// To make some of the examples work we will also need RDD
/**
 * Created by hastimal on 10/10/2015.
 */
object SparkGraphXGeneral {
  def main(args: Array[String]) {
    //https://spark.apache.org/docs/0.9.0/graphx-programming-guide.html
    System.setProperty("hadoop.home.dir","F:\\winutils")
    //St config for Spark
    val conf = new SparkConf().setAppName("SparkGraphxMain").setMaster("local[*]").set("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array(
        (3L, ("rxin", "student")),
        (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")),
        (2L, ("istoica", "prof")))).cache()
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"))).cache()
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    //The following failed I had to change the config on the following
    //worker_max_heapsize to 256MB
    //executor_total_max_heapsize to 3GB

    //This will count all the vertices
    graph.numVertices

    //This will count all the edges
    graph.numEdges

    //This will count the out going edges from each node
    val degrees = graph.outDegrees
    degrees.take(10).foreach(println(_))

    //This will count the in going edges from each node
    val degrees2 = graph.inDegrees
    degrees2.take(10).foreach(println(_))

    //This will giv eyou the node ID with the number of triangles
    //next to that triangle
    val tr = graph.triangleCount()
    tr.vertices.take(10).foreach(println(_))

    //printing stuff
    println("Number of verices: "+graph.numVertices+"\nnumber of edges: "+graph.numEdges+"\ntotal outgoing edges from each node: "+  degrees.collect()+ "and in going edges from each node: "+ degrees2.take(10).toString)
    val countEdges = graph.edges.filter(e => e.srcId < e.dstId).count()
    println(countEdges)

    val indegree = graph.inDegrees
    indegree.foreach(println)

    val degree = graph.degrees
    degree.foreach(println)

    graph.vertices.collect.foreach(println(_))

    //    graph.triplets.map(
    //      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    //    ).collect.foreach(println(_))
    //
    //    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    //
    //    validGraph.vertices.collect.foreach(println(_))

    val ccGraph = graph.connectedComponents();
    val validGraph = graph.subgraph(vpred = (id, attr) =>  attr._2 != "Missing")
    val validCCGraph = ccGraph.mask(validGraph)

    validCCGraph.vertices.foreach(println(_))

    ccGraph.vertices.foreach(println(_))

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
    val maxIndegree = ccGraph.inDegrees.reduce(max)
    println(maxIndegree)
  }
}
