package edu.umkc.sparkGraphX.property

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Created by hastimal on 11/11/2015.
 * https://databricks-training.s3.amazonaws.com/graph-analytics-with-graphx.html
 */
object SparkGraphxPractice {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("SparkGraphxPractice").setMaster("local"))
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    //sc.parallelize (introduced in the Spark tutorial) construct the following RDDs from the vertexArray and edgeArray variables.
    //The vertex property for this graph is a tuple (String, Int) corresponding to the User Name and Age and the edge property is
    // just an Int corresponding to the number of Likes in our hypothetical social network.
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    //Property graph
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    System.out.println("#####Verices printing######")
    graph.vertices.foreach(println) //Verices printing
    System.out.println("#####Edges printing######")
    graph.edges.foreach(println) //Edges printing
    System.out.println("#####graph printing with triplets######")
    graph.triplets.collect().foreach(println)
    System.out.println("#####users that are at least 30 years old######")
    //Use graph.vertices to display the names of the users that are at least 30 years old. The output should contain (in addition to lots of log messages):
    // Solution 1
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    // Solution 2
    graph.vertices.filter(v => v._2._2 > 30).collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    // Solution 3
    for ((id, (name, age)) <- graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect) {
      println(s"$name is $age")}
      System.out.println("#####  Use the graph.triplets view to display who likes who######")
      //Solution -1
      for (triplet <- graph.triplets.collect) {
        println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
      }///for (triplet <- graph.triplets.collect) {
        /**
         * Triplet has the following Fields:
         *   triplet.srcAttr: (String, Int) // triplet.srcAttr._1 is the name
         *   triplet.dstAttr: (String, Int)
         *   triplet.attr: Int
         *   triplet.srcId: VertexId
         *   triplet.dstId: VertexId
         */
      //}
      //solution -2

      graph.triplets.collect.foreach {v=>println(s"${v.srcAttr._1} like.. ${v.dstAttr._1}")
       // case ((id, (name, age)), (id1, (name1, age1)), int) => println(s"$name like.. $name1")

        //If someone likes someone else more than 5 times than
        // that relationship is getting pretty serious. For extra credit, find the lovers.
        System.out.println("#####  Use the graph.triplets view to display someone else more than 5 times than######")
        for (triplet <- graph.triplets.filter(t=>t.attr>5).collect) {
          println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
        }
      }
      }


  }