package edu.umkc.sparkGraphX.property

import org.apache.spark.graphx.{GraphLoader, VertexRDD, Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hastimal on 10/24/2015.
 */
object GraphAccessSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sc = new SparkContext(new SparkConf().setAppName("FaceBookDataGraphAccess").setMaster("local"))
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 700),
      Edge(2L, 4L, 200),
      Edge(3L, 2L, 400),
      Edge(3L, 6L, 300),
      Edge(4L, 1L, 100),
      Edge(5L, 2L, 200),
      Edge(5L, 3L, 800),
      Edge(5L, 6L, 300)
    )

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    println("#####Print users with age >30 ######")
    //    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
    //      case (id, (name, age)) => println(s"$name is $age")
    //    }
    // Solution 1
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    // Solution 2
    graph.vertices.filter(_._2._2 > 30).foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    // Solution 3
    for ((id, (name, age)) <- graph.vertices.filter(_._2._2 > 30).collect)
      println(s"$name is $age")

    println("##### Display the in-degree of each vertex. ######")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.foreach(println)

    println("##### Display who follows who (through the edges direction)??? ######")

    /**
     * Triplet has the following Fields:
     * triplet.srcAttr: (String, Int)
     * triplet.dstAttr: (String, Int)
     * triplet.attr: Int
     * triplet.srcId: VertexId
     * triplet.dstId: VertexId
     */
    graph.triplets.foreach(t => println(s"${t.srcAttr._1} follows ${t.dstAttr._1}"))

    println("#####Display who likes who (if the edge value is greater than 5).??? ######")
    graph.triplets.filter(_.attr > 5).foreach(t =>
      println(s"${t.srcAttr._1} likes ${t.dstAttr._1}"))

    println("#####Print users with who knows whom ??? ######")
    val knowTriplets = graph.triplets.collect()
    knowTriplets.foreach { triplet =>
      println(triplet.srcAttr._1 + "   knows  " + triplet.dstAttr._1)
    }
    knowTriplets.foreach { triplet =>
      println(triplet.srcAttr._1 + "   and  " + triplet.dstAttr._1 + " are " + triplet.toTuple._3 + " friends away")
    }
    println("#####Print users triplets with distance  >=3############")
    val filteredTripltes = knowTriplets.filter(triplet => triplet.attr >= 3)
    filteredTripltes.foreach(println(_))

    println("##########All triplets##########")
    println()
    knowTriplets.foreach(println(_))

    println("##########Make a user graph such that each vertex stores the number of its incoming and outgoing links.##########")

    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
    // Create a user Graph
    val initialUserGraph: Graph[User, Int] = graph.mapVertices {
      case (id, (name, age)) => User(name, age, 0, 0)
    }
    // Fill in the degree information
    /**
     * def outerJoinVertices[U, VD2](other: RDD[(VertexID, U)])
     * (mapFunc: (VertexID, VD, Option[U]) => VD2)
     * : Graph[VD2, ED]
     */
    val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }
    println("##########Display the userGraph##########")
    // Display the userGraph
    userGraph.vertices.foreach { case (id, u) => println(s"User $id is called ${u.name} and is followed by ${u.inDeg} people.")
    }
    println("##########Display the name of the users who are followed by the same number of\n      people they follow. For example Bob follows two persons, and two persons\n    follow Bob.##########")
    userGraph.vertices.filter { case (id, u) => u.inDeg == u.outDeg }
      .foreach { case (id, u) => println(u.name) }

    println("##########Display the oldest follower for each user.##########")

    /**
     * def mapReduceTriplets[MsgType](
     * // Function from an edge triplet to a collection of messages (i.e., Map)
     * map: EdgeTriplet[VD, ED] => Iterator[(VertexId, MsgType)],
     * // Function that combines messages to the same vertex (i.e., Reduce)
     * reduce: (MsgType, MsgType) => MsgType)
     * : VertexRDD[MsgType]
     */
      val oldestFollower: VertexRDD[(String, Int)] =userGraph.mapReduceTriplets[(String,Int)]( //userGraph.mapReduceTriplets[(String, Int)]
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      (a, b) => if (a._2 > b._2) a else b)
      userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) =>
      optOldestFollower match {
        case None => s"${user.name} does not have any followers."
        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
      }
    }.foreach { case (id, str) => println(str)}

      println("########## Find the average age of the followers of each user.##########")

      val averageAge: VertexRDD[Double] = userGraph.mapReduceTriplets[(Int, Double)] (
      edge => Iterator((edge.dstId, (1, edge.srcAttr.age.toDouble))),
      (a, b) => ((a._1 + b._1), (a._2 + b._2))).mapValues((id, p) => p._2 / p._1)

      userGraph.vertices.leftJoin(averageAge) { (id, user, optAverageAge) =>
        optAverageAge match {
          case None => s"${user.name} does not have any followers."
          case Some(avgAge) => s"The average age of ${user.name}'s  followers is $avgAge."
        }
      }.foreach { case (id, str) => println(str) }
    println("########## Make a subgraph of the users that are 30 or older.##########")

    val olderGraph = userGraph.subgraph(vpred = (id, u) => u.age >= 30)
    println("########## Make a subgraph of the users that are 30 or older and Compute the connected components and display the component id of each\n      user..##########")

    val cc = olderGraph.connectedComponents
    olderGraph.vertices.leftJoin(cc.vertices) {
      case (id, u, comp) => s"${u.name} is in component ${comp.get}"
    }.foreach{ case (id, str) => println(str) }
    println("##########  Write a standalone application to make a graph from the followers file\n      (shown below), measure the page rank of the graph, and display the page\n      rank of each vertex along with its user name. Each row of the users file is\n    assigning a name to a vertex..##########")

    val graph1=GraphLoader.edgeListFile(sc, "src/main/resources/inputData/followers.txt")
       // Run PageRank
    val ranks1 = graph1.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("src/main/resources/inputData/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks1).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))


  }
  }
