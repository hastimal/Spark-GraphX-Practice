package edu.umkc.sparkGraphX.general

/**
 * Created by hastimal on 10/16/2015.
 */

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FaceBookDataGraphAccess {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("FaceBookDataGraphAccess").setMaster("local"))
    val vertexArray = Array(
      (1L, ("Amit", 28)),
      (2L, ("Bhushan", 27)),
      (3L, ("Chandan", 65)),
      (4L, ("Dilip", 42)),
      (5L, ("Eshwar", 55)),
      (6L, ("Farhan", 50))
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

    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    println("#####Print users with age >30 ######")
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }
    println("#####Print users with who knows whom ??? ######")
    val knowTriplets = graph.triplets.collect()
    knowTriplets.foreach{ triplet =>
      println(triplet.srcAttr._1 + "   knows  " + triplet.dstAttr._1)
    }
    knowTriplets.foreach{ triplet =>
      println(triplet.srcAttr._1 + "   and  " + triplet.dstAttr._1+ " are "+triplet.toTuple._3+" friends away")
    }
    println("#####Print users triplets with distance  >=3############")
    val filteredTripltes = knowTriplets.filter(triplet => triplet.attr >=3)
    filteredTripltes.foreach(println(_))

    println("##########All triplets##########")
    println()

    knowTriplets.foreach(println(_))

  }

}