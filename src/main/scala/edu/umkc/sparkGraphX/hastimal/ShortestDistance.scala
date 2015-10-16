package edu.umkc.sparkGraphX.hastimal
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hastimal on 10/16/2015.
 */


object ShortestDistance {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    val sc = new SparkContext(new SparkConf().setAppName("ShortestDistance").setMaster("local"))
    val graph: Graph[Long, Double] =
      GraphGenerators.logNormalGraph(sc, numVertices = 100).mapEdges(e => e.attr.toDouble)

    val sourceId: VertexId = 42
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a,b) => math.min(a,b) // Merge Message
    )
    println(sssp.vertices.collect.mkString("\n"))
  }

}