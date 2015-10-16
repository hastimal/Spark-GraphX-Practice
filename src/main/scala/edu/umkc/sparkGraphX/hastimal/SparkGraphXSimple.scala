package edu.umkc.sparkGraphX.hastimal

import java.io.{File, PrintWriter}
import scala.language.postfixOps
import scala.reflect.io.Path
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
 * /** Suppose I want to build a graph from some text files, restrict
 * the graph to important relationships and users, run page-rank on
 * the sub-graph, and then finally return attributes associated with
 * the top users. I can do all of this in just a few lines with GraphX:*/
 * */
/**
 *  * All information is found from the Spark site :
 * http://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel
 *
 * Structure of Graph Class in Spark.graphx -
 * class Graph[VD, ED] {
 * val vertices: VertexRDD[VD]
 * val edges: EdgeRDD[ED]
 * }
 *
 * Created by hastimal on 10/12/2015.
 */



// you can also extend App and then there is no need for a main method
object SparkGraphXSimple {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "F:\\winutils")
    //Deleting output files recursively if exists
    val dir = Path("src/main/resources/outputData")
    if (dir.exists) {
      dir.deleteRecursively()
      println("Successfully existing output deleted!!")
    }
    val file  = new File("src/main/resources/outputData").mkdir();
    val pw = new PrintWriter(new File("src/main/resources/outputData/result.txt"))
    /**
     * This is the basic two lines of code needed to setup Spark
     */
    val conf = new SparkConf().setAppName("SparkGraphXSimple").setMaster("local[*]").set("spark.executor.memory", "3g")
    val sc: SparkContext = new SparkContext(conf)

    /**
     * Pre-processing meta-data step
     * Load my user data and parse into tuples of user id and attribute list
     * Split each line by commas and generate a tuple of (uid, list of attributes)
     */
    val users : RDD[(Long, Array[String])]= sc.textFile("src/main/resources/inputData/users.txt")
      .map(line => line.split(","))
      .map(parts => (parts.head.toLong, parts.tail))

    /**
     * Load the edgelist file which is in uid -> uid format
     */
   // val followerGraph = GraphLoader.edgeListFile(sc, args(1))
    val followerGraph = GraphLoader.edgeListFile(sc, "src/main/resources/inputData/followers.txt")
    /**
     * Performs an outer join of the follower graph and the user graph
     * (uid, uid) join (uid, list of attributes) -> ??
     * TODO :: What exactly is happening here? Isn't this just the same as the userlist?
     */
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    pw.write("\nThe follower graph joined with the user graph\n")
   // pw.write(graph.vertices.collect().mkString("\n"))
    pw.write(graph.vertices.collect().mkString("\n"))

    /**
     * Subgraph is a graph filter function
     */

    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)
    pw.write("\nThe subgraph\n")
    pw.write(subgraph.vertices.collect().mkString("\n"))
    /**
     * This function calls PageRank.runUntilConvergence which takes the following parameters
     * graph the graph on which to compute PageRank
     *tol the tolerance allowed at convergence (smaller => more accurate).
     *resetProb the random reset probability (alpha)
     */
    val pageRankGraph = subgraph.pageRank(0.001)
    pw.write("\nThe page rank results of the sub-graph\n")
    pw.write(pageRankGraph.vertices.collect().mkString("\n"))
    /**
     * Performs an outer join which is a union operation with an VertexRDD
     */
    val userInfoWithPageRank = subgraph.outerJoinVertices(pageRankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }
    pw.write("\nThe results graph\n")
    pw.write(userInfoWithPageRank.vertices.top(100)(Ordering.by(_._2._1)).mkString("\n"))
    pw.close()

  }
}