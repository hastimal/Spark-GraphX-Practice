name := "Spark-GraphX-Practice"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.4.0",
  "org.apache.spark" % "spark-yarn_2.10" % "1.3.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.4.0",
  "org.apache.spark" %% "spark-mllib" % "1.4.0",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "org.eclipse.jetty" % "jetty-client" % "8.1.14.v20131031",
  "com.typesafe.play" % "play-json_2.10" % "2.2.1",
  "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.0.0.RC1",
  "net.sf.opencsv" % "opencsv" % "2.0",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.scalaj" %% "scalaj-http" % "1.1.5",
  "com.github.fommil.netlib" % "all" % "1.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.3.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.3.0" classifier "models"
)
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.1.0"


//libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "0.9.0-incubating"
//
import scala.collection.mutable

// Specifies the main class to run when invoking $sbt run
mainClass in Compile := Some("SparkGraphXSimple")

// These are the URL's that spark looks at to resolve where to find dependencies
resolvers ++= Seq()

// These are the dependency libraries


// Jars to be excluded. Needed if you're making a super jar, which is needed when running in cluster mode
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  val inclusions: mutable.ArraySeq[String] = mutable.ArraySeq("jcabi", "aspectj", "validation", "stanford-corenlp", "natty", "ical4j", "backport")
  val jarList = cp.map(lambda => lambda.data.getName)
  val includedSet = inclusions.intersect(jarList)
  cp filter { file => inclusions.filter(pattern => file.data.getName.contains(pattern)).size == 0}
}