import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib._

// define vertex array
val verArray = Array(
  (1L, ("Philadelphia", 1580863)),
  (2L, ("Baltimore", 620961)),
  (3L, ("Harrisburg", 49528)),
  (4L, ("Wilmington", 70851)),
  (5L, ("New York", 8175133)),
  (6L, ("Scranton", 76089))
)

println(verArray)

// define edge array
val edgeArray = Array(
  Edge(2L, 3L, 113),
  Edge(2L, 4L, 106),
  Edge(3L, 4L, 128),
  Edge(3L, 5L, 248),
  Edge(3L, 6L, 162),
  Edge(4L, 1L, 39),
  Edge(1L, 6L, 168),
  Edge(1L, 5L, 130),
  Edge(5L, 6L, 159)
)

println(edgeArray)

// create RDD on top of vertices and edges array
val verRDD = sc.parallelize(verArray)
val edgeRDD = sc.parallelize(edgeArray)

// create a graph out of vertices and edges RDD
val graph = Graph(verRDD, edgeRDD)

// filter graph vertices 
graph.vertices.filter { 
   case (id, (city, population)) => population > 50000
}.collect.foreach {
    case (id, (city, population)) =>
    println(s"The population of $city is $population")
}

// create and display all the triplets
for (triplet <- graph.triplets.collect) {
  println(s"""The distance between ${triplet.srcAttr._1} and 
${triplet.dstAttr._1} is ${triplet.attr} kilometers""")
}

// filter the edges
graph.edges.filter {
  case Edge(city1, city2, distance) => distance < 150
}.collect.foreach {
  case Edge(city1, city2, distance) =>
  println(s"The distance between $city1 and $city2 is $distance")
}

// creating an undirected graph by reversing the edges
val undirectedEdgeRDD = graph.reverse.edges.union(graph.edges)
val graph = Graph(verRDD, undirectedEdgeRDD)

// aggregate
val neighbors = graph.aggregateMessages[Int](ectx => ectx.sendToSrc(ectx.dstAttr._2), _ + _)
neighbors.foreach(println(_))
