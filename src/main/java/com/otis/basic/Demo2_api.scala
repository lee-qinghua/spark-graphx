package com.otis.basic

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object Demo2_api {
  def main(args: Array[String]): Unit = {
    val graph = getGraph()

    println(graph.triplets.collect().mkString("Array(", ", ", ")"))
    // 改变了graph的点结构
    val graph1: Graph[String, PartitionID] = graph.mapVertices {
      case (id, (name, age)) => (name)
    }
    val graph2: Graph[(VertexId, String), PartitionID] = graph.mapVertices((x, y) => (x, y._1))

    //    println(graph2.vertices.collect().mkString("Array(", ", ", ")"))

    val graph3 = graph.mapEdges(e => Edge(e.srcId, e.dstId, e.attr + 100))
    val graph4 = graph.mapEdges(e => e.attr + 100)
    //    println(graph4.edges.collect().mkString("Array(", ", ", ")"))

    // reverse 改变边的方向 指向
    // ((2,(b,13)),(1,(a,11)),7) ==> ((1,(a,11)),(2,(b,13)),7)
    //    println(graph.reverse.triplets.collect().mkString("Array(", ", ", ")"))

    // 相当于过滤 得到attr第二个属性大于13的子图
    val graph5 = graph.subgraph(vpred = (id, attr) => attr._2 > 13)

    println(graph5.vertices.collect().mkString("Array(", ", ", ")"))

  }

  def getGraph(): Graph[(String, Int), Int] = {
    // 1.创建环境
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()


    val vertexArray = Array(
      (1L, ("a", 11)),
      (2L, ("b", 13)),
      (3L, ("c", 15)),
      (4L, ("d", 16)),
      (5L, ("e", 13)),
      (6L, ("f", 17))
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


    val sc = spark.sparkContext
    val vertexRdd = sc.parallelize(vertexArray)
    val edgeRdd = sc.parallelize(edgeArray)

    // 创建graph
    Graph(vertexRdd, edgeRdd)
  }
}
