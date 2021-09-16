package com.otis.basic

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
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
    val graph = Graph(vertexRdd, edgeRdd)

    // 获取所有的点
    println(graph.vertices.collect().mkString("Array(", ", ", ")"))
    // 获取所有的边
    println(graph.edges.collect().mkString("Array(", ", ", ")"))
    // 获取三元组
    println(graph.triplets.collect().mkString("Array(", ", ", ")"))

    // 获取所有点的入度  就是几个关系指向这个点
    println(graph.inDegrees.collect().mkString("Array(", ", ", ")"))
    // 所有点的出度
    println(graph.outDegrees.collect().mkString("Array(", ", ", ")"))
    // 所有点的度
    println(graph.degrees.collect().mkString("Array(", ", ", ")"))

    // 边的数量
    println(graph.numEdges)
    // 点的数量
    println(graph.numVertices)
  }
}
