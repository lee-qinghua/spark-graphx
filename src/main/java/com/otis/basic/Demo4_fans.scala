package com.otis.basic

import org.apache.spark.sql.SparkSession

object Demo4_fans {

  case class User(name: String, age: Int, fans: Int, like: Int)

  def main(args: Array[String]): Unit = {
    val graph = Demo2_api.getGraph()

    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val map_graph = graph.mapVertices((id, v) => (v._1, v._2, 0, 0))
    //    print(map_graph.vertices.collect().mkString("Array(", ", ", ")"))

    // 计算粉丝量
    val fans_graph = map_graph.joinVertices(graph.inDegrees)((id, v1, v2) => (v1._1, v1._2, v1._3 + v2, v1._4))
    //    print(fans_graph.vertices.collect().mkString("Array(", ", ", ")"))

    // 计算关注量
    val fans_like_graph = fans_graph.joinVertices(graph.outDegrees)((id, v1, v2) => (v1._1, v1._2, v1._3, v1._4 + v2))
    //    print(fans_like_graph.vertices.collect().mkString("Array(", ", ", ")"))

    for ((id, v) <- fans_like_graph.vertices.collect()) {
      println(s"User $id is ${v._1} and is liked by ${v._3} people")
    }

  }
}
