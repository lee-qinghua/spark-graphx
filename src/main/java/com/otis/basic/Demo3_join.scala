package com.otis.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Demo3_join {
  def main(args: Array[String]): Unit = {
    val graph = Demo2_api.getGraph()

    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext


    val outInfo: RDD[(Long, String)] = sc.parallelize(Array(
      (1L, "kgc.com"),
      (2L, "abc.edu"),
      (3L, "baidu.com")
    ))

    // v (name,age)  cmpy 是outInfo 的属性
    // 只是对能join上的数据做操作
    val graph1 = graph.joinVertices(outInfo)((id, v, cmpy) => (v._1 + "@" + cmpy, v._2))
    println(graph1.vertices.collect().mkString("Array(", ", ", ")"))

    // outerjoin
    // join不上的数据为None
    val graph2 = graph.outerJoinVertices(outInfo)((id, v1, v2) => (v1._1 + "@" + v2.getOrElse("none"), v1._2))
    println(graph2.vertices.collect().mkString("Array(", ", ", ")"))


  }
}
