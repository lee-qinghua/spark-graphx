package com.otis.basic

import org.apache.spark.sql.SparkSession

/**
 * pageRank 算法是由搜索对网页排名的算法，假设一个网页跳转到另一个网页是给另一个网页打分，网页打分越高那排名就越高，就会优先展示
 */
object Demo5_pageRank {
  def main(args: Array[String]): Unit = {
    val graph = Demo2_api.getGraph()
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val ranks = graph.pageRank(0.0001)

    // 按照每个人计算的得分 倒序排序 找出社交网络中重要的用户
    println(ranks.vertices.sortBy(_._2, false).collect().mkString("Array(", ", ", ")"))

  }
}
