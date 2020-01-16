package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._

    inputs.map(f = file => spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(file)
      .flatMap(row => row.schema.fields.zipWithIndex.map(tup => (row.getString(tup._2), tup._1.name ))))
      .reduce{(a, b) => a.union(b)}
      .groupByKey(_._1)
      .mapGroups{case (_, rows) => rows.map(_._2).toSet}
      .dropDuplicates
      .flatMap(set => set.map(e => (e, set.diff(Set(e)))))
      .groupByKey(_._1)
      .mapGroups{case (k, it) => (k, it.foldLeft(it.next()._2){(a, b) => a.intersect(b._2)})}
      .filter(_._2.nonEmpty)
      .sort("_1")
      .collect()
      .foreach{case (dep, ref) => println(s"$dep < ${ref.toList.sorted.mkString(", ")}")}
  }
}
