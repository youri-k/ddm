package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem")
      .map(name => s"data/TPCH/tpch_$name.csv")

    val readInputs = inputs.map(f = file => spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(file)
      .flatMap(row => row.schema.fields.zipWithIndex.map(tup => (row.getString(tup._2), tup._1.name ))))
      .reduce{(a, b) => a.union(b)}
      .groupByKey(_._1)
      .mapGroups{case (_, rows) => rows.map(_._2).toSet}
      .dropDuplicates
      .flatMap(_.toList.permutations.map(l => (l.head, l.tail)))
      .groupByKey(_._1)
      .mapGroups{case (k, it) => (k, it.foldLeft(it.next()._2){(a, b) => a.intersect(b._2)})}
      .filter(_._2.nonEmpty)
      .sort("_1")
      .collect()
      .foreach{case (dep, ref) => println(s"$dep < ${ref.sorted.mkString(", ")}")}





      //.csv(file).flatMap(row => row.schema.fields.zipWithIndex).map{ case (field, i) => Row(v, row(i)) })


      //.csv(file).flatMap(row => row.getValuesMap(row.schema.names).map{case (k, v) => (v, k)})

    /*
    for(file <- inputs){
      val dataframe =  spark.read.csv(file)
    }

     */

    /*
    inputs.map(
      file =>  spark.read.csv(file)

    )
    */

    /*
    The goal of IND discovery is to state for every irreflexive column pair (A, B) in the given relational dataset
    if A ⊆ B holds. However, instead of checking each column pair individually, SINDY efficiently checks all pairs
    in a single pass over the data

    O is defined as full outer joinf of A, B, ...
    A ⊆ B ⇔ ∀t ∈ O : (t[A] 6= ⊥ ⇒ t[B] 6= ⊥)

    SINDY consists of a join and a checking phase, too, but performs both phases in a distributed manner.

    PHASE 1 (full outer join):
      (Each worker starts out with one table !?)

      In the first phase, the full outer join O of all columns in the database
      is computed. Since each tuple in the join product is of the form t_O_v = (vA, vB, . . .)
      with v_x ∈ {v, ⊥} (i.e., each tuple can only contain null values and one distinct value v),
      these tuples can be simply represented as r(t_O_v) = (v, {X|t_O_v != ⊥})
      (This can be further simplified to an attribute set that contains only the attribute names for each tuple (that are not null !?))

      To find the attribute sets, each worker initially reads its local share of the input data and splits each read record into cells, i.e., tuples with
      a value and a singleton set containing the corresponding attribute
      E.g.d (“Thriller”, 4, “Thriller”) -> (“Thriller”, {album}), (4, {position}) and (“Thriller”, {title})

      SINDY now reorders all cells among the workers of the cluster by the value (e.g. "Thriller") using hashing.
      It is thereby guaranteed that cells with the same value are placed on the same worker

      Finally, each worker groups all its cells by their values and aggregates their attribute sets using the union.
       The value of each aggregated cell can be discarded, leaving only the attribute sets.

    PHASE 2 (Derive INDs from the join product):

      A ⊆ B ⇔ ∀ attribute set S : (A ∈ S ⇒ B ∈ S) ⇔ ... (nochmal nachlesen)

      SINDY performs this check by creating inclusion lists for each attribute set.
      Let S be an attribute set with n attributes, then n inclusion lists of the form (A, S \ A) with A ∈ S are created.
      E.g. {a,t,ta}  ->  (a, {t,ta}), (t, {a,ta}) and (ta, {a,t})

      These inclusion lists are globally grouped by the first attribute and the attribute sets are intersected

      Finally, the aggregated inclusion lists can be disassembled into INDs: For every inclusion list (A, S), each A ⊆ B (B ∈ S)
      is a valid IND
      E.g. result after grouping = (a, {t,ta})  -> a ⊆ t  and  a ⊆ ta

      The logic of SINDY can be directly expressed in terms of map and reduce operators


  Plan:
     Data Source (Multi-File)
        -> FlatMap (Split Records)
            -> Reduce (by value union attributes)
                -> FlatMap (Create inclusion lists)
                    -> Reduce (by first attribute intersect attributes)
                        -> Data Sink


    */

    // TODO
  }
}
