package datalogEvaluation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

class Evaluator(datalogProgram: DatalogProgram, spark: SparkSession) {

  private val idb = datalogProgram.idbList
  private val idb_array: Array[String] = idb.map(_._1).toArray

  /*def broadcastBaseRelation: Unit = {
      spark.catalog.listTables().foreach(table => broadcast(spark.table(table.name)))
  }*/

  def semi_naive(): Unit = {

     def toEvaluate(rule: Rule, hashMap: mutable.HashMap[String, Boolean]): Boolean = {
      val newResult = rule.bodies.filter(p => p.name.take(2).equals("d_")).filter(p => hashMap(p.original_name))
      val result = if (newResult.isEmpty) true else false
      result
    }

    val start = System.currentTimeMillis()

    val evaluateRules = testIDB()
    //idb = empty
    for (i <- idb) {
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.cache().createOrReplaceTempView(i._1)
    }

    loadFact()

    for (i <- idb) {
      val delta = spark.table(i._1)
      val name = "d_" + i._1
      delta.cache().createOrReplaceTempView(name)
    }

    var fixpoint = false
    val n = idb.length
    val fixpoints: Array[Boolean] = new Array(n)

    val recursiveRules = new Array[Seq[Rule]](n)

    // initialization
    for (i <- idb_array.indices) {
      val edb_rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(!_._3).map(_._2)
      val edb_rules = for (i <- edb_rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
      if (edb_rules.nonEmpty) {
        val name = edb_rules.head.head.name
        val delta_name = "d_" + name
        val dfs =
          if (datalogProgram.fList.map(_._1).contains(name))
            edb_rules.map(evaluate_rule(_, spark)).filter(_.head(1).nonEmpty) :+ spark.table("fact_" + name)
          else
            edb_rules.map(evaluate_rule(_, spark)).filter(_.head(1).nonEmpty)

        val delta: DataFrame = dfs.reduce((l, r) => l.union(r)).distinct().cache()
        delta.createOrReplaceTempView(delta_name)
        val all = delta.cache()
        all.createOrReplaceTempView(name)
      }
    }

    var iter = 1

    while (!fixpoint) {
      println("iter:" + iter)

      for (i <- idb) {
        val df = spark.table("d_" + i._1)
        df.createOrReplaceTempView("pre_d_" + i._1)
      }

      var pre_fixpoints = mutable.HashMap[String, Boolean]()

      for (i <- idb_array.indices) {
        pre_fixpoints += (idb_array(i) -> fixpoints(i))
      }

      for (i <- idb_array.indices) {
        var rules = Seq[Rule]()
        iter match {
          case 1 =>
            val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).filter(_._3).map(_._2)
            rules = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
            val rewriteRules = semi_naive_rewrite(rules)
            recursiveRules(i) = rewriteRules
            //val rulesToEvaluate = recursiveRules(i)
            val rulesToEvaluate = recursiveRules(i).filter(toEvaluate(_, pre_fixpoints))
            fixpoints(i) = semi_naive_evaluate_idb(rulesToEvaluate, spark)
          case _ =>
            // val rulesToEvaluate = recursiveRules(i)
            val rulesToEvaluate = recursiveRules(i).filter(toEvaluate(_, pre_fixpoints))
            fixpoints(i) = semi_naive_evaluate_idb(rulesToEvaluate, spark)
        }
      }

      for (i <- idb) {
        val delta_name = "d_" + i._1
        if (!spark.table(delta_name).head(1).isEmpty) {
          val delta = spark.table(delta_name).localCheckpoint(true)
          delta.createOrReplaceTempView(delta_name)
          val all = Util.union(spark.table(i._1), spark.table(delta_name)).localCheckpoint(true)
          all.cache().createOrReplaceTempView(i._1)

        }
      }

      println(fixpoints.mkString(","))
      fixpoint = if (fixpoints.exists(_.equals(false))) false else true
      iter = iter + 1
    }

    output();
    println("semi_naive:" + (System.currentTimeMillis() - start))
  }


  private def semi_naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession): Boolean = {



    var fixpoint = true
    if (rules.isEmpty) fixpoint
    else {
      //1. name
      val name = rules.head.head.name
      val delta_name = "d_" + name
      //2. evaluate rule
      val dfs = rules.map(evaluate_rule(_, spark)).filter(_.head(1).nonEmpty)

      val start = System.currentTimeMillis()
      //3. update delta
      if (dfs.nonEmpty) {
        val result = dfs.reduce((l, r) => Util.union(l, r)).cache()
        val delta: DataFrame = result.except(spark.table(name)).cache()
        if (delta.head(1).nonEmpty) {
          delta.createOrReplaceTempView(delta_name)
          fixpoint = false
        }
      }
      else {
        val delta = spark.emptyDataFrame.cache()
        delta.createOrReplaceTempView(delta_name)
      }

      println("evaluate idb: " + (System.currentTimeMillis() - start))
      fixpoint
    }
  }

  private def evaluate_rule(rule: Rule, spark: SparkSession): DataFrame = {

    val relations: Seq[DataFrame] = rule.bodies.map(Util.giveAlias(_, spark)) //.filter(_.isInstanceOf[DataFrame])
    val result: DataFrame = {
      relations.length match {
        case 0 =>
          spark.emptyDataFrame
        case _ =>
          val selectCondition = rule.selectCondition
          val cols: List[String] = rule.head.argArray.toList
          val joined = relations.reduce(Util.naturalJoin(_, _, spark))
          val selected = if (selectCondition.nonEmpty) Util.select(selectCondition, joined) else joined
          Util.project(cols, selected, spark)
      }
    }
    result
  }

  private def semi_naive_rewrite(rules: Seq[Rule]): Seq[Rule] = {
    var rewriteRules = Seq[Rule]()
    for (r <- rules) {
      var rw = false
      for (i <- r.bodies.indices)
        if (idb_array.contains(r.bodies(i).name)) {
          rw = true
          val rp = r.bodies(i).copy(id = Identifier("d_" + r.bodies(i).name))
          rp.original_name = r.bodies(i).name
          val rbody: Seq[Literal] = r.body.patch(i, Seq(rp), 1)
          val rr = r.copy(body = rbody)
          rewriteRules = rewriteRules :+ rr
        }
      if (!rw)
        rewriteRules = rewriteRules :+ r
    }
    rewriteRules
  }

  private def loadFact(): Unit = {
    if (datalogProgram.hasFact) {
      val names: Seq[String] = datalogProgram.factList.map(_._1).toSeq
      val schema: Seq[StructType] = datalogProgram.factList.map(p => p._2.head._2).toSeq
      val value: Seq[RDD[Row]] = datalogProgram.factList.map(p => spark.sparkContext.parallelize(p._2.map(_._3))).toSeq
      for (i <- names.indices) {
        var df: DataFrame = spark.createDataFrame(value(i), schema(i))
        if (spark.catalog.tableExists(names(i))) {
          println(names(i))
          val df0 = spark.table(names(i))
          df = df.union(df0)
        }

        val df1 = broadcast(df)
        df1.cache().createOrReplaceTempView(names(i))
        df1.cache().createOrReplaceTempView("fact_" + names(i))
      }
    }
  }

  private def testIDB() = {
    datalogProgram.clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule]).foreach(markIDB)

    val evaluateRulesLine = datalogProgram.clauses.zipWithIndex.filter(_._1.isInstanceOf[Rule]).
      map(pair => (pair._1.asInstanceOf[Rule].head.name, pair._2, pair._1.asInstanceOf[Rule].hasIdbSubgoal)).groupBy(_._1)

    evaluateRulesLine
  }

  private def markIDB(rule: Rule) = {
    val ary = datalogProgram.idbList.map(_._1).toArray
    rule.hasIdbSubgoal = rule.bodies.exists(p => ary.contains(p.name))
  }

  def naive(): Unit = {

    val evaluateRules: Map[String, Seq[(String, Int, Boolean)]] = testIDB()

    //idb = empty
    for (i <- idb) {
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], i._2)
      df.createOrReplaceTempView(i._1)
    }

    loadFact()

    var fixpoint = false
    var iter = 1
    val n = idb.length
    val fixpoints: Array[Boolean] = new Array(n)

    while (!fixpoint) {


       // spark.table("tc").write.csv("testresult/result_" + iter)


      println("iter:" + iter)
      for (i <- idb) {
        val df = spark.table(i._1).cache()
        df.createOrReplaceTempView("pre_" + i._1)
      }

      for (i <- idb.indices) {

        val rulesIndice: Seq[Int] = evaluateRules(idb(i)._1).map(_._2)

        val rules: Seq[Rule] = for (i <- rulesIndice) yield datalogProgram.clauses(i).asInstanceOf[Rule]
        fixpoints(i) = naive_evaluate_idb(rules, spark)

        if (iter == 0)
          fixpoints(i) = false
      }

      println(fixpoints.mkString(","))
      fixpoint = if (!fixpoints.exists(_.equals(false))) true else false
      iter = iter + 1
    }

    output();
  }

  private def naive_evaluate_idb(rules: Seq[Rule], spark: SparkSession): Boolean = {

    val start = System.currentTimeMillis()
    var fixpoint = true
    if (rules.nonEmpty) {
      val name = rules.head.head.name
      val dfs =
        if (datalogProgram.fList.map(_._1).contains(name))
          rules.map(evaluate_rule(_, spark)).filter(_.head(1).nonEmpty) :+ spark.table("fact_" + name)
        else
          rules.map(evaluate_rule(_, spark)).filter(_.head(1).nonEmpty)
      if (dfs.nonEmpty) {
        val result: DataFrame = dfs.reduce((l, r) => Util.union(l, r)).cache()
        if (result.except(spark.table("pre_" + name)).head(1).nonEmpty) {
          val all = result.localCheckpoint(true)
          all.createOrReplaceTempView(name)
          fixpoint = false
        }
      }
    }
    println("evaluate idb:" + (System.currentTimeMillis() - start))
    fixpoint
  }

  private def output() ={

    idb_array.foreach(n => spark.table(n).write.csv("testresult/result_" +n))

  }
}