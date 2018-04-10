package datalogEvaluation


import org.apache.spark.sql.SparkSession

class MagicSet(datalogProgram: DatalogProgram, spark: SparkSession) {

  private val idb_array = datalogProgram.idbList.map(_._1).toArray

  def magic_set(): DatalogProgram = {
    val query = datalogProgram.query match {
      case Some(qr) => qr
      case None => throw SemanticException("no query")
    }
    val arg_list = query.free
    //bind the constant
    for (index <- query.bindIndex) {
      query.predicate.bindFree(index) = 'b'
    }
    val query_predicate: Predicate = query.predicate.copy(id = Identifier(query.predicate.name + "_" + query.predicate.bindFree.mkString("")))
    for (i <- query_predicate.bindFree.indices) {
      query_predicate.bindFree(i) = query.predicate.bindFree(i)
    }
    query_predicate.original_name = query.predicate.name
    val query_r_h = Predicate(Identifier("query"), arg_list)
    val query_rule = Rule(head = query_r_h, body = Seq(query_predicate))
    val rw_query_p = Predicate(query_predicate.id, query.queryV)
    val rw_query_r = Rule(head = query_r_h, body = Seq(rw_query_p) ++ query.queryCondition)
    val cons = query.predicate.args.filter(_.isInstanceOf[Constant]).map(_.asInstanceOf[Constant])
    val magic_fact_name = magic_predicate_generate(query_predicate).name
    val magicFact = Fact(Identifier(magic_fact_name), args = cons)
    val clausesWithQuery = datalogProgram.clauses :+ query_rule
    val programWithQuery = datalogProgram.copy(clausesWithQuery, None)
    val adProgram = magic_adorn_clauses(programWithQuery, query_predicate)
    val mPredicate: Seq[Rule] = magic_predicates_generate(adProgram)
    val addMagicPredicate: Seq[Rule] = add_magic_predicate(adProgram)
    val rwClauses = rw_query_r +: magicFact +: mPredicate ++: addMagicPredicate
    // val rwClauses = magicFact +: mPredicate ++: addMagicPredicate
    println(rwClauses.mkString("\n"))
    val rwProgram = DatalogProgram(rwClauses, None)
    rwProgram

  }

  def magic_adorn_clauses(program: DatalogProgram, query_predicate: Predicate): Seq[Rule] = {

    // record the adorned idb
    var current_adorned_idb = Seq[Predicate](query_predicate)
    var all_adorned_idb = Set[String](query_predicate.name)

    //collect the adorned clauses
    var adorned_clauses = Seq[Rule]()

    // function for finding the idb predicate which need adorned
    def has_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
      val bind1 = p.argArray.toSet & hb // bound by the head
      val bind2 = p.argArray.toSet & pb //bound by previously evaluated predicates
      val re = if (bind1.isEmpty && bind2.isEmpty) false else true
      re
    }

    def magic_adorn_rule(rule: Rule, h: Predicate): Rule = {

      // collect the adorned predicates
      var adorned = Seq[Predicate]()

      // boound: variable occurs in a literal in the adorned list
      var pre_bind = Set[String]()

      //bound: variable is bound by the head
      var non_adorned = rule.bodies.filter(_.isInstanceOf[Predicate])

      val h_p = rule.head.copy(Identifier(rule.head.name + "_" + h.bindFree.mkString("")))

      // record the predicate name
      h_p.original_name = rule.head.name

      //pass the bf to the rewrite bound head predicate
      for (i <- h.bindFree.indices) {
        h_p.bindFree(i) = h.bindFree(i)
      }

      // adorn the head predicate
      val head_bind = (for (i <- h_p.bindFree.indices; if h_p.bindFree(i).equals('b')) yield h_p.argArray(i)).toSet


      //find the edb predicates need binding
      def edb_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
        val b = if (!idb_array.contains(p.original_name) && has_bind(p, hb, pb)) true
        else false
        b
      }

      //find the idb predicates nedd binding
      def idb_bind(p: Predicate, hb: Set[String], pb: Set[String]): Boolean = {
        val b = if (idb_array.contains(p.original_name) && has_bind(p, hb, pb)) true
        else false
        b
      }

      def find(): (Predicate, Char, Boolean) = {

        //find the next predicate need to be adorned
        //return( predicate, edb/idb, has/has not bind)

        val ebind_index: Int = non_adorned.indexWhere(edb_bind(_, head_bind, pre_bind) == true)
        if (ebind_index != -1) {
          // if has bind, true; else,false
          val e = non_adorned(ebind_index)
          non_adorned = non_adorned.take(ebind_index) ++ non_adorned.drop(ebind_index + 1)
          (e, 'e', true)
        } else {
          val ibind_index = non_adorned.indexWhere(idb_bind(_, head_bind, pre_bind))
          if (ibind_index != -1) {
            val e = non_adorned(ibind_index)
            non_adorned = non_adorned.take(ibind_index) ++ non_adorned.drop(ibind_index + 1)
            (e, 'i', true)
          } else {
            val edb_index = non_adorned.indexWhere(p => !idb_array.contains(p.original_name))
            if (edb_index != -1) {
              val e = non_adorned(edb_index)
              non_adorned = non_adorned.take(edb_index) ++ non_adorned.drop(edb_index + 1)
              (e, 'e', false)
            }
            else {
              val idb_index = non_adorned.indexWhere(p => idb_array.contains(p.original_name))
              val e = non_adorned(idb_index)
              non_adorned = non_adorned.take(idb_index) ++ non_adorned.drop(idb_index + 1)
              (e, 'i', false)
            }

          }
        }
      }

      while (non_adorned.nonEmpty) {

        val pair = find()
        val b = pair._1

        (pair._2, pair._3) match {
          case ('e', true) =>
            val bind1 = b.argArray.toSet & head_bind
            val bind2 = b.argArray.toSet & pre_bind
            val bind = bind1 ++ bind2
            for (i <- b.argArray.indices) {
              if (bind.contains(b.argArray(i))) b.bindFree(i) = 'b'
            }
            adorned = adorned :+ b
          case ('i', true) =>
            val bfs = new Array[Char](h.argArray.length)
            for (i <- bfs.indices) bfs(i) = 'f'
            val bind1 = b.argArray.toSet & head_bind
            val bind2 = b.argArray.toSet & pre_bind
            val bind = bind1 ++ bind2
            for (i <- b.argArray.indices) {
              if (bind.contains(b.argArray(i))) bfs(i) = 'b'
            }
            val ap = b.copy(id = Identifier(b.name + "_" + bfs.mkString("")))
            ap.original_name = b.name
            for (i <- ap.bindFree.indices) {
              ap.bindFree(i) = bfs(i)
            }
            if (all_adorned_idb.contains(ap.name).equals(false)) {
              current_adorned_idb = current_adorned_idb :+ ap
              all_adorned_idb = all_adorned_idb + ap.name
              // println("all:" +all_adorned_idb.mkString("，"))
            }
            adorned = adorned :+ ap
          case ('e', false) =>
            adorned = adorned :+ b
          case ('i', false) =>
            val bfs = new Array[Char](h.argArray.length)
            for (i <- bfs.indices) bfs(i) = 'f'
            val ap = b.copy(id = Identifier(b.name + "_" + bfs.mkString("")))
            ap.original_name = b.name
            for (i <- ap.bindFree.indices) {
              ap.bindFree(i) = bfs(i)
            }
            adorned = adorned :+ ap
          case _ =>

        }
        pre_bind = pre_bind ++ b.argArray
      }

      val ar = rule.copy(head = h_p, body = adorned)
      ar
    }

    while (current_adorned_idb.nonEmpty) {
      val p = current_adorned_idb.head
      val rulesToAdorn: Seq[Rule] = program.clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule]).filter(h => h.head.name.equals(p.original_name))
      for (r <- rulesToAdorn) {
        val rr = magic_adorn_rule(r, p)
        adorned_clauses = adorned_clauses :+ rr
      }
      current_adorned_idb = current_adorned_idb.tail
    }

    // println("result:" + adorned_clauses.mkString("\n"))
    adorned_clauses
  }

  def magic_predicates_generate(rules: Seq[Rule]): Seq[Rule] = {
    var magic_predicate_rules = Seq[Rule]()
    for (r <- rules) {
      val magic_head = magic_predicate_generate(r.head)
      for (i <- r.bodies.indices; if r.bodies(i).isInstanceOf[Predicate]) {
        // idb predicates
        if (idb_array.contains(r.bodies(i).original_name)) {
          if (r.bodies(i).bindFree.exists(_.equals('b'))) {
            val newHead = magic_predicate_generate(r.bodies(i))
            val newBodies: Seq[Literal] = magic_head +: r.bodies.take(i)
            val newRule = r.copy(head = newHead, body = newBodies)
            magic_predicate_rules = magic_predicate_rules :+ newRule
          }
        }
      }
    }
    magic_predicate_rules
  }


  def magic_predicate_generate(p: Predicate): Predicate = {
    val name = "m_" + p.name
    val args: Seq[Term] = for (i <- p.bindFree.indices; if p.bindFree(i).equals('b')) yield p.args(i)
    val mp = Predicate(Identifier(name), args)
    mp.original_name = p.original_name
    mp
  }

  def add_magic_predicate(rules: Seq[Rule]): Seq[Rule] = {
    var magic_rules = Seq[Rule]()
    for (r <- rules) {
      if (r.head.bindFree.exists(_.equals('b'))) {
        val newBodies = magic_predicate_generate(r.head) +: r.body
        val newRule = r.copy(body = newBodies)
        magic_rules = magic_rules :+ newRule
      }

    }
    magic_rules
  }

}