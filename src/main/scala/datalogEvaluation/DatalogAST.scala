
package datalogEvaluation

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


trait DatalogAST

case class DatalogProgram(clauses: Seq[Clause], query: Option[Query]) extends DatalogAST {

  val hasQuery: Boolean = query.nonEmpty

  val ruleList: Seq[Rule] = clauses.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])

  val idbList: List[(String, StructType)] = ruleList.map(p => (p.head.name, p.headSchema)).toSet.toList

  val fList: List[(String, StructType)] = clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f => (f.name, f.schema)).toSet.toList

  val hasFact: Boolean = clauses.exists(_.isInstanceOf[Fact])

  val factList: Map[String, Seq[(String, StructType, Row)]] =
    clauses.filter(_.isInstanceOf[Fact]).map(_.asInstanceOf[Fact]).map(f => (f.name, f.schema, f.value)).groupBy(_._1)

  val safety: Either[Boolean,SemanticException] = checkSafety()
  private def checkSafety(): Either[Boolean,SemanticException] = {
    ruleList.map(_.safety).exists(!_) match{
      case true =>
           val freeVarible = ruleList.flatMap(_.freeVariable)
           Right(SemanticException("The program is not safe. Free variable: " + freeVarible.mkString(",")))
      case false =>
           Left(true)
    }
  }

  override def toString = clauses.mkString("\n")+"\n"+query
}


case class Query(predicate: Predicate) extends DatalogAST {

  val name: String = predicate.name

  //find the constraint arg
  val bind: Seq[Constant] = predicate.args.filter(_.isInstanceOf[Constant]).map(_.asInstanceOf[Constant])
  val free: Seq[Variable] = predicate.args.filter(_.isInstanceOf[Variable]).map(_.asInstanceOf[Variable])
  val bindIndex: Seq[Int] = predicate.args.zipWithIndex.filter(_._1.isInstanceOf[Constant]).map(_._2)

  // variable lists of the query predicate
  val queryV: Seq[Variable] =
    for (i <- predicate.args.indices) yield {
      predicate.args(i) match {
        case p: Variable => p
        case q: Constant => Variable("Variable_" + i)
      }
    }

  val queryP = Predicate(predicate.id, queryV)

  val queryCondition: Seq[Condition] = for (i <- predicate.args.indices ; if predicate.args(i).isInstanceOf[Constant] )
    yield Condition(Variable("Variable_" + i), "==", predicate.args(i).asInstanceOf[Constant])

  override def toString = "?" + predicate.toString

}

trait Clause extends DatalogAST

case class Fact(id: Identifier, args: Seq[Constant]) extends Clause {

  val name: String = id.value
  val argArray: Array[String] = args.map(_.value).toArray
  val arity:Int = argArray.length

  // : List[(String,StringType, Boolean)]
  val schema: StructType = StructType(argArray.zipWithIndex.map(p => StructField(p._2.toString, StringType)))
  val value: Row = Row(argArray: _*)

  override def toString = id.toString + args.mkString("(",",",")")

}

case class Rule(head: Predicate, body: Seq[Literal]) extends Clause {

  val bodies: Seq[Predicate] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate])

  val headSchema: StructType = StructType(head.argArray.zipWithIndex.map(p => StructField(p._2.toString, StringType)))

  var hasIdbSubgoal: Boolean = false

  // collect all the Variable in the RHS
  val bodyVariable: Set[String] = body.filter(_.isInstanceOf[Predicate]).map(_.asInstanceOf[Predicate]).flatMap(_.argArray.toSeq).toSet
  val freeVariable: Set[String] = (head.argArray.toSet -- bodyVariable)
  val safety: Boolean = if (freeVariable.isEmpty) true else false

  val selectCondition: Seq[Expr] = body.filter(_.isInstanceOf[Expr]).map(_.asInstanceOf[Expr])

  override def toString = head.toString + " :- " + body.mkString(",")

}

trait Literal extends DatalogAST

case class Predicate(id: Identifier, args: Seq[Term]) extends Literal {

  var name: String = id.value
  var original_name: String = id.value
  val argArray: Array[String] = args.map({
    case x: Variable => x.value
    case y: Constant => y.value
  }).toArray
  val bindFree: Array[Char] = argArray.map(_ => 'f')

  override def toString = id.toString + args.mkString("(",",",")")

}

trait Expr extends Literal

case class Condition(lhs: Variable, op: String, rhs: Term) extends Expr{
  override def toString = lhs.toString +" " + op + " " + rhs.toString
}

trait Term extends Expr

case class Constant(const: Term) extends Term {

   val value :String = const match {
    case Identifier(v) => v
    case Str(v) => v
  }

  override def toString = value
}

case class Identifier(value: String) extends Term{
  override def toString = value
}

case class Variable(value: String) extends Term{
  override def toString = value
}

case class Str(str: String) extends Term {
  override def toString = str
}