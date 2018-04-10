package datalogEvaluation

import scala.util.parsing.combinator.RegexParsers


object DatalogParser extends RegexParsers {

  lazy val datalogProgram: Parser[DatalogProgram] = clauseList ~ opt(query) ^^ {
    case cl ~ None => DatalogProgram(cl, None)
    case cl ~ Some(qr) => DatalogProgram(cl, Some(qr))
  }

  lazy val query: Parser[Query] = predicate <~ "?" ^^ {
    Query(_)
  }

  lazy val clause: Parser[Clause] = rule | fact

  lazy val clauseList: Parser[Seq[Clause]] = rep1(clause)

  lazy val rule: Parser[Rule] = predicate ~ ":-" ~ literalList ~ "." ^^ {
    case l ~ _ ~ ll ~ _ => Rule(l, ll)
  }

  lazy val fact: Parser[Fact] = identifier ~ "(" ~ constList ~ ")" ~ "." ^^ {
    case id ~ "(" ~ cl ~ ")" ~ "." => Fact(id, cl)
  }

  lazy val constList: Parser[Seq[Constant]] = rep1sep(constant, ",")

  lazy val literalList: Parser[Seq[Literal]] = rep1sep(literal, ",")

  lazy val literal: Parser[Literal] = predicate | condition

  lazy val predicate: Parser[Predicate] = identifier ~ "(" ~ terms ~ ")" ^^ {
    case id ~ _ ~ ts ~ _ => Predicate(id, ts)
  }

  lazy val condition: Parser[Expr] = variable ~ ("==" | "!=" | ">" | "<" | ">=" | "<=") ~ term ^^ {
    case t1 ~ sym ~ t2 => Condition(t1, sym, t2)
  }

  lazy val terms: Parser[Seq[Term]] = rep1sep(term, ",")

  lazy val term: Parser[Term] = variable | constant

  lazy val constant: Parser[Constant] = identifier ^^ { id => Constant(id) } | string ^^ { str => Constant(str) }

  lazy val identifier: Parser[Identifier] = "[a-z][a-zA-Z0-9_]*".r ^^ {
    Identifier(_)
  }

  lazy val variable: Parser[Variable] = "[A-Z][a-zA-Z0-9_]*".r ^^ {
    Variable(_)
  }

  lazy val string: Parser[Str] =
    """'[\w\s]*'""".r ^^ {
      str =>
        val str1 = str.drop(1)
        val str2 = str1.dropRight(1)
        Str(str2)
    }

  def apply(code: String): Either[DatalogException, DatalogProgram] = {
    parse(datalogProgram, code) match {
      case NoSuccess(msg, next) =>
        Left(SyntacticException(msg))
      case Success(result, next) =>
        Right(result)
    }
  }
}