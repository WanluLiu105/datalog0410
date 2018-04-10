package datalogEvaluation


abstract class DatalogException(msg: String) extends Exception {

}

case class SemanticException(msg: String) extends DatalogException(msg) {
  override def toString = "Semantic Exception:" + msg
}

case class SyntacticException(msg: String) extends DatalogException(msg) {
  override def toString = "Syntactic Exception:" + msg

}