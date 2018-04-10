package datalogEvaluation

/**
  * Created by Wanlu on 3/13/18.
  */
import org.apache.spark.sql.SparkSession

class Datalog(sparkSession: SparkSession){


  def datalog(program: String, evaluation: Int): Unit = {

    val datalogProgram = parse(program)

    datalogProgram.safety match{
      case Right(ex) => throw ex
      case _ =>
    }

    //val sparkSession = SparkSession.builder().appName("test").config("spark.master", "local"). getOrCreate()

    val evaluator = new Evaluator(datalogProgram, sparkSession)
    val magic = new MagicSet(datalogProgram, sparkSession)

    evaluation match {
      // naive
      case 1 =>
        val evaluator = new Evaluator(datalogProgram, sparkSession)
        evaluator.naive()
      // semi naive
      case 2 =>
        val evaluator = new Evaluator(datalogProgram, sparkSession)
        evaluator.semi_naive()
      //magic + naive
      case 3 =>
        val magic = new MagicSet(datalogProgram, sparkSession)
        val rwProgram = magic.magic_set()
        val evaluator = new Evaluator(rwProgram, sparkSession)
        evaluator.naive()
      //magic + semi naive
      case 4 =>
        val magic = new MagicSet(datalogProgram, sparkSession)
        val rwProgram = magic.magic_set()
        val evaluator = new Evaluator(rwProgram, sparkSession)
        evaluator.semi_naive()
    }

  }

  def parse(code: String): DatalogProgram = {
    DatalogParser(code) match {
      case Right(datalogProgram) => datalogProgram
      case Left(datalogException) => throw datalogException
    }
  }


}
