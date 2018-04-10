package datalogEvaluation
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.io._;
/**
  * @author ${user.name}
  */
object test {


  case class Par(parent: String, child: String)

  case class Person(person: String)

  case class Path(from: String, to: String)


  def main(args: Array[String]) {



    val spark = SparkSession.builder().appName("test").config("spark.master", "local"). getOrCreate()


      //config("spark.eventLog.enabled",true).config("spark.eventLog.dir","/usr/local/spark-master/").getOrCreate()
    spark.sparkContext.setCheckpointDir("/usr/local/spark-master/checkpoint/")


    import org.apache.log4j.{Level, Logger}
    import spark.implicits._

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


     val path = spark.read.format("csv").option("header","true").load("testdata/path1.txt").as[Path]

    val path1 = broadcast(path)

   // path1.write.csv("testresult/result3")

    //val par = Seq(Par("george", "dorothy"), Par("george", "evelyn"), Par("dorothy", "bertrand"), Par("hilary", "ann"), Par("evelyn", "charles"), Par("dorothy", "ann")).toDS
    //val par = Seq(Par("dorothy","dorothy"), Par( "evelyn","george"), Par("bertrand","dorothy"), Par("ann","hilary" ), Par("charles","evelyn"), Par ("ann","dorothy")).toDF()

    val person = Seq(Person("ann"), Person("bertrand"), Person("charles"), Person("dorothy"), Person("evelyn"), Person("fred"), Person("george"), Person("hilary")).toDS()

    person.cache()
    person.createOrReplaceTempView("person")

    path1.createOrReplaceTempView("path")


   /* val file = new File("/home/hadoop/Desktop/path1")
    val bw = new BufferedWriter(new FileWriter(file))
    for(i <- 1 until 99){
      bw.write(i+","+(i+1) + "\n")
    }
    bw.flush()
    bw.close()*/

    spark.catalog.listTables().show()

    val p1 = "sgc(X,X) :- person(X). " +
              "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y). "

    val p1q = "sgc(X,X) :- person(X). " +
               "sgc(X,Y) :- par(XP,X), sgc(XP,YP), par(YP, Y). " +
               "sgc('ann',X)?"

    val p2 = "tc(X,Y) :- path(X,Y). " +
             "tc(X,Y) :- path(X,Z), tc(Z,Y)."

    val p2q = "tc(X,Y) :- path(X,Y). " +
              "tc(X,Y) :- path(X,Z), tc(Z,Y)."+
              "tc(X,'20')?"

    val p3 =  "samegen(X,X) :- person(X) ." +
              "samegen(X,Y) :- parent(U,Y), samegen(Z,U), parent(Z,X) ." +
              "parent(X,Y) :- father(X,Y)." +
              "parent(X,Y) :- mother(X,Y)." +
              "father(X,Y) :- mother(Z,Y), wife(Z,X)." +
              "samegen('ann',Y)?"

    //use the second par
    val s10 = "m_sg(XP) :- sup_21(X,XP)." +
              "sup_10(X) :- m_sg(X)." +
              "sup_20(X) :- m_sg(X)." +
              "sup_21(X,XP) :- sup_20(X), par(X,XP) ."+
              "sup_22(X,YP) :- sup_21(X,XP),sg(XP,YP) ." +
              "sg(X,X) :- sup_10(X),person(X). " +
              "sg(X,Y) :- sup_22(X,YP), par(Y,YP)." +
              "m_sg('ann'). "


    val start = System.currentTimeMillis()


     val datalogTest = new Datalog(spark)
     datalogTest.datalog(p2,2)




    println("time:" + ((System.currentTimeMillis() - start) / 1000.0))
    println("count:"  )


  }

}