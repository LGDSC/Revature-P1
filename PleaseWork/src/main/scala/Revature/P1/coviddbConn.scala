package Revature.P1
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager, Statement}

//import org.apache.hive.jdbc.HiveDriver

object coviddbConn extends App {

  def viewUsers(): Unit ={
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/revaturep1"
    val userName = "root"
    val password ="RootPass1"

    try {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, password)
      val statement: Statement = connection.createStatement()
      println(statement.executeQuery("show databases"))

      val rs = statement.executeQuery("select * from AuthTable")
      while (rs.next) {
        val userid = rs.getString("userid")
        val passwd = rs.getString("password")
        val adminFlag = rs.getString("AdminFlag")

        println(userid, passwd, adminFlag)

      }
      connection.close()

    }catch {
      case e: Exception => e.printStackTrace()
      }
      finally {
        println("Press Enter toclose DB Connection")
      }

 }

  def somthingtoconn() {
    // connect to the database named "mysql" on port 8889 of localhost
    val url = "jdbc:mysql://localhost:3306/revaturep1"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "RootPass"
    var connection: Connection = DriverManager.getConnection(url,username, password)
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val rs = statement.executeQuery("SELECT userid, password FROM AuthTable")
      while (rs.next) {
        val userid = rs.getString("userid")
        val passwd = rs.getString("password")
        println("Username = %s, Password = %s".format(userid, passwd))
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
  }

def covidSpartContextConn() {
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val conf = new SparkConf().setMaster("local[*]").setAppName("CovidDataAnalysis")
  val sc = new SparkContext(conf)
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

  val rdd = spark.sparkContext.parallelize(List(5, 10, 30))
  println(rdd.reduce(_ + _))
  rdd.collect()
  val rdd1 = sc.parallelize(List("Venkat", "Get"))
  println(rdd1.reduce(_ + _))

  //spark.sql("CREATE TABLE sample(id INT, name STRING, Level STRING, city STRING, County STRING, state STRING, country STRING"), population INT, lat FLOAT, longitude FLOAT, url STRING, affregate FLOAT, tz STRING, cases INT, deaths INT, recovered INT, active INT, tested INT, hospitalized INT, hospitalized_current INT, discharged INT, icu INT, icu_current INT, growthfactor INT, dated DATE);");
  spark.sql("DROP TABLE IF EXISTS mainTable1")
  spark.sql("CREATE TABLE IF NOT EXISTS mainTable1(id INT, age DOUBLE, gende0r STRING, height DOUBLE,  weight DOUBLE, ap_high INT, ap_low INT) row format delimited fields terminated by ',' TBLPROPERTIES (\"skip.header.line.count\"=\"2\")")

}

  def connjdbc(){
    val spark = SparkSession.builder.master("local[2]").appName("CovidDataAnalysis").getOrCreate
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    val driverName = "org.apache.hive.jdbc.HiveDriver"
    print( sc, sqlContext, driverName)

    Class.forName(driverName)
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:hive2://localhost:10000/default")
      .option("dbtable", "clicks_json")
      .load()
    df.printSchema()
    println(df.count())
    df.show()

  }

  viewUsers()
}


