package Revature.P1
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, Statement}
import scala.io.StdIn.readInt
import scala.io.StdIn.readLine
import scala.tools.jline_embedded.console.ConsoleReader

object AdminUser extends App {

    def main(): Unit =
    {   var menuSelection =0;
        do{
            menuSelection = menu()
            menuSelection match {
            case 1 => CreateAdmin()
            case 2 => CreateUser()
            case 3 => createandLoadTable()
            case 4 => covidDataAnalysis()
            case 5 => changePassword()
            case 0 => println("Exiting....")
            }
        }while (menuSelection !=0)
    }

    def changePassword(): Unit ={

            val driver = "com.mysql.jdbc.Driver"
            val url = "jdbc:mysql://localhost:3306/revaturep1"
            val userName = "root"
            val password ="RootPass1"

            try {
                Class.forName(driver)
                val connection: Connection = DriverManager.getConnection(url, userName, password)
                val statement: Statement = connection.createStatement()

                println("Enter the username you want to change password for")
                val uName = readLine()
                println("Please enter the old password")
                val oldpass = readLine()
                println("Please choose New password")
                val newPass = readLine()
                println("Do you want to make this user Admin [Yes/No]" )
                var createauthType = false;
                val atype = readLine()
                if (( atype == "yes") || (atype == "YES")) {
                    createauthType = true
                } else if((atype== "no") || (atype== "NO"))
                {
                    createauthType=false
                }

                val updateString = "update AuthTable Set password= \""+ newPass+ "\", AdminFlag =" +  createauthType + " Where userid = \""+ uName + "\" and password = \"" + oldpass+"\""
                println(updateString)
                val insNewuser = statement.execute(updateString)

                if (insNewuser == true)
                    println("Your password changed successfully")
                connection.close()

            }catch {
                case e: Exception => e.printStackTrace()
            }
            finally {
                println("Press Enter toclose DB Connection")
            }

    }

    def createandLoadTable(){
        System.setProperty("hadoop.home.dir", "C:\\hadoop")
        val spark = SparkSession.builder().appName("AdminUser").config("spark.master", "local").enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        var menuSelection=0;

        //craete a table
        spark.sql("DROP TABLE IF EXISTS FULL_GROUPED")
        spark.sql("CREATE TABLE IF NOT EXISTS FULL_GROUPED(DateCol DATE, Country_Region STRING, ConfimedCases INT, Deaths INT, Recovered INT, Active INT, NewCases INT, NewDeaths INT, NewRecoveries INT, WHORegion STRING) ROW FORMAT Delimited Fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'Covid19DataSet/full_grouped.csv' INTO TABLE FULL_GROUPED")

        spark.sql("DROP TABLE IF EXISTS AuthTable")
        spark.sql("CREATE TABLE IF NOT EXISTS AuthTable(userid String, passwd String, admin Boolean) ROW FORMAT Delimited Fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'Covid19DataSet/user.csv' INTO TABLE AuthTable")

        println("New Cases")
        spark.sql("SELECT DateCol, Country_Region, ConfimedCases, Recovered, Active FROM FULL_GROUPED WHERE NewCases > 0").show(50)
        println("Press Enter to continue to get Conformed Cases before 2021-Jan-22")
        readLine()

        spark.sql("SELECT * FROM FULL_GROUPED WHERE ConfimedCases > 0 AND DateCol < '2021-01-22").show(50)
        readLine()
        println("Press Enter to continue to get Conformed Cases before  ")
        spark.sql("SELECT * FROM FULL_GROUPED WHERE ConfimedCases > 0 and (DateCol < '2021-02-01' & DateCol > '2021-01-22')").show()

        println("Please enter to continue to run analysis on Mortality")
        readLine()
        spark.sql("SELECT * FROM FULL_GROUPED WHERE Deaths > 0.orderBy deaths").show(200)

        println("Please enter to continue to run analysis on Recovered Cases")
        readLine()
        spark.sql("SELECT * FROM FULL_GROUPED WHERE Recovered > 0").show(100)
        println("Please enter to continue to run analysis on Active Cases")
        readLine()
        spark.sql("SELECT * FROM FULL_GROUPED WHERE Active > 0 order by Active DeSC").show(100)

        println(" Please enter to run analysis on Total Cases history by All countries")
        //spark.sql("SELECT count(ConfimedCases), count(NewCases), count(Recovered), count(Deaths), count(Active) FROM FULL_GROUPED").show(100)

        spark.sql("select DateCol, Country_Region, ConfimedCases, Deaths, Recovered, Active, NewCases, NewDeaths from full_grouped limit 50").show(100)

    }

    def CreateAdmin(): Unit ={
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/revaturep1"
        val userName = "root"
        val password ="RootPass1"

        try {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, password)
            val statement: Statement = connection.createStatement()
            println(statement.executeQuery("show databases"))

            println("What is the Admin username you want to create")
            val createUserName = readLine()
            println("Please set the default passwd")
            val createPasswd = readLine()
            println("Do you want to make this user Admin [Yes/No]" )
            var createauthType = false;
            val atype = readLine()
            if (( atype == "yes") || (atype == "YES")) {
                createauthType = true
            } else if((atype== "no") || (atype== "NO"))
            {
                createauthType=false
            }

            val addNewUserStr = "insert into AuthTable (userid, password, AdminFlag) values(\""+ createUserName +"\",\"" + createPasswd +"\","+ createauthType +")"
            println(addNewUserStr)

            //      val is = statement.execute("insert into AuthTable (userid, password, AdminFlag) values("+ createUserName +"," + createPasswd +","+ createauthType +")")
            val insNewuser = statement.execute(addNewUserStr)
            if (insNewuser == true)
                println("New user created successfully")
            connection.close()

        }catch {
            case e: Exception => e.printStackTrace()
        }
        finally {
            println("Press Enter toclose DB Connection")
        }

    }

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
    def menu():Int={
        print("\033[2J")
        println("MainMenu")
        println("********")
        print("1. Create Admin \n2. Create user \n3. CreateTable and Load Date\n4. Covid Data Analysis Menu\n5. Change Password\n0. Exit \n ..Select your choice: ")
        readInt();
    }

    def CreateAdmin1()
    {
        System.setProperty("hadoop.home.dir", "C:\\hadoop")
        val spark = SparkSession.builder().appName("AdminUser").config("spark.master", "local").enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        spark.sql("DROP TABLE IF EXISTS AuthTable")
        spark.sql("CREATE TABLE IF NOT EXISTS AuthTable(userid String, passwd String, admin Boolean) ROW FORMAT Delimited Fields terminated by ','")
        spark.sql("LOAD DATA LOCAL INPATH 'Covid19DataSet/user.csv' INTO TABLE AuthTable")
        spark.sql(" INSERT INTO AuthTable VALUES ('BobBrown', 'PassWord', false)")
        println("What is the Admin username you want to create")
        val username = readLine()
        println("Please set the default passwd")
        val passwd =
        println("Do you want to make this user Admin [Yes/No]" )
        val authType = readLine()
        var admin= false;

        if (authType == "yes" || authType == "YES")1
             admin = true

        spark.sql("INSERT OVERWRITE AuthTable VALUES ('MARX','passwd', false)")

    }

    def CreateUser(): Unit ={
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/revaturep1"
        val userName = "root"
        val password ="RootPass1"

        try {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, password)
            val statement: Statement = connection.createStatement()
            println(statement.executeQuery("show databases"))

            println("What is the Admin username you want to create")
            val createUserName = readLine()
            println("Please set the default passwd")
            val createPasswd = readLine()
            println("Do you want to make this user Admin [Yes/No]" )
            var createauthType = false;


            val newUserStr = "insert into AuthTable (userid, password, AdminFlag) values(\""+ createUserName +"\",\"" + createPasswd +"\","+ createauthType +")"
            println(newUserStr)

           val insNewuser = statement.execute(newUserStr)
            if (insNewuser == true)
                println("new user is succesfully created")

         connection.close()

        }catch {
            case e: Exception => e.printStackTrace()
        }
        finally {
            println("Press Enter to close DB Connection")
        }

    }

    def usermenu(){

    }

    def createUser(): Unit = {
    }

    def covidDataAnalysis(){
        System.setProperty("hadoop.home.dir", "C:\\hadoop")
        val spark = SparkSession.builder().appName("AdminUser").config("spark.master", "local").enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        //craete a table
        println("New Cases")
        spark.sql("SELECT DateCol, ConfimedCases, Recovered, Active FROM FULL_GROUPED WHERE NewCases > 0").show(50)
        println("Conformed Cases")
        spark.sql("SELECT * FROM FULL_GROUPED WHERE ConfimedCases > 0").show(100)
        println("Mortality")
        spark.sql("SELECT DateCol, Deaths, Country_Region FROM FULL_GROUPED WHERE Deaths > 0").show(100)
        println("Recovered Cases")
        spark.sql("SELECT Recovered, DateCol, Country_Region FROM FULL_GROUPED WHERE Recovered > 0 and DateCol <'2020-12-31' ").show(100)
        println("Active Cases")
        spark.sql("SELECT * FROM FULL_GROUPED WHERE Active > 0 order by Active DeSC").show(100)

        println("Total Cases history by All countries")
        //spark.sql("SELECT count(ConfimedCases), count(NewCases), count(Recovered), count(Deaths), count(Active) FROM FULL_GROUPED").show(100)

        spark.sql("select DateCol, Country_Region, ConfimedCases, Deaths, Recovered, Active, NewCases, NewDeaths from full_grouped limit 50").show(100)

        println("Future prediction of New cases")

        spark.sql("SELECT (AVG(NewCases)-AVG(NewRecoveries))AS FutureExpect FROM FULL_GROUPED").show()

    }

    def login() {
        print("\033[2J")
        System.setProperty("hadoop.home.dir", "C:\\hadoop")
        val spark = SparkSession.builder().appName("AdminUser").config("spark.master", "local").enableHiveSupport().getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        print("\033[2J")

        println("please enter your username")
        val username = readLine()
        println("Enter your password")
        val password = readLine()


     //   val utype =  verifyloginpassword(username, password)
        if (username == "su" && password =="root")
            main()
        else
            {
                println("This login does not exist, You are previleged to let you view Covid Data analysis as guest as Guest")
                covidDataAnalysis()
            }

     //   val textS = spark.sql("SELECT ADMIN  FROM AUTHTABLE WHERE userid= $'username' and passwd = $'password'").show()
    }

    def verifyLogin()
    {
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/revaturep1"
        val userName = "root"
        val password ="RootPass1"

        println("please enter your username")
        val uName = readLine()
        println("Enter your password")
        val passwd = readLine()

        println(uName, passwd)
        try {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, password)
            println(connection)
            val statement: Statement = connection.createStatement()
            println(statement.executeQuery("show databases"))

            val verifyStr = "select * from AuthTable where ((userid= \'"+uName+ "\') and (password = \'"+passwd+"\'))"
            println(verifyStr)
            val rs = statement.executeQuery(verifyStr)
            rs.next()
            var adminFlag:Boolean = rs.getBoolean("AdminFlag")
            println(rs.getRow)
            val rsusrs = rs.getString("userid")
            println(rsusrs)
            /*/~~~~~~~~~~~~~~~
            while (rs.next) {
                val rsuser = rs.getString("userid")
                val rspasswd = rs.getString("password")
                  adminFlag = rs.getBoolean("AdminFlag")
                print("userid = %s, password = %s".format(rsuser, rspasswd))

               }
            //~~~~~~~~~~~~~~~*/

                if (adminFlag == true)
                {
                    main()
                }
                else {
                    usermenu()
                    println("This login does not exist, You are previleged to let you view Covid Data analysis as guest as Guest")
                    covidDataAnalysis()
                }

            connection.close()

        }catch {
            case e: Exception => e.printStackTrace()
        }
        finally {
            println("Press Enter to close DB Connection")
        }

    }
    verifyLogin()
}
