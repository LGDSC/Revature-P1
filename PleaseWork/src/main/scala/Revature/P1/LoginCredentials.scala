package Revature.P1
import scala.io.StdIn.readLine

object LoginCredentials extends App {
  def loginService(): Unit ={
    print("Please enter  [Admin/A or /User/U]")
    val authType= readLine();
    print("Please enter your login: ")
    val userid = readLine();
    print("Please enter your password")
    val password = readLine();
    if (authType == "Admin") {
      adminmenu()
    }
    else if (authType == "User")
    {
      usermenu()
    }
    else
      print("you must choose Choose Login Type")
      loginType()
  }
  def loginType(){
    println("How do you want to login")
    print("Please enter  [Admin/A or /User/U]")
    val authType= readLine();
    if (authType == "Admin") {
      print("Entering Admin 1menu")
      adminmenu()
    }
    else if (authType == "User") {
      println("Entering ")
      usermenu()
    }
    else
        println("Please select the user type")
  }
  def adminmenu() {
    // Need to code later.

  }
  def usermenu(){
      // Need to code later.
  }

}
