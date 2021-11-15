package ExceptionsProofOfConcept

import java.io.IOException
object CFG2 {

  def main(args:Array[String]){

      try{
        var res = 5/0;
      }catch {
        case ex: ArithmeticException => {
          println("Arithmetic Exception Occured.")
        }

      } finally {
                println("This is a finally block, This will be executed no matterwhat the exception is raised or not")
      }

  }
}
