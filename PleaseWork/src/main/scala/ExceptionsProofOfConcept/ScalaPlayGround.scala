package ExceptionsProofOfConcept

import org.apache.derby.iapi.error.PassThroughException

object ScalaPlayGround extends App {
  def getInt(throughException: Boolean):Int={
      if(throughException) throw new RuntimeException("Exception was thrown forcefully")
      26
  }
  val exception= try {
    getInt(false)
  }catch {
    case e: NullPointerException => println("Nothing will be printed")
    case e: RuntimeException => println(s"Exception caught with message = ${e.getMessage}")
  } finally {
    println("Anything in the finally block will always be executed")
  }
  println(exception)
}
