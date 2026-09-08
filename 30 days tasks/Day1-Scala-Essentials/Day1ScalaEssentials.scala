import scala.collection.immutable.{List, Vector, Set, Map}

// ============================================================
// Day 1 - Scala Essentials
// ============================================================

object Day1ScalaEssentials {

  // ------------------------------------------------------------
  // 1. val, var, lazy val and immutable collections
  // ------------------------------------------------------------

  def variablesAndCollections(): Unit = {
    println("=== 1. val, var, lazy val and Immutable Collections ===")

    val studentName = "Harsha"
    var marks = 75

    lazy val finalMessage = {
      println("lazy val is evaluated now")
      s"$studentName scored $marks marks"
    }

    println(s"Student: $studentName")
    println(s"Initial marks: $marks")

    marks = 85
    println(s"Updated marks: $marks")

    println(finalMessage)

    val subjects = List("Scala", "Spark", "Hadoop")
    val updatedSubjects = subjects :+ "Kafka"

    println(s"Original List: $subjects")
    println(s"Updated List: $updatedSubjects")
  }


  // ------------------------------------------------------------
  // 2. For-comprehension using yield
  // ------------------------------------------------------------

  def forComprehensionExample(): Unit = {
    println("\n=== 2. For-Comprehension with yield ===")

    val students = List("Harsha", "Rahul", "Priya", "Anjali")
    val marks = List(85, 72, 91, 64)

    val studentMarks = for {
      student <- students
      mark <- marks
    } yield s"$student scored $mark"

    println("Student-Mark combinations:")
    studentMarks.foreach(println)
  }


  // ------------------------------------------------------------
  // 3. List, Vector, Set and Map
  // ------------------------------------------------------------

  def collectionComparison(): Unit = {
    println("\n=== 3. List, Vector, Set and Map ===")

    val studentList = List("Harsha", "Rahul", "Priya")
    val studentVector = Vector("Harsha", "Rahul", "Priya")
    val studentSet = Set("Harsha", "Rahul", "Priya", "Harsha")
    val studentMap = Map(
      "Harsha" -> 85,
      "Rahul" -> 72,
      "Priya" -> 91
    )

    println(s"List  : $studentList")
    println(s"Vector: $studentVector")
    println(s"Set   : $studentSet")
    println(s"Map   : $studentMap")
  }


  // ------------------------------------------------------------
  // 4. Logger trait implemented by two classes
  // ------------------------------------------------------------

  trait Logger {
    def log(message: String): Unit
  }

  class ConsoleLogger extends Logger {
    override def log(message: String): Unit = {
      println(s"[Console] $message")
    }
  }

  class FileLogger extends Logger {
    override def log(message: String): Unit = {
      println(s"[File] $message")
    }
  }


  // ------------------------------------------------------------
  // 5. Student Grade Processor using Scala Collections
  // ------------------------------------------------------------

  case class Student(name: String, marks: Int)

  def calculateGrade(marks: Int): String = {
    marks match {
      case m if m >= 90 => "A"
      case m if m >= 80 => "B"
      case m if m >= 70 => "C"
      case m if m >= 60 => "D"
      case _            => "F"
    }
  }

  def processStudents(): Unit = {
    println("\n=== 5. Student Grade Processor ===")

    val students = List(
      Student("Harsha", 85),
      Student("Rahul", 72),
      Student("Priya", 91),
      Student("Anjali", 64),
      Student("Kiran", 55)
    )

    val results = students.map { student =>
      val grade = calculateGrade(student.marks)
      (student.name, student.marks, grade)
    }

    println("Student Results:")
    results.foreach {
      case (name, marks, grade) =>
        println(s"$name -> Marks: $marks, Grade: $grade")
    }

    val passedStudents = students
      .filter(_.marks >= 60)
      .map(_.name)

    println(s"\nPassed Students: $passedStudents")

    val averageMarks =
      students.map(_.marks).sum.toDouble / students.size

    println(f"Average Marks: $averageMarks%.2f")
  }


  // ------------------------------------------------------------
  // Main method
  // ------------------------------------------------------------

  def main(args: Array[String]): Unit = {

    variablesAndCollections()

    forComprehensionExample()

    collectionComparison()

    println("\n=== 4. Logger Trait ===")

    val consoleLogger = new ConsoleLogger
    val fileLogger = new FileLogger

    consoleLogger.log("Console logger is working")
    fileLogger.log("File logger implementation is working")

    processStudents()

    println("\n=== Day 1 Completed Successfully ===")
  }
}
