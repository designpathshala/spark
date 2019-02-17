package com.dp.scala

object Basics {
  def main(args: Array[String]) {
    try {
      val x = 2 / 10
    } catch {
      case _: NumberFormatException => println("Use proper numbers")
      case ex: ArithmeticException  => println("Check your math...  " + ex)
    }

    var scores = Map("scala" -> 10, "Spark" -> 8, "Hadoop" -> 6)
    scores("scala")

    var scores1 = Map(("scala", 10), ("Spark", 8), ("Hadoop", 6))
    println(totalSeleVal(List(1, 2, 3, 4, 5), { e => true }))
    println(totalSeleVal(List(1, 2, 3, 4, 5), { e => e % 2 == 0 }))
    println(totalSeleVal(List(1, 2, 3, 4, 5), { _ > 4 }))

    val users: List[User] = User("joe", 22) :: User("Bob", 43) :: User("Kip", 56) :: Nil

    getAge("Bob", users)
  }
  
  def getAge(name: String, users: List[User]): Option[Int] = users match {
    case Nil                               => println("nil") ;None
    case User(n, age) :: tail if n == name => println("user, tail" + n) ;Some(age)
    case head :: tail                      => println("head, tail") ; getAge(name, tail)
  }

  def totalSeleVal(list: List[Int], selector: Int => Boolean) = {
    var sum = 0
    list foreach (e =>
      if (selector(e))
        sum += e)
    sum
  }

}

case class User(name: String, age: Int)

