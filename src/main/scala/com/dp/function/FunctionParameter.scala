package com.dp.function

/**
 * @author miraj
 */
class FunctionParameter {
  /**
   * Sum all numbers 
   */
  def total(list: List[Int]) = {
    var sum = 0
    list.foreach {x => sum += x}
    sum
  }
  println(total(List(1,2,3,4,5)))
  
  /**
   * Sum even values
   */
  def totalEven(list: List[Int]) = {
    var sum = 0
    list.foreach {x => 
      if(x % 2 == 0) sum += x}
    sum
  }
  println(totalEven(List(1,2,3,4,5)))
  
  /**
   * Sum based on given selector
   */
  def totalSelectValue(list: List[Int], selector: Int => Boolean) = {
    var sum = 0
    list.foreach {x => 
      if(selector(x)) sum += x}
    sum
  }
  //sum all values
  println(totalSelectValue(List(1,2,3,4,5),{x => true}))
  
  //sum even values
  println(totalSelectValue(List(1,2,3,4,5),{x => x % 2 == 0}))
  
  //sum values greater than 4
  println(totalSelectValue(List(1,2,3,4,5),{_ > 4}))
  
  
}