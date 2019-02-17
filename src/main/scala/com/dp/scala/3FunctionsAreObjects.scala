
object Timer {
  def oncePerSecond(callback: () => Unit) {
    while (true) {
      callback(); Thread sleep 1000
    } // 1-arg method sleep used as operator
  }
  def welcome() {
    println("Welcome to Design Pathashala!")
  }
  def main(args: Array[String]) {
    oncePerSecond(welcome)
  }
}

