package assignment1

import org.apache.spark.{SparkConf, SparkContext}

object Assignment1 extends App {

  val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Assignment-1").setMaster("local[*]"))
  sc.setLogLevel("ERROR")
  val file = sc.textFile("SampleData.txt")
  val words = file.flatMap(line => line.split(" "))

  val ans1 = words.map(word => (word, 1)).reduceByKey(_ + _).count

  val ans2 = words.reduce((word1, word2) =>
    if(word1.length > word2.length)
      word1
    else
      word2
  )

  val ans3 = words.map(word => (word, 1)).reduceByKey(_ + _).collect

  val ans4 = words.filter(word => word.length == 4 || word.length == 5 || word.length == 6)
      .map(word => (word, 1)).reduceByKey(_ + _).collect


  def isVowel(char: Char): Boolean = {
    val ch = char.toUpper
    if(ch == 'A' || ch == 'E' || ch == 'I' || ch == 'O' || ch == 'U')
      true
    else
      false
  }

  def ifContainsAtleastThreeVowels(word: String): Boolean = {
    val listOfChar = word.toList
    def innerFunc(listOfChar: List[Char], vowelsCount: Int): Boolean = {
      listOfChar match {
        case Nil if vowelsCount >= 3 => true
        case Nil => false
        case head :: tail if isVowel(head) => innerFunc(tail, vowelsCount + 1)
        case _ :: tail => innerFunc(tail, vowelsCount)
      }
    }
    innerFunc(listOfChar, 0)
  }

  val ans5 = words.filter(word => ifContainsAtleastThreeVowels(word)).collect

}
