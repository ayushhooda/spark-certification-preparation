package explode

object Data {

  case class Person(name: String, knownLanguages: List[String] = List(), properties: Map[String, String] = Map())

  val person1 = Person("Person-1", List("Scala", "Java"), Map("hair" -> "black", "eye" -> "brown"))
  val person2 = Person("Person-2", List("C#", "PHP", null), null)

  val data: Seq[Person] = Seq(person1, person2)

}
