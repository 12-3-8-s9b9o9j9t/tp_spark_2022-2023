case class Station(
  val id: String,
  val name: String,
  val latitude: Double,
  val longitude: Double) {

}

case class StationWithDist(
  val id: String,
  val name: String,
  val latitude: Double,
  val longitude: Double,
  val dist: Double) {

}

case class StationWithPath(
  val id: String,
  val name: String,
  val latitude: Double,
  val longitude: Double,
  val dist: Double,
  val path: List[String]) {

}
