case class Station(
  id: String,
  name: String,
  latitude: Double,
  longitude: Double)
case class StationWithDist(
  id: String,
  name: String,
  latitude: Double,
  longitude: Double,
  dist: Double)
case class StationWithPath(
  id: String,
  name: String,
  latitude: Double,
  longitude: Double,
  dist: Double,
  path: List[String])
