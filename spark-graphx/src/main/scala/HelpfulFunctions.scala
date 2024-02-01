class HelpfulFunctions {

  def timeToLong(tString: String): Long = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(tString).getTime
  }

  def getDistKilometers(lngDegreeA : Double, latDegreeA : Double, lngDegreeB : Double, latDegreeB: Double) : Double = {

    val longRadiansA = Math.toRadians(lngDegreeA)
    val latRadiansA = Math.toRadians(latDegreeA)
    val longRadiansB = Math.toRadians(lngDegreeB)
    val latRadiansB = Math.toRadians(latDegreeB)

    val deltaLon = longRadiansB - longRadiansA
    val deltaLat = latRadiansB - latRadiansA
    val a = Math.pow(Math.sin(deltaLat / 2), 2) +
      Math.cos(latRadiansA) *
        Math.cos(latRadiansB) *
        Math.pow(Math.sin(deltaLon / 2), 2)

    val c = 2 * Math.asin(Math.sqrt(a))

    val r = 6371 // Radius of earth in kilometers
    c*r
  }

  def milisToString(milis: Long): String = {
    val h = milis / 3600000
    val m = (milis - h * 3600000) / 60000
    val s = (milis - h * 3600000 - m * 60000) / 1000
    String.format("%02d:%02d:%02d", h, m, s)
  }

}
