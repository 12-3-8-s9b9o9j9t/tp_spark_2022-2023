import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.Console.println

object Main {

  private val helper = new HelpfulFunctions()

  private def distToStation(id: VertexId, vd: Station, to: VertexId): StationWithDist = {
    if (id == to) {
      StationWithDist(vd.id, vd.name, vd.latitude, vd.longitude, 0)
    } else {
      StationWithDist(vd.id, vd.name, vd.latitude, vd.longitude, Double.PositiveInfinity)
    }
  }

  private def pathToStation(id: VertexId, vd: Station, to: VertexId): StationWithPath = {
    if (id == to) {
      StationWithPath(vd.id, vd.name, vd.latitude, vd.longitude, 0, List(vd.name))
    } else {
      StationWithPath(vd.id, vd.name, vd.latitude, vd.longitude, Double.PositiveInfinity, List())
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("graphXTP").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    /*  Graph creation  */
    val data = sc.textFile("src/main/resources/JC-202112-citibike-tripdata.csv")
    val header = data.first()
    val rdd = data.filter(row => row != header) // Get the data without csv header
      .map(row => row.split(','))

    val trips = rdd.filter(row => !row(7).isBlank) // Remove trips with unknown end station
      .map(row => {
        Trip(row(0), row(5), row(7), helper.timeToLong(row(2)), helper.timeToLong(row(3)), row(1), row(12))
      }) // -> RDD(Trip)

    val start_stations = rdd.map(row => (row(5), row(4), row(8), row(9)))
      .distinct() // -> RDD(id, name, latitude, longitude)
    val end_stations = rdd.filter(row => !row(7).isBlank)
      .map(row => {
        (row(7), row(6), row(10), row(11))
      }).distinct() // -> RDD(id, name, latitude, longitude)

    val stations = start_stations.union(end_stations)
      .distinct()
      // Next two lines are to remove stations with same name but different coordinates
      .map(row => (row._1, row)) // -> RDD(id, (id, name, latitude, longitude))
      .reduceByKey((x, _) => x) // -> RDD(id, (id, name, latitude, longitude))
      .map {
        case (_, (id, name, lat, lng)) => Station(id, name, lat.toDouble, lng.toDouble)
      } // -> RDD(Station)

    /*  Création du graphe  */
    // Create a mapping from String to VertexId for stations' id
    val idMapping = stations.zipWithIndex()
      .map {
        case (station, idx) => (station.id, idx)
      }.collectAsMap()

    val stationsRDD = stations.map(station => {
        val vertexId: VertexId = idMapping(station.id)
        (vertexId, station)
      }) // -> RDD(vertex id, Station)

    val tripsRDD: RDD[Edge[Trip]] = trips.map(trip => {
        val srcId: VertexId = idMapping(trip.start_station)
        val dstId: VertexId = idMapping(trip.end_station)
        Edge(srcId, dstId, trip)
      }) // -> RDD(start station vertex id, end station vertex id, Trip)

    val graph = Graph[Station, Trip](stationsRDD, tripsRDD)

    /*  Extraction de sous-graphe  */

    val start_date = helper.timeToLong("2021-12-05 00:00:00")
    val end_date = helper.timeToLong("2021-12-25 23:59:59")
    val interval = graph.subgraph(e => e.attr.start_time >= start_date && e.attr.end_time <= end_date)

    /* Calcul de degré */
    val count_start_end = interval.aggregateMessages[(Int, Int)](
        ctx => {
          ctx.sendToSrc((1, 0))
          ctx.sendToDst((0, 1))
        }, // Send message to count how many times a station is used as a start and how many times as an end
        (a, b) => (a._1 + b._1, a._2 + b._2)
      ) // -> RDD(vertex id, (start count, end count))
      .join(interval.vertices) // -> RDD(vertex id, ((start count, end count), Station))
      .map {
        case (_, (count, station)) => (station, count._1, count._2)
      } // -> RDD(Station, start count, end count)

    println("Stations the most left:")
    count_start_end.sortBy(_._2, ascending=false) // Sort by start count, most used as start first
      .take(10)
      .map{case (station, start, _) => (station.name, start)} // -> RDD(station name, start count)
      .foreach(println)
    println()

    println("Stations the most arrived:")
    count_start_end.sortBy(_._3, ascending=false) // Sort by end count, most used as end first
      .take(10)
      .map{case (station, _, end) => (station.name, end)} // -> RDD(station name, end count)
      .foreach(println)
    println()

    /* Proximité entre les stations */
    val jc013_id = idMapping("JC013")
    val nearest_dist_time = graph.subgraph(e => {
        e.srcId == jc013_id ^ e.dstId == jc013_id // Keep only edges that have JC013 as either source or destination
      }).aggregateMessages[(Double, Long)](
        ctx => {
          val dist = helper.getDistKilometers(ctx.srcAttr.longitude, ctx.srcAttr.latitude, ctx.dstAttr.longitude, ctx.dstAttr.latitude)
          val time = ctx.attr.end_time - ctx.attr.start_time
          ctx.sendToSrc((dist, time))
          ctx.sendToDst((dist, time))
        }, // Send message to compute distance and time between JC013 and other stations
        (a, b) => (math.min(a._1, b._1), math.min(a._2, b._2))
      ) // -> RDD(vertex id, (distance, time))
      .join(graph.vertices) // -> RDD(vertex id, ((distance, time), Station))
      .filter{case (id, _) => id != jc013_id} // Remove JC013 from the list
      .map{case (_, ((dist, time), station)) => (station, dist, time)} // -> RDD(Station, distance, time)

    println("Nearest stations to JC013:")
    val nearest_dist = nearest_dist_time.sortBy(_._2, ascending=true) // Sort by distance, nearest first
      .take(1) // Keep only the nearest station
      .map{case (station, dist, _) => (station.name, dist)} // -> RDD(station name, distance)
      .apply(0) // -> (station name, distance)
    println(String.format(" -by distance: %s with a distance of %.3f km", nearest_dist._1, nearest_dist._2))

    val nearest_time = nearest_dist_time.sortBy(_._3, ascending=true) // Sort by time, nearest first
      .take(1) // Keep only the nearest station
      .map{case (station, _, time) => (station.name, time)} // -> RDD(station name, time)
      .apply(0) // -> (station name, time)
    println(String.format(" -by time: %s with a time of %s", nearest_time._1, helper.milisToString(nearest_time._2)))
    println()

    /* Plus court chemin */

    // find stations nearest to JC013
    println("Nearest stations to JC013 (by distance):")
    Pregel[StationWithDist, Trip, Double](
      graph.mapVertices((id, vd) => distToStation(id, vd, to=jc013_id)), // Initialize the graph with the distance to JC013
      initialMsg=Double.PositiveInfinity,
      maxIterations=3,
      activeDirection = EdgeDirection.Out) (
      (_, vd, a) => StationWithDist(vd.id, vd.name, vd.latitude, vd.longitude, math.min(a, vd.dist)),
      et => {
        val src = et.srcAttr
        val dst = et.dstAttr
        Iterator((et.dstId, src.dist+helper.getDistKilometers(src.longitude, src.latitude, dst.longitude, dst.latitude)))
      },
      (a, b) => math.min(a,b) // Keep the minimum distance
    ) // -> Graph(StationWithDist, Trip)
      .vertices // -> RDD(vertex id, StationWithDist)
      .filter {case (_, vd) => vd.dist != Double.PositiveInfinity} // Keep only the stations that are reachable
      .sortBy {case (_, vd) => vd.dist} // Sort by distance, nearest first
      .take(11)
      .filter {case (id, _) => id != jc013_id} // Remove JC013 from the list
      .map {case (_, vd) => (vd.name, vd.dist)} // -> RDD(station name, distance)
      .foreach(println)
    println()

    println("Nearest stations to JC013 with path (by distance):")
    Pregel[StationWithPath, Trip, (Double, List[String])](
      graph.mapVertices((id, vd) => pathToStation(id, vd, to=jc013_id)),
      initialMsg = (Double.PositiveInfinity, List[String]()),
      maxIterations = 3,
      activeDirection = EdgeDirection.Out) (
      (_, vd, a) => {
        val (dist, path) = a
        if (dist < vd.dist) {
          StationWithPath(vd.id, vd.name, vd.latitude, vd.longitude, dist, path)
        } else {
          vd
        }
      },
      et => {
        val src = et.srcAttr
        val dst = et.dstAttr
        Iterator((et.dstId, (src.dist+helper.getDistKilometers(src.longitude, src.latitude, dst.longitude, dst.latitude), dst.name+:src.path)))
      },
      (a, b) => {
        if (a._1 < b._1) a else b // Keep the path with the minimum distance
      }
    ) // -> Graph(StationWithPath, Trip)
      .vertices // -> RDD(vertex id, StationWithPath)
      .filter {case (_, vd) => vd.dist != Double.PositiveInfinity} // Keep only the stations that are reachable
      .sortBy {case (_, vd) => vd.dist} // Sort by distance, nearest first
      .take(11)
      .filter {case (id, _) => id != jc013_id} // Remove JC013 from the list
      .map {case (_, vd) => (vd.name, vd.dist, vd.path.mkString(" -> "))} // -> RDD(station name, distance, path)
      .foreach(println)
  }
}
