import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

import java.io.File
import javax.imageio.ImageIO
import scala.math.BigDecimal.double2bigDecimal


object Main {

  private sealed trait VertexType
  private case class Player(name: String) extends VertexType
  private case class Game(id: String, map: String) extends VertexType

  private sealed trait EdgeType
  private case class Ranked(rank: Int) extends EdgeType
  private case class Killed(weapon: String, time: Int, killer_pos: (Double, Double),
                            victim_pos: (Double, Double), map: String) extends EdgeType

  private sealed trait WeaponRange
  private case object Melee extends WeaponRange
  private case object Close extends WeaponRange
  private case object Mid extends WeaponRange
  private case object Far extends WeaponRange

  private def weaponRangeFromDist(dist: Double): WeaponRange = dist match {
    case d if d < 1.5 => Melee
    case d if d < 10 => Close
    case d if d < 50 => Mid
    case _ => Far
  }

  private val RANGE_ORDER = Map[WeaponRange, Int](Melee -> 0, Close -> 1, Mid -> 2, Far -> 3)
  private implicit val weaponRangeOrdering: Ordering[WeaponRange] = Ordering.by(RANGE_ORDER)

  private val MAP_SIZE = 800000.0 // in cm
  private val ERANGEL_SCALE = MAP_SIZE / 812500.0
  private val MIRAMAR_SCALE = MAP_SIZE / 819200.0

  private val NaW = Set("Bluezone", "RedZone", "Down and Out", "Drown", "Falling")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("project").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val path = "src/main/resources/kill_match_stats_final_0_head.csv"

    val data = sc.textFile(path)
    val header = data.first()
    val rdd = data.filter(row => row != header)
      .map(row => row.split(','))

    val killers = rdd.filter(row => !row(1).isBlank)
      .map(row => Player(row(1)))
      .distinct()
    val victims = rdd.map(row => Player(row(8)))
      .distinct()

    val players: RDD[(VertexId, VertexType)] = killers.union(victims)
      .distinct()
      .map(
        p => (p.name.hashCode.toLong, p)
      )

    val games: RDD[(VertexId, VertexType)] = rdd.map(row => Game(row(6), row(5)))
      .distinct()
      .map(
        g => (g.id.hashCode.toLong, g)
      )

    val killers_ranked = rdd.filter(row => !(row(1).isBlank || row(1) == "#unknown"))
      .map(row => ((row(1), row(6)), row(2)))
      .distinct()
    val victims_ranked = rdd.filter(row => row(8) != "#unknown")
      .map(row => ((row(8), row(6)), row(9)))
      .distinct()

    val ranked: RDD[Edge[EdgeType]] = killers_ranked.union(victims_ranked)
      .distinct()
      .reduceByKey((x, y) => if (x.isBlank) y else x)
      .filter {
        case ((_, _), rank) => !rank.isBlank
      }.map {
        case ((player, game), rank) => Edge(
          player.hashCode.toLong,
          game.hashCode.toLong,
          Ranked(rank.toDouble.toInt)
        )
      }

    val killed: RDD[Edge[EdgeType]] = rdd.filter(row => !(row(1).isBlank || NaW.contains(row(0))))
      .map(row => {
        val scale = row(5) match {
          case "ERANGEL" => ERANGEL_SCALE
          case "MIRAMAR" => MIRAMAR_SCALE
          case _ => 1
        }
        Edge(row(1).hashCode.toLong,
          row(8).hashCode.toLong,
          Killed(row(0), row(7).toInt,
            (scale * row(3).toDouble, scale * row(4).toDouble),
            (scale * row(10).toDouble, scale * row(11).toDouble), row(5)))
      })

    val graph: Graph[VertexType, EdgeType] = Graph(players.union(games), ranked.union(killed))

    val player_rank_weapon = graph.aggregateMessages[(Int, Int, List[String])](
        ctx => {
          ctx.sendToSrc(ctx.attr match {
            case Ranked(rank) => (rank, 1, Nil)
            case Killed(weapon, _, _, _, _) => (0, 0, List(weapon))
          })
        },
        (a, b) => (a._1 + b._1, a._2 + b._2, a._3 ++ b._3)
      ).map {
        case (id, (rank_sum, nb_game, weapons)) => (id, (rank_sum.toDouble / nb_game, weapons))
      }.join(players)
      .filter {
        case (_, (_, Player(name))) => name != "#unknown"
      }.map {
        case (_, ((rank, weapons), Player(name))) => (name, rank, weapons)
      }

    def printRankWeapon(tuple: (String, Double, List[String])): Unit = {
      tuple match {
        case (name, avg_rank, weapons) => val weapons_str = weapons match {
          case Nil => "does not kill anyone 😇💩"
          case ws => "uses " + ws.distinct.mkString(", ")
        }
          println(s"$name has an average rank of $avg_rank and $weapons_str")
      }
    }

    player_rank_weapon.sortBy(_._2)
      .take(10)
      .foreach(printRankWeapon)
    println()

    player_rank_weapon.sortBy(_._2, ascending = false)
      .take(10)
      .foreach(printRankWeapon)
    println()

    val kill_known_victim_location = killed.filter{
      case Edge(_, _, Killed(_, _, _, (x, y), map)) => !(x == 0 && y == 0) && !map.isBlank
    }.map {
      case Edge(_, _, k: Killed) => k
    }

    val SQRT_SUBDIV = 16

    val kill_zones = kill_known_victim_location.map {
        case Killed(_, _, _, (x, y), map) =>
          ((map, (SQRT_SUBDIV * x / MAP_SIZE).toInt, (SQRT_SUBDIV * y / MAP_SIZE).toInt), 1)
      }.reduceByKey(_ + _)
      .map {
        case ((map, x, y), count) => (map, (x, y, count))
      }.groupByKey()
      .map {
        case (map, counts) =>
          val zones = Array.fill(SQRT_SUBDIV, SQRT_SUBDIV)(0)
          counts.foreach {
            case (x, y, count) => zones(x)(y) = count
          }
          (map, zones)
      }

    val NB_ALPHA = 10
    val alpha_range = 0.0 to 1.0 by 1.0 / (NB_ALPHA - 1)

    def generate_visualization(map: String, zones: Array[Array[Int]]): Unit = {
      val img_name = map.toLowerCase
      val image = ImageIO.read(new File(s"src/main/resources/$img_name.jpg"))
      val (w, h) = (image.getWidth, image.getHeight)
      val max_kill = zones.flatten.max
      val xx = 0 to w by w / SQRT_SUBDIV
      val yy = 0 to h by h / SQRT_SUBDIV

      for (n <- 1 until SQRT_SUBDIV; m <- 1 until SQRT_SUBDIV) {
        val (i, j) = (SQRT_SUBDIV * xx(n - 1) / w, SQRT_SUBDIV * yy(m - 1) / h)
        val alpha = alpha_range((NB_ALPHA - 1) * zones(i)(j) / max_kill)

        val xStart = xx(n - 1)
        val yStart = yy(m - 1)
        val xEnd = xx(n) - 1
        val yEnd = yy(m) - 1

        val width = xEnd - xStart
        val height = yEnd - yStart

        val old_zone = image.getRGB(xStart, yStart, width, height, null, 0, width)

        val new_zone = old_zone.map(
          old_color =>
            ((255 * 0.9 * alpha + (1 - alpha) * ((old_color >> 16) & 255)).toInt << 16) |
              (((old_color >> 8) & 255) << 8) |
              (old_color & 255)
        )
        image.setRGB(xStart, yStart, width, height, new_zone, 0, width)
      }
      ImageIO.write(image, "jpg", new File(s"src/main/resources/${img_name}_deadly.jpg"))
    }

    kill_zones.foreach {
      case (map, zones) => generate_visualization(map, zones)
    }

    val ranges = kill_known_victim_location.filter {
      case Killed(_, _, (x, y), _, _) => !(x == 0 && y == 0)
    }.map {
      case Killed(weapon, _, (kx, ky), (vx, vy), _) =>
        val dist = Math.sqrt(Math.pow(vx - kx, 2) + Math.pow(vy - ky, 2)) / 100
        val range = weaponRangeFromDist(dist)
        (weapon, range)
    }.groupByKey()
      .map {
        case (weapon, ranges) =>
          val total = ranges.size
          val count = ranges.groupMapReduce(identity)(_ => 1)(_ + _)
          val percentage = count.toSeq
            .map {
            case (range, nb) => (range, nb.toDouble * 100 / total)
          }.sorted
          (weapon, percentage)
      }

    ranges.foreach {
      case (weapon, ranges) =>
        val range_str = ranges.map {
          case (range, percentage) => s"$range ${BigDecimal(percentage).setScale(2, BigDecimal.RoundingMode.HALF_UP)}%"
        }.mkString(", ")
        println(s"$weapon has a range of $range_str")
    }

  }

}
