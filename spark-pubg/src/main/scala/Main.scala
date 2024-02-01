import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn.readLine
object Main {

  private def score(assist: Int, damage: Double, kill: Int, placement: Int): Double = {
    val score = assist * 50 + damage.toInt + kill * 100 + (101 - placement) * 10
    score
  } : Int
  def main(args: Array[String]): Unit = {

    // Create a SparkConf and SparkContext
    val conf = new SparkConf().setAppName("pubg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Get the Path to the CSV file
    val path = args.length > 0 match {
      case true => args(0)
      case false =>
        println("Please enter the path to the CSV file:")
        readLine()
    }

    val data = sc.textFile(path)
    val header = data.first()
    val rdd = data.filter(row => row != header).map(row => row.split(','))

    val name_kill = rdd.map(row => (row(11), row(10).toInt))

    // print first 10 rows
    println("Player name and number of kills:")
    name_kill.take(10).foreach(println)
    println()

    val avg_kill = name_kill.map(row => (row._1, (row._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(row => (row._1, row._2._1.toDouble / row._2._2.toDouble, row._2._2))

    // print first 10 rows
    println("Player name, average number of kills and number of games played:")
    avg_kill.take(10).foreach(println)
    println()

    val best_players = avg_kill.sortBy(_._2, ascending = false).take(10)

    // print first 10 rows
    println("Best players:")
    best_players.foreach(println)
    println()

    val played_4games = avg_kill.filter(row => row._3 >= 4)

    // print first 10 rows
    println("Players who played at least 4 games:")
    played_4games.sortBy(_._2, ascending = false).take(10).foreach(println)
    println()

    val no_blank = played_4games.filter(!_._1.isBlank)

    // print first 10 rows
    println("Filtered players without name:")
    no_blank.sortBy(_._2, ascending = false).take(10).foreach(println)
    println()

    val player_score = rdd.map(row => (row(11), score(row(5).toInt, row(9).toDouble, row(10).toInt, row(14).toInt)))

    // print first 10 rows
    println("Player name and score:")
    player_score.sortBy(_._2, ascending = false).take(10).foreach(println)

  }

}
