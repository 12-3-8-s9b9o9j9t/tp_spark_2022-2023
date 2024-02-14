import org.apache.spark.{SparkConf, SparkContext}

object Main {

  /**
   * Computes a score for a player
   */
  private def score(assist: Int, damage: Double, kill: Int, placement: Int): Double = {
    val score = assist * 50 + damage.toInt + kill * 100 + (101 - placement) * 10
    score
  } : Int

  def main(args: Array[String]): Unit = {

    // Create a SparkConf and SparkContext
    val conf = new SparkConf().setAppName("pubg").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "src/main/resources/agg_match_stats_0_head.csv"
    val data = sc.textFile(path)
    val header = data.first()
    val rdd = data.filter(row => row != header) // Get data without csv header
      .map(row => row.split(','))

    val name_kill = rdd.map(row => (row(11), row(10).toInt)) // -> RDD(player name, number of kills)

    println("Player name and number of kills:")
    name_kill.take(10)
      .foreach(println)
    println()

    val avg_kill = name_kill.map{
        case (name, kill) => (name, (kill, 1))
      }// -> RDD(player name, (number of kills, 1))
      .reduceByKey{
        case ((kill1, games1), (kill2, games2)) => (kill1 + kill2, games1 + games2)
      } // -> RDD(player name, (sum of kills, number of games))
      .map {
        case (name, (kill, games)) => (name, kill.toDouble / games, games)
      } // -> RDD(player name, average number of kills, number of games)

    println("Player name, average number of kills and number of games played:")
    avg_kill.take(10)
      .foreach(println)
    println()

    // Sort by average number of kills, best first
    val best_players = avg_kill.sortBy(_._2, ascending = false) // -> RDD(player name, average number of kills, number of games)

    println("Best players:")
    best_players
      .take(10)
      .foreach(println)
    println()

    // Keep only players who played at least 4 games
    val played_4games = best_players.filter {
        case (_, _, games) => games >= 4
      } // -> RDD(player name, average number of kills, number of games)

    // print first 10 rows
    println("Players who played at least 4 games:")
    played_4games.take(10)
      .foreach(println)
    println()

    println("Filtered players without name:")
    played_4games.filter {
        case (name, _, _) => !name.isBlank
      } // -> RDD(player name, average number of kills, number of games)
      .take(10)
      .foreach(println)
    println()

    println("Player name and score:")
    rdd.map(row => (row(11), score(row(5).toInt, row(9).toDouble, row(10).toInt, row(14).toInt))) // -> RDD(player name, score)
      .sortBy(_._2, ascending = false) // sort by score, best first
      .take(10)
      .foreach(println)

  }

}
