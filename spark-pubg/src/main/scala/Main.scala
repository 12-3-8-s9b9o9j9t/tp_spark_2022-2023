import org.apache.spark.{SparkConf, SparkContext}

object Main {

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
    val rdd = data.filter(row => row != header)
      .map(row => row.split(','))

    val name_kill = rdd.map(row => (row(11), row(10).toInt))

    // print first 10 rows
    println("Player name and number of kills:")
    name_kill.take(10)
      .foreach(println)
    println()

    val avg_kill = name_kill.map(row => (row._1, (row._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(row => (row._1, row._2._1.toDouble / row._2._2.toDouble, row._2._2))

    // print first 10 rows
    println("Player name, average number of kills and number of games played:")
    avg_kill.take(10)
      .foreach(println)
    println()

    // print first 10 rows
    val best_players = avg_kill.sortBy(_._2, ascending = false)

    println("Best players:")
    best_players
      .take(10)
      .foreach(println)
    println()

    val played_4games = best_players.filter(row => row._3 >= 4)

    // print first 10 rows
    println("Players who played at least 4 games:")
    played_4games.take(10)
      .foreach(println)
    println()

    println("Filtered players without name:")
    played_4games.filter(!_._1.isBlank)
      .take(10)
      .foreach(println)
    println()

    println("Player name and score:")
    rdd.map(row => (row(11), score(row(5).toInt, row(9).toDouble, row(10).toInt, row(14).toInt)))
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach(println)

  }

}
