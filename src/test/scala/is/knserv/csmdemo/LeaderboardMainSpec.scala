package is.knserv.csmdemo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest._
import org.apache.log4j.{Level, Logger}

class LeaderboardMainSpec extends FlatSpec with Matchers {
  "LeaderboardMain" should "produce the expected output" in {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sconf = new SparkConf().setAppName("influenza")
    sconf.set("spark.master", "local[*]")
    val ss = SparkSession.builder.config(sconf).getOrCreate()
    val appConf =
      LeaderboardMainConf(playerDataInputPath =
                            "src/test/resources/player_data",
                          gameDataInputPath = "src/test/resources/game_data",
                          startTime = 1502323200000L // 2017-08-10 00:00:00.000
      )
    val influenza = CalculateInfluenceLeadebBoard(ss, appConf)

    val res = influenza.coughOutResults
      .select(col("p"), col("g"), col("score_final"))
      .rdd
      .map(r =>
        ((r.getAs[String]("p"), r.getAs[String]("g")),
         r.getAs[Float]("score_final")))
      .collectAsMap()

    val expectedResult = Map(
      ("player_C", "game_1") -> 2800.0,
      ("player_A", "game_1") -> 55500.0,
      ("player_B", "game_2") -> 22500.0,
      ("player_B", "game_1") -> 11250.0,
      ("player_B", "game_3") -> 22500.0
    )

    res should equal(expectedResult)
  }
}
