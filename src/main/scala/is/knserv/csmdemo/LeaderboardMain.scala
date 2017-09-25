package is.knserv.csmdemo

//Spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//Scala
import org.rogach.scallop._
//Java
import java.util.Calendar
import com.github.lalyos.jfiglet.FigletFont

case class LeaderboardMainConf(val playerDataInputPath: String,
                               val gameDataInputPath: String,
                               val startTime: Long =
                                 Calendar.getInstance.getTimeInMillis)

class LeaderboardMainCmdParse(args: Array[String]) extends ScallopConf(args) {
  val playerDataInputPath =
    opt[String]("player-data", descr = "Path to player data")
  val gameDataInputPath = opt[String]("game-data", descr = "Path to game data")
  val outputPath        = opt[String]("output-path", descr = "Output path")
  verify()
}

object Functions {
  val RECENT_THRESHOLD_MILLIS = 86400000L // 1 day

  val pisMapping = Map[String, (Int, String, String)](
    "AchievementLikedEvent"     -> (1, "payload.achievement_owner_id", "payload.follower_id"),
    "AchievementCommentedEvent" -> (2, "payload.achievement_owner_id", "payload.follower_id"),
    "AchievementSharedEvent"    -> (2, "payload.follower_id", "payload.achievement_owner_id"),
    "ProfileVisitedEvent"       -> (5, "payload.visitor_profile_id", "payload.user_id"),
    "DirectMessageSentEvent"    -> (3, "payload.target_profile_id", "payload.source_profile_id")
  )
  
  val pglMapping = Map[String, (Int, String, String)](
    "GameRoundStarted" -> (1, "payload.user_id", "payload.game_id"),
    "GameRoundEnded"   -> (1, "payload.user_id", "payload.game_id"),
    "GameRoundDropped" -> (-20, "payload.user_id", "payload.game_id"),
    "GameWinEvent"     -> (3, "payload.user_id", "payload.game_id"),
    "MajorWinEvent"    -> (7, "payload.user_id", "payload.game_id")
  )

def mappingScoreUDF (mapping: Map[String, (Int, String, String)]) =
    udf(
      (eventType: String) =>
        mapping
          .get(eventType)
          .map(_._1)
          .getOrElse(0))

  def pisMappingScoreUDF = mappingScoreUDF(pisMapping)
  def pglMappingScoreUDF = mappingScoreUDF(pglMapping)

  private def fromMappingGetField( mapping: Map[String, (Int, String, String)], fieldNo: Int) = coalesce(
      // Warning: only safe for these two mappings and for these two fields!
      mapping.map(k => when(col("event_type") === k._1, col(k._2.productElement(fieldNo).toString)).otherwise(lit(null))).toSeq: _*
    )
  def pisMappingPField = fromMappingGetField(pisMapping, 1)
  def pisMappingIField = fromMappingGetField(pisMapping, 2)

  def pglMappingIorPField = fromMappingGetField(pglMapping, 1)
  def pglMappingGField = fromMappingGetField(pglMapping, 2)

  def pisModifyRecent(now: Long) = when(lit(now) - (unix_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss") * 1000) <= RECENT_THRESHOLD_MILLIS, col("score") * 1.5).otherwise(col("score"))

}

case class CalculateInfluenceLeadebBoard(val spark: SparkSession, val conf: LeaderboardMainConf) {


  def coughOutResults = {
    val playerDataInputPath = conf.playerDataInputPath
    val gameDataInputPath   = conf.gameDataInputPath
    val startTime           = conf.startTime
    val playerDataDf        = spark.read.format("json").load(playerDataInputPath)

    import Functions._
    val pis = playerDataDf
      .select(pisMappingScoreUDF(col("event_type")).alias("score"), pisMappingPField.alias("p"), pisMappingIField.alias("i"), col("event_time")).withColumn("score_recent", pisModifyRecent(startTime))
      .groupBy(col("i"), col("p")).agg(sum(col("score_recent")).alias("score_total"))
    val gameDataDf = spark.read.format("json").load(gameDataInputPath)
    val pgl = gameDataDf
      .select(pglMappingScoreUDF(col("event_type")).alias("score"), pglMappingIorPField.alias("iorp"), pglMappingGField.alias("g"))
      .where(col("score").notEqual(0))
      .groupBy(col("iorp"), col("g")).agg(sum(col("score")).alias("score_total"))
    val sumscoreDF = pis.join(pgl, pis("i") === pgl("iorp")).withColumn("score_mult", pis("score_total") * pgl("score_total"))
      .groupBy(col("p")).agg(sum(col("score_mult")).alias("sumscore"))
    sumscoreDF.join(pgl, sumscoreDF("p") === pgl("iorp")).withColumn("score_final", when(pgl("score_total") > 0, col("score_total") * col("sumscore")).otherwise(col("sumscore")))
  }
}

object LeaderboardMain {
  val APP_NAME = "influenza"
  def sparkConfig = new SparkConf().setAppName(APP_NAME)

  def main(args: Array[String]): Unit = {
    println("Starting..")
    println(FigletFont.convertOneLine(s"Influence Leaderboard (${APP_NAME})"))
    val argsParse = new LeaderboardMainCmdParse(args)
    val outputPath          = argsParse.outputPath
    val appConf =
      LeaderboardMainConf(playerDataInputPath = argsParse.playerDataInputPath(),
                          gameDataInputPath = argsParse.gameDataInputPath())
    val ss               = SparkSession.builder.config(sparkConfig).getOrCreate()
    val influenza = CalculateInfluenceLeadebBoard(ss, appConf)
    influenza.coughOutResults

  }
}
