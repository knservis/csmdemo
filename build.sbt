val sparkVersion     = "2.1.1"
val scallopVersion   = "2.0.3"
val jfigletVersion   = "0.0.7"
val scalaTestVersion = "3.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "influenza",
    version := "0.0.1",
    description := "Influence leaderboard spark batch job",
    scalaVersion := "2.11.11"
  )
  .settings(
    libraryDependencies ++= Seq(
      // Scala
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.rogach" %% "scallop" % scallopVersion,
      // Java
      "com.github.lalyos" % "jfiglet" % jfigletVersion, // Nice ascii-art messages
      // Test
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    )
  )
  .settings(scalafmtConfig in ThisBuild := Some(file(".scalafmt.conf")))
  .settings(
    Keys.compile <<= (Keys.compile in Compile) dependsOn (scalafmt in Compile))
  .settings(
    (Keys.compile in Test) <<= (Keys.compile in Test) dependsOn (scalafmt in Test))
  .settings(
    javaOptions in (Test, console) ++= Seq("-Xms512M",
                                           "-Xmx2048M",
                                           "-XX:MaxPermSize=2048M",
                                           "-XX:+CMSClassUnloadingEnabled"),
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
    fork in Test := true)
