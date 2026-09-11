ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Day3-Spark-Setup",
    Compile / run / fork := true,
    Compile / run / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.6",
      "org.apache.spark" %% "spark-sql" % "3.5.6"
    )
  )