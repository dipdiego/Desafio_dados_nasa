name := "ParserApp"

version := "1.1.0"

scalaVersion := "2.10.5"

lazy val root = (project in file("."))
  .settings(
    mainClass in Compile := Some("br.net.oi.DfToJson"),

    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % "2.6",
      "org.apache.spark" %% "spark-core" % "1.6.0",
      "org.apache.spark" %% "spark-sql" % "1.6.0",
      "org.apache.spark" %% "spark-hive" % "1.6.0"
  )
)