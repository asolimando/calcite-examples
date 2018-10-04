name := "CalciteSpark"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "Maven Central Server" at "http://repo1.maven.org/maven2"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.apache.calcite.avatica" % "avatica" % "1.11.0",
  "org.apache.calcite" % "calcite-core" % "1.17.0",
  //  "org.apache.calcite" % "calcite-spark" % "1.17.0",

  "org.postgresql" % "postgresql" % "42.2.5",

  "org.xerial" % "sqlite-jdbc" % "3.25.2"
)
    
