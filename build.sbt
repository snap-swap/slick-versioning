name := "slick-versioning"

version := "1.0.5"

organization := "com.snapswap"

scalaVersion := "2.11.8"


scalacOptions := Seq(
  "-feature",
  "-unchecked",
  "-deprecation",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture",
  "-Ywarn-unused-import",
  "-encoding",
  "UTF-8"
)


libraryDependencies ++= {
  val slickV = "3.2.0"

  Seq(
    "com.typesafe.slick" %% "slick" % slickV,
    "ch.qos.logback" % "logback-classic" % "1.1.7" % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.opentable.components" % "otj-pg-embedded" % "0.7.1" % "test"
  )
}