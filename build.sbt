name := "slick-versioning"

version := "1.0.0"

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
  val slickV = "3.2.0-M1"

  Seq(
    "com.typesafe.slick" %% "slick" % slickV,
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "com.opentable.components" % "otj-pg-embedded" % "0.7.1" % "test",
    "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  )
}