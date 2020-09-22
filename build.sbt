name := "CatsMapReduce"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.1" withSources() withJavadoc())

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "co.fs2" %% "fs2-core" % "2.4.0"
libraryDependencies += "co.fs2" %% "fs2-io" % "2.4.0"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds")