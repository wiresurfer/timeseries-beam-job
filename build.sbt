import sbt._
import Keys._

val scioVersion = "0.9.5"
val beamVersion = "2.11.0"
val scalaMacrosVersion = "2.1.0"

val shapelessVersion = "2.3.3"
val datastoreVersion = "1.3.0"
val bigqueryVersion = "v2-rev20181104-1.27.0"
val tensorflowVersion = "1.14.0"



fork           := true // run program in forked JVM - so exiting it won't kill sbt
trapExit       := false // sbt don't prevent process from exiting
connectInput   := true // forked process still reads from std-in
outputStrategy := Some(StdoutOutput) // prevent output from prepending [info]

envVars := Map("GOOGLE_APPLICATION_CREDENTIALS" -> "/Users/manik/Downloads/dataglen-prod-196710-3ac597af4238.json")

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "atriaee",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.8",
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  
  addCompilerPlugin(paradiseDependency)
)

javaOptions ++= Seq("-Xdebug","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006")

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "beaamjob",
    description := "beaamjob",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-avro" % scioVersion,
      "com.spotify" %% "scio-bigquery" % scioVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.spotify" %% "scio-tensorflow" % scioVersion,
      "com.twitter" %% "algebird-core" % "0.1.11",
      "org.scalanlp" %% "breeze" % "0.13.2",
  
  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
      "org.scalanlp" %% "breeze-natives" % "0.13.2",
      
      "com.spotify" %% "scio-test" % scioVersion % Test,

      "com.fasterxml.jackson.core" % "jackson-databind" % "2.2.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.2.2",
      "org.postgresql" % "postgresql" % "42.2.6",

      "org.apache.beam" % "beam-sdks-java-io-kafka" % beamVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-mqtt" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-rabbitmq" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-elasticsearch" % beamVersion,
      "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion,

      "com.chuusai" %% "shapeless" % "2.3.3",
      "me.lyh" %% "shapeless-datatype-core" % "0.2.0",
      "me.lyh" %% "shapeless-datatype-bigquery" % "0.2.0",
      "me.lyh" %% "shapeless-datatype-tensorflow" % "0.2.0",
      "me.lyh" %% "shapeless-datatype-datastore" % "0.2.0",

      // optional dataflow runner
      // "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.25"
    )
  )
  .enablePlugins(PackPlugin)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for beaamjob",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true
  )
  .dependsOn(root)


enablePlugins(DockerPlugin)
dockerfile in docker := {
  val jarFile: File = sbt.Keys.`package`.in(Compile, packageBin).value
  val classpath = (managedClasspath in Compile).value
  val mainclass = mainClass.in(Compile, packageBin).value.getOrElse(sys.error("Expected exactly one main class"))
  val jarTarget = s"/app/${jarFile.getName}"
  // Make a colon separated classpath with the JAR file
  val classpathString = classpath.files.map("/app/" + _.getName)
    .mkString(":") + ":" + jarTarget
  new Dockerfile {
    // Base image
    from("openjdk:8-jre")
    // Add all files on the classpath
    add(classpath.files, "/app/")
    // Add the JAR file
    add(jarFile, jarTarget)
    // On launch run Java with the classpath and the main class
    entryPoint("java", "-cp", classpathString, mainclass)
  }
}