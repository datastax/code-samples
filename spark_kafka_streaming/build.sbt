val DSE_HOME = sys.env.getOrElse("DSE_HOME", sys.env("HOME")+"dse")
val sparkClasspathStr = s"dse spark-classpath".!!.trim
val sparkClasspathArr = sparkClasspathStr.split(':')

val dseScalaVersionStr = Seq("/bin/sh", "-c", s"ls ${DSE_HOME}/resources/spark/lib/scala-compiler*jar").!!.trim
val dseScalaVersionArr = dseScalaVersionStr.split("-").last.split(".jar")(0).split('.')
val dseScalaVersion = dseScalaVersionArr.mkString(".") 
val dseScalaMajorMinorVersion = Seq(dseScalaVersionArr(0), dseScalaVersionArr(1)).mkString(".") 

val DSE_BIN = s"$DSE_HOME/bin/dse"
val dseVersionArr = s"$DSE_BIN -v".!!.trim.split('.')
val dseVersion = Seq(dseVersionArr(0), dseVersionArr(1)).mkString(".")

// This needs to match whatever Spark version being used in DSE
val sparkVersionStr = Seq("/bin/sh", "-c", s"ls ${DSE_HOME}/resources/spark/lib/spark-core_*jar").!!.trim
val sparkVersionArr = sparkVersionStr.split('-')(2).split('.') // expected: spark-core_2.11-2.0.0.1-2cf48f7.jar
val sparkVersion = Seq(sparkVersionArr(0), sparkVersionArr(1), sparkVersionArr(2)).mkString(".")
val kafkaVersion = "0.8.2.1" // we'll want to generalize this once spark officially supports newer versions
val kafkaMajorVersion = kafkaVersion.split('.')(0) 
val kafkaMinorVersion = kafkaVersion.split('.')(1) 
val scalaTestVersion = "2.2.4"
val jodaVersion = "2.9"

val sparkStreamingKafkaDep: String = {
  if (dseVersion.toDouble >= 5.1) { 
    s"spark-streaming-kafka-${kafkaMajorVersion}-${kafkaMinorVersion}"   
  } else {
    "spark-streaming-kafka"   
  }
}

// Find all Jars on dse spark-classpath
val sparkClasspath = {
  for ( dseJar <- sparkClasspathArr if dseJar.endsWith("jar"))
    yield Attributed.blank(file(dseJar))
}.toSeq 

val globalSettings = Seq(
  version := "0.1",
  scalaVersion := dseScalaVersion 
)

lazy val streaming = (project in file("streaming"))
                       .settings(name := "streaming")
                       .settings(globalSettings:_*)
                       .settings(libraryDependencies ++= streamingDeps)

val akkaVersion = "2.3.11"

// Do not define in streaming deps if we reference them in existing DSE libs
lazy val streamingDeps = Seq(
  "joda-time"         %  "joda-time"             % jodaVersion  % "provided",
  "org.apache.spark"  %% "spark-mllib"           % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-graphx"          % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-sql"             % sparkVersion % "provided",
  "org.apache.spark"  %% "spark-streaming"       % sparkVersion % "provided",
  "org.apache.spark"  %% sparkStreamingKafkaDep  % sparkVersion exclude("org.spark-project.spark", "unused"),
  "com.databricks"    %% "spark-csv"             % "1.2.0"
)

lazy val printenv = taskKey[Unit]("Prints classpaths and dependencies")
val env = Map("DSE_HOME" -> DSE_HOME, 
              "dseScalaVersion" -> dseScalaVersion,
              "dseVersion" -> dseVersion,
              "sparkClasspath" -> sparkClasspath)
              
printenv := println(env)

//Add dse jars to classpath
unmanagedJars in Compile ++= sparkClasspath 
unmanagedJars in Test ++= sparkClasspath 
