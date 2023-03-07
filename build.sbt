lazy val root = (project in file("."))
  .settings(
    name := "s3-data-processing",
    idePackagePrefix := Some("com.vigil"),
    version := "0.1.0-SNAPSHOT",
    organization := "com.vigil",
    scalaVersion := "2.12.14",
    // Add assembly settings
    assemblySettings
  )
  .enablePlugins(AssemblyPlugin)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "com.amazonaws" % "aws-java-sdk" % "1.12.411"
)

// Define assembly settings
lazy val assemblySettings = {
  // Use sbt-assembly plugin
  import sbtassembly.AssemblyPlugin
  import AssemblyPlugin.autoImport._

  // Define assembly settings
  Seq(
    // Merge all dependencies into a single JAR file
    assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    // Name the output JAR file
    assemblyJarName := s"${name.value}-${version.value}.jar"
  )
}

resolvers ++= Resolver.sonatypeOssRepos("snapshots")
resolvers += "sbt-plugin-releases" at "https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"
