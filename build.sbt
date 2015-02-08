import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import generate.protobuf._

val akkaVersion = "2.3.9"

val project = Project(
  id = "akka-data-replication",
  base = file("."),
  settings = Project.defaultSettings ++ SbtMultiJvm.multiJvmSettings ++ bintrayPublishSettings ++ Protobuf.settings ++ Seq(
    organization := "com.github.patriknw",
    name := "akka-data-replication",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    version := "0.11-SNAPSHOT",
    scalaVersion := "2.11.5",
    crossScalaVersions := Seq("2.10.4", "2.11.5"),
    // compile options
    scalacOptions in compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.6", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in compile ++= Seq("-encoding", "UTF-8", "-source", "1.6", "-target", "1.6", "-Xlint:unchecked", "-Xlint:deprecation"),
    javacOptions in doc ++= Seq("-encoding", "UTF-8", "-source", "1.6"),
    (packageBin in Compile) := {
      // should run the check before publish, but couldn't find out how, so this will do
      val specVersion = sys.props("java.specification.version")
      assert(specVersion == "1.6", "Java 1.6 required for release")
      (packageBin in Compile).value
    },
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % "2.1.3" % "test"),
    // add scala-xml dependency when needed (for Scala 2.11 and newer) in a robust way
    // this mechanism supports cross-version publishing
    libraryDependencies := {
      CrossVersion.partialVersion(scalaVersion.value) match {
        // if scala 2.11+ is used, add dependency on scala-xml module (needed for scalatest)
        case Some((2, scalaMajor)) if scalaMajor >= 11 =>
          libraryDependencies.value ++ Seq("org.scala-lang.modules" %% "scala-xml" % "1.0.1" % "test")
        case _ => libraryDependencies.value
      }
    }, 
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target, 
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults)  =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
          Tests.Output(overall,
            testResults.events ++ multiNodeResults.events,
            testResults.summaries ++ multiNodeResults.summaries)
    }
  )
) configs (MultiJvm)




