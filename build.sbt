import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import generate.protobuf._

val akkaVersion = "2.3.6"


val project = Project(
  id = "akka-data-replication",
  base = file("."),
  settings = Project.defaultSettings ++ SbtMultiJvm.multiJvmSettings ++ bintrayPublishSettings ++ Protobuf.settings ++ Seq(
    organization := "com.github.patriknw",
    name := "akka-data-replication",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    version := "0.8-SNAPSHOT",
    scalaVersion := "2.10.4",
    crossScalaVersions := Seq("2.10.4", "2.11.2"),
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


