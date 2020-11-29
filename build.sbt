name := "ngstk"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.13.2"
organization := "vreuter"

assemblyJarName in assembly := s"${name.value}_v${version.value}.jar"
publishTo := Some(Resolver.file(s"${name.value}",  new File(Path.userHome.absolutePath + "/.m2/repository")))

// https://mvnrepository.com/artifact/com.github.samtools/htsjdk
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.23.0"

/* More runtime-y stuff */
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

/* Core abstractions */
libraryDependencies += "org.typelevel" %% "cats-core" % "2.3.0"
libraryDependencies += "org.typelevel" %% "mouse" % "0.25"

/* Java and compiler options */
scalacOptions ++= Seq("-deprecation", "-feature", "-language:higherKinds")//pre-2.13, "-Ypartial-unification")
scalacOptions ++= Seq(
  "-Ymacro-annotations" // Circe
)

// Autogenerate source code as package episteminfo, with object BuildInfo, to access version; useful for GFF provenance.
lazy val root  = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoKeys := Seq[BuildInfoKey](name, version), buildInfoPackage := "ngstkinfo")

// Enable quitting a run without quitting sbt.
cancelable in Global := true

// Ignore certain file patterns for the build.
excludeFilter in unmanagedSources := HiddenFileFilter || "Interact*.scala" || ( new FileFilter { def accept(f: File) = Set("reserved")(f.getParentFile.getName) } )
