lazy val scala213 = "2.13.5"
lazy val scala212 = "2.12.10"
lazy val supportedScalaVersions = List(scala213, scala212)

ThisBuild / name := "ngstk"
ThisBuild / version := "0.0.4-SNAPSHOT"
ThisBuild / scalaVersion := scala213
ThisBuild / organization := "vreuter"

assemblyJarName in assembly := s"${name.value}_v${version.value}.jar"
publishTo := Some(Resolver.file(s"${name.value}",  new File(Path.userHome.absolutePath + "/.m2/repository")))

// https://mvnrepository.com/artifact/com.github.samtools/htsjdk
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.23.0"

/* More runtime-y stuff */
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

/* Core abstractions */
libraryDependencies += "org.typelevel" %% "cats-core" % "2.4.2"
libraryDependencies += "org.typelevel" %% "mouse" % "1.0.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.4.2"

/* Java and compiler options */
Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-language:higherKinds")
Compile / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n < 13 => List("-Ypartial-unification")
    case _                      => Nil
  }
}

// Autogenerate source code as package episteminfo, with object BuildInfo, to access version; useful for GFF provenance.
lazy val root  = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version), 
    buildInfoPackage := "ngstkinfo", 
    crossScalaVersions := supportedScalaVersions
  )

// Enable quitting a run without quitting sbt.
cancelable in Global := true

// Ignore certain file patterns for the build.
excludeFilter in unmanagedSources := HiddenFileFilter || "Interact*.scala" || ( 
  new FileFilter { def accept(f: File) = Set("reserved")(f.getParentFile.getName) } )
