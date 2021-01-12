package ngstk

import com.typesafe.scalalogging.StrictLogging

/**
 * Symlink FASTQ and {@code BAM} files for a project into respective target folders.
 *
 * The benefits here are at least twofold:
 * 1. Structural compliance with tools/software that expect data file type-based layout
 *    on disk, rather than sample-based>
 * 2. Solving permissions issues, where the extant data are in read-only locations, but 
 *    we want to write to a folder based on input file folder.
 *
 *
 * The expeected input layouts are:
 *   Raw: raw root --> Sample_<> --> files
 *   Aln: aln root --> Sample_<> --> files
 * 
 * Level of nesting is fixed at 1, but the prefixes with which to locate the sample subfolders 
 * are adjustable via CLI options and arguments.
 *
 * @author Vince Reuter
 */
object SymlinkFqAndBam extends StrictLogging {
  import scala.sys._
  import java.io.File
  import java.nio.file.{ Files, Path }
  import scopt._
  import ngstkinfo.BuildInfo

  case class Config(
    rawSrc: File = new File(""), 
    alnSrc: File = new File(""), 
    rawDst: File = new File(""), 
    alnDst: File = new File(""), 
    rawSamplePrefix: String = "Sample_", 
    alnSamplePrefix: String = "Sample_", 
    rawSuffix: String = "fastq.gz", 
    alnSuffix: String = "bam", 
    useSampleSubfolders: Boolean = false
  )

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config](BuildInfo.name) {
      head(s"${BuildInfo.name}.SymlinkFqAndBam", BuildInfo.version.toString)
      opt[File]("rawSrc")
        .required()
        .action((f, c) => c.copy(rawSrc = f))
        .validate(f => if (f.isDirectory) Right(()) else Left(s"Alleged raw root folder isn't a directory: $f"))
        .text("Path to FS root where raw (e.g., FASTQ) files are to be found")
      opt[File]("alnSrc")
        .required()
        .action((f, c) => c.copy(alnSrc = f))
        .validate(f => if (f.isDirectory) Right(()) else Left(s"Alleged alignment folder isn't a directory: $f"))
        .text("Path to FS root where alignment files (e.g., BAMs) are to be found")
      opt[File]("rawDst")
        .required()
        .action((f, c) => c.copy(rawDst = f))
        .validate(f => if (f.exists) Left(s"Path for raw links exists: $f") else Right(()))
        .text("Path to folder into which raw reads files (e.g., FASTQs) are to be linked")
      opt[File]("alnDst")
        .required()
        .action((f, c) => c.copy(alnDst = f))
        .validate(f => if (f.exists) Left(s"Path for alignment file links exists: $f") else Right(()))
        .text("Path to folder into which alignment records (e.g., BAMs) are to be linked")
      opt[String]("rawSamplePrefix")
        .action((s, c) => c.copy(rawSamplePrefix = s))
        .text("Prefix for folders in the raw root")
      opt[String]("alnSamplePrefix")
        .action((s, c) => c.copy(alnSamplePrefix = s))
        .text("Prefix for folders in the alignment files' root")
      opt[String]("rawSuffix")
        .action((s, c) => c.copy(rawSuffix = s))
        .text("Suffix expected for each raw file to link")
      opt[String]("alnSuffix")
        .action((s, c) => c.copy(alnSuffix = s))
        .text("Suffix expected for each alignment file to link")
      opt[Unit]("useSampleSubfolders")
        .action((_, c) => c.copy(useSampleSubfolders = true))
        .text("Create a subfolder for each sample in the destination roots")
    }
    
    parser.parse(args, Config()).fold(throw new Exception("Illegal program usage")){ conf => {
      logger.info(s"Creating path(s) for raw files' folder: ${conf.rawDst}")
      conf.rawDst.mkdirs()
      logger.info(s"Creating path(s) for alignment files' folder: ${conf.alnDst}")
      conf.alnDst.mkdirs()
      val findFolders: (String, File) => List[File] = 
        (pfx, p) => p.listFiles.filter(x => x.isDirectory && x.getName.startsWith(pfx)).toList
      val findFiles: (String, File) => List[File] = 
        (sfx, p) => p.listFiles.filter(x => x.isFile && x.getName.endsWith(sfx)).toList
      val rawFolders = findFolders(conf.rawSamplePrefix, conf.rawSrc)
      val rawFiles: List[(File, String)] = 
        rawFolders.flatMap(fldr => findFiles(conf.rawSuffix, fldr).map(f => f -> fldr.getName))
      val alnFolders = findFolders(conf.alnSamplePrefix, conf.alnSrc).map(f => new File(f, "star"))
      val alnFiles: List[(File, String)] = 
        alnFolders.flatMap(fldr => findFiles(conf.alnSuffix, fldr).map(f => f -> fldr.getName))
      def getTarget(folder: File): (String, String) => Path = {
        if (conf.useSampleSubfolders) { (sample, filename) => {
          val subfolder = new File(folder, sample)
          subfolder.mkdirs()
          new File(subfolder, filename).toPath
        } } else { (_, fn) => new File(folder, fn).toPath }
      }
      logger.info(s"Linking ${rawFiles.size} raw files, to ${conf.rawDst}")
      rawFiles foreach { case (f, sample) => {
        val dst = getTarget(conf.rawDst)(sample, f.getName)
        logger.debug(s"Sample: $sample")
        logger.debug(s"Source: $f")
        //logger.debug(s"Target: $target")
        Files.createSymbolicLink(dst, f.toPath)
        ()
      } }
      logger.info(s"Linking ${alnFiles.size} alignment files, to ${conf.alnDst}")
      alnFiles foreach { case (f, sample) => Files.createSymbolicLink(getTarget(conf.alnDst)(sample, f.getName), f.toPath); () }
      logger.info("Done.")
    } }
  }

}