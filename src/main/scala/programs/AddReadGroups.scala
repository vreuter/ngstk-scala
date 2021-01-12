package ngstk

import com.typesafe.scalalogging.StrictLogging

/**
 * Add read groups to BAM header and records, based on project metadata.
 *
 * @author Vince Reuter
 */
object AddReadGroups extends StrictLogging {
  import java.io.{ BufferedWriter, File, FileWriter }
  import cats.Eq, cats.instances.string._, cats.syntax.eq._
  import scopt._
  import RGAttributes._
  import ngstkinfo.BuildInfo
  
  case class Config(
    metadataFile: File = new File(""), 
    fastqFolder: File = new File(""), 
    bamFolder: File = new File(""), 
    inBamExt: String = ".unique.bam", 
    outBamExt: String = ".grouped.bam", 
    metadataDelimiter: String = "\t", 
    overwrite: Boolean = false, 
    sampleSheetResult: File = new File("")
  )
  
  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config](BuildInfo.name) {
      head(s"${BuildInfo.name}.AddReadGroups", s"${BuildInfo.version}")
      opt[File]('M', "metadataFile")
        .required()
        .action((f, c) => c.copy(metadataFile = f))
        .text("Path to sample sheet/table file")
      opt[File]('F', "fastqFolder")
        .required()
        .action((f, c) => c.copy(fastqFolder = f))
        .text("Path to folder with FASTQ files, usually >= 1/sample/lane")
      opt[File]('B', "bamFolder")
        .required()
        .action((f, c) => c.copy(bamFolder = f))
        .text("Path to folder with alignments")
      opt[String]("inBamExt")
        .action((ext, c) => c.copy(inBamExt = ext))
        .text("Extension of BAM files to use")
      opt[String]("metadataDelimiter")
        .action((sep, c) => c.copy(metadataDelimiter = sep))
        .text("Delimiter between fields in metadata sheet/table file")
      opt[String]("outBamExt")
        .action((ext, c) => c.copy(outBamExt = ext))
        .text("Extension of BAM file to write")
      opt[Unit]("overwrite")
        .action((_, c) => c.copy(overwrite = true))
        .text("Overwrite any existing output file")
      opt[File]("outputSheetFile")
        .action((f, c) => c.copy(sampleSheetResult = f))
        .text("Path to output sample sheet to write, if desired")
    }
    parser.parse(args, Config()).fold(
      throw new Exception("Illegal program usage")){ conf => {
        logger.info(s"Determining RGs based on ${conf.metadataFile} and FASTQ folder ${conf.fastqFolder}")
        val rgBlockBySample: Map[SampleName, RGBlock] = defaultMapSampleToReadGroups(
          metaFile = conf.metadataFile, 
          fastqFolder = conf.fastqFolder, 
          sep = conf.metadataDelimiter)
        logger.info(
          s"${rgBlockBySample.size} sample(s): ${sampleNamesText(rgBlockBySample.toList.map(_._1))}")
        val inBams = conf.bamFolder.listFiles.toVector filter { f => f.getPath.endsWith(conf.inBamExt) }
        logger.info(s"${inBams.size} BAM(s) to use: ${inBams.map(_.getName).sorted.mkString(", ")}")
        val outBams = inBams.map(f => new File(f.getPath.replace(".bam", conf.outBamExt)))
        implicit val eqFile: Eq[File] = Eq.by(_.getPath)
        logger.info("Starting RG addition.")
        val rawResults = inBams.zip(outBams).foldLeft(Vector.empty[Either[String, (SampleName, File)]]){
          case (acc, (inBam, outBam)) => {
            val lockFile = new File(s"${outBam}.lock")
            val doAddRG = (lockFile.isFile -> outBam.isFile) match {
              case (false, false) => true
              case (false, true) => {
                if (conf.overwrite) {
                  logger.info(s"Overwriting target: $outBam")
                  outBam.delete()
                  true
                }
                else { logger.info(s"Target exists, skipping: $outBam"); false }
              }
              case (true, false) => {
                logger.warn(s"Lock file exists but target ($outBam) doesn't; removing lock: $lockFile")
                lockFile.delete()
                true
              }
              case (true, true) => {
                logger.info(s"Target ($outBam) exists but so does lock ($lockFile); replacing")
                lockFile.delete()
                outBam.delete()
                true
              }
            }
            val currentResult = if (doAddRG) {
              logger.info(s"Processing: $inBam")
              lockFile.createNewFile()
              val tmpRes: Either[String, (SampleName, File)] = addReadGroup(rgBlockBySample)(inBam, outBam)
              tmpRes.fold( msg => logger.warn(msg), { case (name, out) => {
                if (out =!= outBam) throw new Exception(s"Output and target differ; target=$outBam; output=$out")
                if (!out.isFile) throw new Exception(s"Target wasn't created: $out")
                else {
                  lockFile.delete()
                  logger.info(s"Wrote: $out")
                }
              } } )
              tmpRes
            } else { Left[String, (SampleName, File)]("Skipped RG addition attempt") }
            acc :+ currentResult
          }
        }
        val (_, nameOutFilePairs) = {
          import cats.Alternative, cats.instances.either._, cats.instances.list._
          Alternative[List].separate(rawResults.toList)
        }
        logger.info(s"Output files: ${nameOutFilePairs.map(_._2.getName).mkString(", ")}")
        if (conf.sampleSheetResult.getName === "") { logger.debug("No output sample sheet") }
        else {
          val sampleSheetFile = new File(conf.bamFolder, "grouped_sample_sheet.txt")
          logger.info(s"Writing sample sheet: ${conf.sampleSheetResult}")
          val writer = new BufferedWriter(new FileWriter(conf.sampleSheetResult))
          try {
            writer.write(s"sample_name\tgrouped_file")
            writer.newLine()
            nameOutFilePairs foreach { case (name, file) => {
              writer.write(s"${name.get}\t$file")
              writer.newLine()
            } }
          }
          finally { writer.close() }
        }
        logger.info("Done.")
    }}
  }

  private[this] def sampleNamesText(names: Iterable[SampleName], sep: String = ", "): String = 
    names.map(_.get).toList.sorted mkString sep

}
