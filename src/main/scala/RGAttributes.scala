package ngstk

/**
 * Attributes / data associated with sequencing read groups
 *
 * @author Vince Reuter
 */
object RGAttributes {
  import java.io.File
  import htsjdk.samtools.{ SAMReadGroupRecord }

  /** A type that represents some piece of read group data */
  sealed trait ReadGroupAttribute { def print: String }
  /** Name of sequencing platform, e.g. ILLUMINA or PACBIO */
  final case class PlatformName(get: String) extends ReadGroupAttribute { def print: String = this.get.toLowerCase }
  
  /** Sequencing device's flowcell identifier */
  final case class Flowcell(get: String) extends ReadGroupAttribute { def print: String = this.get }
  
  /** The sequencer */
  final case class Instrument(get: String) extends ReadGroupAttribute { def print: String = this.get }
  
  /** Sequencing lane */
  final case class Lane(get: Int) extends ReadGroupAttribute { def print: String = this.get.toString }
  
  /** Specific error type for an supported sequencing platform */
  final case class IllegalPlatformException(msg: String, platform: PlatformName) extends Exception(msg)

  /** Name of a sequenced sample */
  final case class SampleName(get: String)
  
  /** Sequencing library ID */
  final case class LibId(get: String)

  /**
   * Pair up each sample with a pair of platform name and library ID. This basically just checks for length agreement.
   *
   * @param sampleNames Sequence of sample names
   * @param platformNames Sequnence of platform names
   * @param libIds Sequence of library IDs 
   * @return Paired-up sample names and pairs of platform name and library ID
   */ 
  def bindMetadataToSampleNames(sampleNames: List[SampleName], 
    platformNames: List[PlatformName], libIds: List[LibId]): List[(SampleName, (PlatformName, LibId))] = {
    require(sampleNames.size == platformNames.size, 
      s"${sampleNames.size} sample(s) and ${platformNames.size} platform name(s)")
    require(sampleNames.size == libIds.size, s"${sampleNames.size} sample(s) and ${libIds.size} library ID(s)")
    require(sampleNames.map(_.get).toSet.size == sampleNames.size, 
      s"${sampleNames.size} sample name(s) but only ${sampleNames.map(_.get).toSet.size} unique")
    (sampleNames zip platformNames.zip(libIds))
  }

  /**
   * Attempt to parse lane number from FASTQ filepath
   *
   * @param fq Path to FASTQ file
   * @return Either a {@code Left}-wrapped failure reason or a {@code Right}-wrapped lane
   */
  def fastq2LaneNumber(fq: File): Either[String, Lane] = {
    import scala.util.matching.Regex
    "L00[1-9]".r.findAllMatchIn(fq.getName).toList match {
      case m :: Nil => Right(Lane(m.toString.last.toString.toInt))
      case ms => Left(s"${ms.size} match(es)")
    }
  }

  /**
   * Read sameple name from filepath. 
   *
   * @param f Filepath from which to infer sample name
   * @return Name of sample inferred from filepath
   */
  def file2Sample(f: File): SampleName = 
    SampleName(f.getName.split("\\.").head.stripPrefix("Sample_"))

  /**
   * Create a function to parse instrument, flowcell, and lane from sequencing read name.
   *
   * @param platform Name of a sequencing platform, which implies how to parse read names
   * @return Function parsing instrument, flowcell, and sequencing lane from read name
   */
  def getReadNameParser(platform: PlatformName): (String => (Instrument, Flowcell, Lane)) = 
    platform.get.toLowerCase match {
      case "illumina" => parseIlluminaReadName(_: String)
      case name => throw new IllegalPlatformException(
        s"Read name parsing not implemented for platform: ${name}", platform)
    }

  /**
   * How to parse instrument, flowcell, and lane from read names emitted by Illumina sequencing.
   *
   * @param readName The name of a sequencing read
   * @return Instrument name, flowcell, and sequencing lane corresponding to the read
   */
  def parseIlluminaReadName(readName: String): (Instrument, Flowcell, Lane) = {
    val fields = readName.split(":")
    (Instrument(fields(0)), Flowcell(fields(2)), Lane(fields(3).toInt))
  }

  /** Attempt to read Illumina flowcell and lane from a read group record. */
  def rg2IluminaFCLane: SAMReadGroupRecord => Either[String, (Flowcell, Lane)] = rg => {
    val id = rg.getId
    id.split("\\.").toList match {
      case f :: l :: Nil => {
        try { Right(Flowcell(f) -> Lane(l.toInt)) }
        catch { case _: NumberFormatException => Left(s"Non-numeric lane in ID: $id") }
      }
      case fields => Left(s"Cannot resolve flow/lane pair from ID: ${id}")
    }
  }

  /**
   * Determine sample name from delimited filename, splitting on delimited and taking first field.
   *
   * @param fq Filepath from which to infer sample name
   * @param sep Delimiter of 'fields' encoded in filepath
   * @return Inferred sample name
   */
  def sampleFromDelimitedFilenameHead(fq: File, sep: String = "_"): SampleName = 
    SampleName(fq.getName.split(sep).head)

  /**
   * Attempt to parse flowcell from FASTQ, peeking at the first record.
   *
   * @param fq Path to the FASTQ file to use
   * @return Either a {@code Left}-wrapped fail message, or a {@code Right}-wrapped inferred flowcell
   */
  def tryFlowcellFromFastq(fq: File): Either[String, Flowcell] = {
    import htsjdk.samtools.fastq.FastqReader
    val reader = new FastqReader(fq)
    try { Right(Flowcell(reader.next().getReadName.split(":")(2))) }
    catch { case e: Exception => Left(e.getMessage) }
    finally { reader.close() }
  }

}