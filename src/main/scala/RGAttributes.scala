package ngstk

object RGAttributes {
  import java.io.File
  import htsjdk.samtools.{ SAMReadGroupRecord }

  sealed trait ReadGroupAttribute { def print: String }
  final case class PlatformName(get: String) extends ReadGroupAttribute { def print: String = this.get.toLowerCase }
  final case class Flowcell(get: String) extends ReadGroupAttribute { def print: String = this.get }
  final case class Instrument(get: String) extends ReadGroupAttribute { def print: String = this.get }
  final case class Lane(get: Int) extends ReadGroupAttribute { def print: String = this.get.toString }
  final case class SampleNumber(get: Int) extends ReadGroupAttribute { def print: String = this.get.toString }
  final case class IllegalPlatformException(msg: String, platform: PlatformName) extends Exception(msg)

  final case class SampleName(get: String)
  final case class LibId(get: String)

  def bindMetadataToSampleNames(
    sampleNames: List[SampleName], platformNames: List[PlatformName], libIds: List[LibId]): List[(SampleName, (PlatformName, LibId))] = {
    require(sampleNames.size == platformNames.size, s"${sampleNames.size} sample(s) and ${platformNames.size} platform name(s)")
    require(sampleNames.size == libIds.size, s"${sampleNames.size} sample(s) and ${libIds.size} library ID(s)")
    require(sampleNames.map(_.get).toSet.size == sampleNames.size, 
      s"${sampleNames.size} sample name(s) but only ${sampleNames.map(_.get).toSet.size} unique")
    (sampleNames zip platformNames.zip(libIds))
  }

  def fastq2LaneNumber(fq: File): Either[String, Lane] = {
    import scala.util.matching.Regex
    "L00[1-9]".r.findAllMatchIn(fq.getName).toList match {
      case m :: Nil => Right(Lane(m.toString.last.toString.toInt))
      case ms => Left(s"${ms.size} match(es)")
    }
  }

  def file2Sample(f: File): SampleName = 
    SampleName(f.getName.split("\\.").head.stripPrefix("Sample_"))

  def getReadNameParser(platform: PlatformName): (String => (Instrument, Flowcell, Lane)) = platform.get.toLowerCase match {
    case "illumina" => parseIlluminaReadName(_: String)
    case name => throw new IllegalPlatformException(s"Read name parsing not implemented for platform: ${name}", platform)
  }

  def parseIlluminaReadName(readName: String): (Instrument, Flowcell, Lane) = {
    val fields = readName.split(":")
    (Instrument(fields(0)), Flowcell(fields(2)), Lane(fields(3).toInt))
  }

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

  def sampleFromDelimitedFilenameHead(fq: File, sep: String = "_"): SampleName = 
    SampleName(fq.getName.split(sep).head)

  def tryFlowcellFromFastq(fq: File): Either[String, Flowcell] = {
    import htsjdk.samtools.fastq.FastqReader
    val reader = new FastqReader(fq)
    try { Right(Flowcell(reader.next().getReadName.split(":")(2))) }
    catch { case e: Exception => Left(e.getMessage) }
    finally { reader.close() }
  }

}