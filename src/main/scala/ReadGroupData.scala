package ngstk

import java.io.File
import htsjdk.samtools.{ SAMReadGroupRecord }
import com.typesafe.scalalogging.LazyLogging
import RGAttributes._

/**
 * Encapsulation of the minimal data to fully specify a read's read group.
 *
 * There's a bit of ambiguity over exactly what 'read group' defines, but this type aims to comply with 
 * [[https://gatk.broadinstitute.org/hc/en-us/articles/360035890671-Read-groups Broad's GATK specification]].
 *
 * @param flow The instrument cell from on which the read / group was sequenced
 * @param lane The sequencing lane a read / group came from
 * @param platform The "platform," like instrument makes, used for sequencing, e.g. ILLUMINA or PACBIO
 * @param sample The name of the sample with which the a read / group is associated
 * @param lib The library ID
 * @return A new instance of this data storage class
 * @author Vince Reuter
 */
final case class ReadGroupData(flow: Flowcell, lane: Lane, platform: PlatformName, sample: SampleName, lib: LibId) {
  final def library: LibId = lib
  final def flowcell: Flowcell = flow
  final def sampleName: SampleName = sample
  final def platformName: PlatformName = platform
  private def id: String = s"${flow.get}.${lane.get}"
  private def pl: String = platform.get
  private def pu: String = s"${flow.get}.${lane.get}.$lb"
  private def lb: String = lib.get
  private def sm: String = sample.get
  def toReadGroup: SAMReadGroupRecord = {
    val rec = new SAMReadGroupRecord(this.id)
    rec.setPlatformUnit(this.pu)
    rec.setPlatform(this.pl)
    rec.setSample(this.sm)
    rec.setLibrary(this.lb)
    rec
  }
}

/**
 * Helpers for working with read groups and {@code ReadGroupData}
 *
 * @author Vince Reuter
 */
object ReadGroupData extends LazyLogging {

  /**
   * For each sample parsed from a metadata table / sheet, determine associated read groups.
   *
   * This fuction operates on a table-like text file (arbitrarily delimited), and aims to 
   * parse the minimal metadata set for each sample to fully specify the read groups associated 
   * with each sample.
   *
   * To do so, besides the metadata sheet, the FASTQ files for the batch of samples 
   * defined by the sheet are necessary, to associate flowcell and lane with each sample. 
   * It's assumed that the flowcell can be gleaned from the read name, and that the lane can be 
   * gleaned from the FASTQ filename.
   *
   * @param sampleParsers 
   * @param platformParsers
   * @param libraryParsers
   * @param metadata Path to sample table / sheet file
   * @param sep Delimiter in the sample table / sheet
   * @param fastqFolder Path to directory with this batch's FASTQ files
   * @param fqFile2Sample How to attempt to parse sample name from FASTQ filepath
   * @return List of pairs of sample name and associated bundles of read group identifying data
   */
  def defineReadGroups(
    sampleParsers: List[MetadataColumnParser], 
    platformParsers: List[MetadataColumnParser], 
    libraryParsers: List[MetadataColumnParser])(metadata: File, sep: String)(
    fastqFolder: File, fqFile2Sample: File => Either[String, SampleName]): List[(SampleName, List[ReadGroupData])] = {
    import cats.{ Alternative, Order }
    import cats.instances.either._, cats.instances.int._, cats.instances.list._, cats.instances.string._
    
    val boundMeta: Either[String, Map[SampleName, (PlatformName, LibId)]] = for {
      samples <- MetadataColumnParser.tryParseMetadata(sampleParsers)(metadata, sep)(SampleName(_))
      platforms <- MetadataColumnParser.tryParseMetadata(platformParsers)(metadata, sep)(PlatformName(_))
      libraries <- MetadataColumnParser.tryParseMetadata(libraryParsers)(metadata, sep)(LibId(_))
    } yield bindMetadataToSampleNames(samples, platforms, libraries).toMap
    
    val (bads, goods): (List[String], List[(SampleName, Flowcell, Lane)]) = Alternative[List].separate(
      fastqFolder.listFiles.toList map { fq => for {
        sample <- fqFile2Sample(fq)
        flow <- tryFlowcellFromFastq(fq)
        lane <- fastq2LaneNumber(fq)
      } yield (sample, flow, lane) } )
    
    if (bads.nonEmpty) throw new Exception(s"${bads.size} error(s): ${bads mkString ", "}")
    
    implicit val ordLane: Ordering[Lane] = Order.by((_: Lane).get).toOrdering
    val flowAndLaneBySample: Map[SampleName, List[(Flowcell, Lane)]] = {
      import scala.collection.compat._    // for .view.mapValues from 2.13, for 2.12 compat
      goods.groupBy(_._1).view.mapValues(_.map(tup => tup._2 -> tup._3).sortBy(_._2)).toMap
    }
    
    implicit val ordSampleName: Ordering[SampleName] = Order.by((_: SampleName).get).toOrdering
    boundMeta.fold( err => throw new Exception(err), bound => {
      flowAndLaneBySample.toList.sortBy(_._1).foldRight(List.empty[(SampleName, List[ReadGroupData])]){
        case ((sn, flowLanePairs), acc) => {
          try {
            val (platName, libId) = bound(sn)
            val rgDatas = flowLanePairs map { case (flow, lane) => ReadGroupData(flow, lane, platName, sn, libId) }
            (sn, rgDatas) :: acc
          }
          catch { case e: NoSuchElementException => {
            logger.warn(s"Sample name isn't bound to platform/ID: ${sn.get}")
            acc
          } }
        }
      }
    } )
  }

}
