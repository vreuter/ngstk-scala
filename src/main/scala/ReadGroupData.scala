package ngstk

import java.io.File
import htsjdk.samtools.{ SAMReadGroupRecord }
import RGAttributes._

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

object ReadGroupData {

  def defineReadGroups(
    sampleReads: List[MetadataColumnParser], 
    platReads: List[MetadataColumnParser], 
    libReads: List[MetadataColumnParser])(metadata: File, sep: String)(
    fastqFolder: File, fqFile2Sample: File => Either[String, SampleName]): List[(SampleName, List[ReadGroupData])] = {
    import cats.{ Alternative, Order }, cats.instances.either._, cats.instances.int._, cats.instances.list._, cats.instances.string._
    val boundMeta: Either[String, Map[SampleName, (PlatformName, LibId)]] = for {
      samples <- MetadataColumnParser.tryParseSampleNames(sampleReads)(metadata, sep)
      platforms <- MetadataColumnParser.tryParsePlatformNames(platReads)(metadata, sep)
      libraries <- MetadataColumnParser.tryParseLibraryIds(libReads)(metadata, sep)
    } yield bindMetadataToSampleNames(samples, platforms, libraries).toMap
    val (bads, goods): (List[String], List[(SampleName, Flowcell, Lane)]) = Alternative[List].separate(
      fastqFolder.listFiles.toList map { fq => for {
        sample <- fqFile2Sample(fq)
        flow <- tryFlowcellFromFastq(fq)
        lane <- fastq2LaneNumber(fq)
      } yield (sample, flow, lane) } )
    if (bads.nonEmpty) throw new Exception(s"${bads.size} error(s): ${bads mkString ", "}")
    implicit val ordLane: Ordering[Lane] = Order.by((_: Lane).get).toOrdering
    val flowAndLaneBySample: Map[SampleName, List[(Flowcell, Lane)]] = 
      goods.groupBy(_._1).view.mapValues(_.map(tup => tup._2 -> tup._3).sortBy(_._2)).toMap
    implicit val ordSampleName: Ordering[SampleName] = Order.by((_: SampleName).get).toOrdering
    boundMeta.fold(err => throw new Exception(err), bound => {
      flowAndLaneBySample.toList.sortBy(_._1).foldRight(List.empty[(SampleName, List[ReadGroupData])]){
        case ((sn, flowLanePairs), acc) => (sn, flowLanePairs map { case (flow, lane) => {
          val (platName, libId) = bound(sn)
          ReadGroupData(flow, lane, platName, sn, libId)
        } } ) :: acc
      }
    })
  }

}
