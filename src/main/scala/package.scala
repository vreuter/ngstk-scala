/**
 * NGS toolkit
 * @author Vince Reuter
 */
package object ngstk {
  import java.io.File
  import cats.{ Eq }
  import htsjdk.samtools.{
    SAMFileHeader, SAMRecord, SAMReadGroupRecord, SAMRecordIterator, 
    SAMFileWriterFactory, SamReaderFactory }
  import RGAttributes._
  
  def sampleFromDelimitedFilenameHead(fq: File, sep: String = "_"): SampleName = 
    SampleName(fq.getName.split(sep).head)

  private[this] def fq2Sn: File => SampleName = sampleFromDelimitedFilenameHead(_: File, sep = "_")

  def defaultDefineReadGroups(
    metaFile: File, fastqFolder: File, 
    sep: String = "\t"): List[(SampleName, List[ReadGroupData])] = {
    ReadGroupData.defineReadGroups(    // go with named args here since types are the same.
      sampleParsers = List(MetadataColumnParser.basicParse("Sample Name")), 
      platformParsers = List(MetadataColumnParser.basicParse("Machine Manufacturer")), 
      libraryParsers = List(MetadataColumnParser.basicParse("BarcodeIdx")))(
        metadata = metaFile, sep = sep)(
          fastqFolder = fastqFolder, 
          fqFile2Sample = (f: File) => Right[String, SampleName](fq2Sn(f)) )
  }

  def defaultMapSampleToReadGroups(
    metaFile: File, fastqFolder: File, sep: String = "\t"): Map[SampleName, RGBlock] = {
    defaultDefineReadGroups(metaFile, fastqFolder, sep).foldLeft(Map.empty[SampleName, RGBlock]){
      case (accMap, (sn, rgDatas)) => {
        val block = RGBlock.blockFromGroupDataMultiset(rgDatas).fold(
          err => throw new Exception(err), identity _)
        accMap + (sn -> block)
      }
    }
  }

  def tryAddReadGroupToRead(read: SAMRecord)(
    groupByFlowLanePair: Map[(Flowcell, Lane), SAMReadGroupRecord]/*rgData: ReadGroupData*/): Either[String, SAMRecord] = {
    import cats.instances.int._, cats.instances.string._, cats.syntax.eq._
    import mouse.boolean._
    implicit val eqFlow: Eq[Flowcell] = Eq.by(_.get)
    implicit val eqLane: Eq[Lane] = Eq.by(_.get)
    val (_, flow, lane) = parseIlluminaReadName(read.getReadName)
    groupByFlowLanePair.get(flow -> lane).toRight(
      s"No match for flow/lane pair: (${flow.get}, ${lane.get})" ) map { 
      rg => { read.setAttribute("RG", rg.getId); read } }
  }

  def addReadGroup(rgBlockBySample: Map[SampleName, RGBlock])(inBam: File, outBam: File): Either[String, (SampleName, File)] = {
    import scala.collection.JavaConverters._
    import cats.syntax.eq._
    implicit val eqFile: Eq[File] = Eq.by(_.getPath)
    require(inBam =!= outBam, s"Input and output paths match: $inBam")
    val currName: SampleName = file2Sample(inBam)
    //val eqSampleName: Eq[SampleName] = Eq.by(_.get)
    rgBlockBySample.get(currName).toRight(s"No RG block for sample ${currName.get}").flatMap(rgBlock => {
      val inHeader: SAMFileHeader = SamReaderFactory.makeDefault().getFileHeader(inBam)
      val outHeader = RGBlock.addReadGroupsToHeader(inHeader)(rgBlock)
      val keyedBlock: Map[(Flowcell, Lane), SAMReadGroupRecord] = 
        rgBlock.groups.toList.foldRight(Map.empty[(Flowcell, Lane), SAMReadGroupRecord]){
          case (rg, acc) => rg2IluminaFCLane(rg).fold(
            err => throw new Exception(err), flowAndLane => acc + (flowAndLane -> rg))
        }
      val reader = SamReaderFactory.makeDefault().open(inBam)
      val iter: SAMRecordIterator = reader.iterator()
      val writer = new SAMFileWriterFactory().makeSAMOrBAMWriter(outHeader, false, outBam)
      var result: Either[String, (SampleName, File)] = Right(currName -> outBam)
      while (iter.hasNext && result.isRight) {
        result = tryAddReadGroupToRead(iter.next())(keyedBlock) match {
          case Left(err) => { print("Should halt!"); Left(err) }
          case Right(outRead) => {
            outRead.setHeader(outHeader)    // For ensuring the record's header has groups
            writer.addAlignment(outRead)
            result    /* TODO: setHeader? */
          }
        }
      }
      writer.close()
      reader.close()
      result
    })
  }

}
