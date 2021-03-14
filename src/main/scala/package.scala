/**
 * NGS toolkit
 *
 * @author Vince Reuter
 */
package object ngstk {
  import java.io.File
  import cats.{ Eq }
  import htsjdk.samtools.{
    SAMFileHeader, SAMRecord, SAMReadGroupRecord, SAMRecordIterator, 
    SAMFileWriterFactory, SamReaderFactory }
  import RGAttributes._

  /**
   * Read sample name from filename, splitting on a delimiter and taking the first field.
   *
   * @param fq File (e.g. FASTQ) from which to obtain sample name
   * @param sep Delimiter between 'fields' encoded in the filename
   * @return Sample name inferred from the filename
   */
  def sampleFromDelimitedFilenameHead(fq: File, sep: String = "_"): SampleName = 
    SampleName(fq.getName.split(sep).head)

  /**
   * Basic, default definition of read groups based on a metadata table and FASTQs for a batch of samples
   *
   * @param metaFile Path to metadata table / sheet file for this batch of samples
   * @param fastqFolder Path to folder with FASTQ files for this batch of samples
   * @param sep Delimiter between fields in the metadata table / sheet file
   * @return Pairing of sample name to collection of sequencing read groups produced from that sample
   */
  def defaultDefineReadGroups(
    metaFile: File, fastqFolder: File, 
    sep: String = "\t"): List[(SampleName, List[ReadGroupData])] = {
    ReadGroupData.defineReadGroups(    // go with named args here since types are the same.
      sampleParsers = List(MetadataColumnParser.eqIdParse("Sample Name")), 
      platformParsers = List(MetadataColumnParser.eqIdParse("Machine Manufacturer")), 
      libraryParsers = List(MetadataColumnParser.eqIdParse("BarcodeIdx")))(
        metadata = metaFile, sep = sep)(
          fastqFolder = fastqFolder, 
          fqFile2Sample = (fq: File) => Right(sampleFromDelimitedFilenameHead(fq)) )
  }

  /**
   * Bind each sample name from a batch to a bundle of read group data.
   *
   * @param metaFile Path to this batch's metadata table / sheet file
   * @param fastqFolder Path to folder with FASTQs for this batch of samples
   * @param sep Delimiter between fields in metadata file
   * @return Mapping from sample name to bundle of read group data, typed as {@code RGBlock}
   * @see defaultDefineReadGroups
   * @see RGBlock
   */
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

  /**
   * Attempt to augment an aligned read with its read group
   *
   * @param read The aligment record to update
   * @param groupByFlowLanePair Mapping from pair of flowcell and sequencing lane to read group, 
   *                            used to determine the read group once read name admits flowcell and lane
   * @return Either a {@code Left}-wrapped fail message, or a {@code Right}-wrapped updated read
   */
  def tryAddReadGroupToRead(read: SAMRecord)(
    groupByFlowLanePair: Map[(Flowcell, Lane), SAMReadGroupRecord]): Either[String, SAMRecord] = {
    import cats.instances.int._, cats.instances.string._, cats.syntax.eq._
    import mouse.boolean._
    implicit val eqFlow: Eq[Flowcell] = Eq.by(_.get)
    implicit val eqLane: Eq[Lane] = Eq.by(_.get)
    val (_, flow, lane) = parseIlluminaReadName(read.getReadName)
    groupByFlowLanePair.get(flow -> lane).toRight(
      s"No match for flow/lane pair: (${flow.get}, ${lane.get})" ) map { 
      rg => { read.setAttribute("RG", rg.getId); read } }
  }

  /**
   * Add all read group info for an entire aligned reads file.
   *
   * @param rgBlockBySample Binding between sample name and read groups' data
   * @param inBam Path to file with aligned reads that lack read group info
   * @param outBam Path to write for updated BAM (i.e., same as input, but with read groups)
   * @return Either a {@code Left}-wrapped fail message, or a {@code Right}-wrapped pair 
   *         of sample name and read-group-containing filepath
   */
  def addReadGroup(rgBlockBySample: Map[SampleName, RGBlock])(
    inBam: File, outBam: File): Either[String, (SampleName, File)] = {
    import scala.collection.JavaConverters._
    import cats.syntax.eq._
    implicit val eqFile: Eq[File] = Eq.by(_.getPath)
    require(inBam =!= outBam, s"Input and output paths match: $inBam")
    val currName: SampleName = file2Sample(inBam)
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
