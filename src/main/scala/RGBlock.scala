package ngstk

import cats.data.{ NonEmptyList => NEL }
import htsjdk.samtools.{ SAMReadGroupRecord }
import RGAttributes._

final case class RGBlock(groups: NEL[SAMReadGroupRecord]) {
  def sortedByLane: List[SAMReadGroupRecord] = 
    groups.toList.sortBy(rg => unsafeGetLane(rg).get)
  private[this] def unsafeGetLane: SAMReadGroupRecord => Lane = 
    rg => rg2IluminaFCLane(rg).fold(err => throw new Exception(err), _._2)
}

object RGBlock {
  import mouse.boolean._, cats.syntax.eq._, cats.instances.int._, cats.syntax.list._
  import htsjdk.samtools.{ SAMFileHeader }
  
  /*
  def apply(block: List[ReadGroupData]): Either[String, RGBlock] = 
    block.toNel.either("No read groups", _.map(_.toReadGroup)).flatMap { groups => {
      val uniq = groups.toList.map(_.getId).toSet
      (uniq.size === ).either(s"${rgs.size} read groups but ${ids.size} unique IDs", groups)
    } }
  */

  def addReadGroupsToHeader(header: SAMFileHeader)(block: RGBlock): SAMFileHeader = 
    block.sortedByLane.foldLeft(header){ case (h, rg) => { h.addReadGroup(rg); h } }

  // Guard against multiple group data objects mapping to the same RG ID, which is erroneous.
  // This can happen when multiple FASTQ files are for the same lane (as in case of R1 and R2 split)
  def blockFromGroupDataMultiset(groups: List[ReadGroupData]): Either[String, RGBlock] = {
    import cats.syntax.list._
    (groups.map(_.toReadGroup).foldLeft(Set.empty[String] -> List.empty[SAMReadGroupRecord]){
      case ((seen, acc), rg) => {
        val rgid = rg.getId
        if (seen(rgid)) (seen, acc) else (seen + rgid, acc :+ rg)
      }
    })._2.toNel.toRight("No read groups").map(block => RGBlock(block))
  }

}
