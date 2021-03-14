package ngstk

import cats.data.{ NonEmptyList => NEL }
import htsjdk.samtools.{ SAMReadGroupRecord }
import RGAttributes._

/**
 * A collection of read groups, e.g. pertinent to a single BAM
 *
 * @param groups Nonempty collection of read groups records
 * @return A new instance
 */
final case class RGBlock(groups: NEL[SAMReadGroupRecord]) {
  def sortedByLane: List[SAMReadGroupRecord] = 
    groups.toList.sortBy(rg => unsafeGetLane(rg).get)
  private[this] def unsafeGetLane: SAMReadGroupRecord => Lane = 
    rg => rg2IluminaFCLane(rg).fold(err => throw new Exception(err), _._2)
}

/**
 * Helpers for working with blocks/groups of read groups
 *
 * @author Vince Reuter
 */
object RGBlock {
  import mouse.boolean._, cats.syntax.eq._, cats.instances.int._, cats.syntax.list._
  import htsjdk.samtools.{ SAMFileHeader }
  
  /**
   * Update a header with the groups contained within the given block.
   *
   * @param header The alignment file header to which read groups should be added
   * @param block The collection of read groups to add to the header
   * @return Updated header
   */
  def addReadGroupsToHeader(header: SAMFileHeader)(block: RGBlock): SAMFileHeader = 
    block.sortedByLane.foldLeft(header){ case (h, rg) => { h.addReadGroup(rg); h } }

  /**
   * Ensure that each read group is present just once in the block, checking for unique RG ID.
   *
   * @param groups
   * @return Either a {@code Left} explaining why block creation failed, or a {@code Right} 
   *         wrapping a successfully built block, with check that RG IDs present are unique.
   */
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
