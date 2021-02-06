package ngstk

/**
 * Intersection-like operations
 *
 * Mainly, initially at least, intended for read/site operations, like 
 * intersecting targeted SNV sites in BED/VCF-like with alignments in BAM-like
 *
 * @author Vince Reuter
 */
object Intersection {

  import scala.collection.IterableOps  
  import htsjdk.samtools.{ SAMRecord, SAMRecordIterator }

  private[this] type Tally = Map[Int, Int]
  private[this] type TallyPair = (Tally, Tally)

  /**
   * Drag arbitrary, coordinate-paired data "through" a read, retaining only those elements with coordinate "in bounds."
   *
   * Note here the coordinate scheme. Per the {@code htskdk} docs, the {@code getStart} and {@code getEnd} methods on 
   * {@code SAMRecord} return 1-based (inclusive) positions, while the coordinate in each {@code sites} element will likely 
   * come from a format like BED, where the cordinates are 0-based left-inclusive. Also BEWARE of the fact that to return 
   * just the basic {@code Int} type, the star/end methods on {@code SAMRecord} return 0 if there's no alignment.
   *
   * @tparam A The arbirary data type for the piece of data paired with each coordinate / genome position
   * @tparam CC The collection type constructor ("Collection Constructor")
   * @param sites The collection of pairs genome coordinate and arbitrary datum
   * @param r An aligned sequencing read (e.g., BAM file record)
   * @return Collection of input elements for which the coordinate/position is "within" the read {@code r}, 
   *         as determined by the read's alignment start and end position; BEWARE OF SPLIT READS
   */
  def byCoordinateThroughRead[A, CC[_]](sites: IterableOps[(Int, A), CC, CC[(Int, A)]])(r: SAMRecord): CC[(Int, A)] = {
    sites.filter(posAndA => posAndA._1 >= r.getStart && posAndA._1 <= r.getEnd )
  }

  /**
   * Tally up occurrences of sites within reads, by distance from start/end
   *
   * @tparam A The arbirary data type for the piece of data paired with each coordinate / genome position
   * @tparam CC The collection type constructor ("Collection Constructor")
   * @param sites The collection of pairs genome coordinate and arbitrary datum
   * @param reads Iterator over sequencing reads / aligned records
   * @param useUnaligned Whether to use data from an unaligned position
   * @return Mapping from targeted site to pair of count of unaligned overlaps and dist of site from read start/end
   */
  def tallyStartEndDistances[A](sites: List[(Int, A)])(
    reads: SAMRecordIterator, useUnaligned: Boolean): Map[Int, (Int, TallyPair)] = {
    val getOverlaps: SAMRecord => List[(Int, A)] = byCoordinateThroughRead(sites) _
    // Track the number of unaligned sites, and the start/end distances
    var talliesBySite = Map.empty[Int, (Int, TallyPair)]
    while (reads.hasNext) {
      val r = reads.next()
      talliesBySite = getOverlaps(r).foldLeft(talliesBySite){ case (acc, (pos, _)) => {
        val (unalnCount, (fromStart, fromEnd)) = acc.getOrElse(pos, 0 -> (newTally -> newTally))
        val (newUnaln, (newFromStart, newFromEnd)) = {
          if (useUnaligned || r.getReadPositionAtReferencePosition(pos) != 0) {
            val dStart = pos - r.getStart
            val dEnd = r.getEnd - pos
            val nStart = fromStart.getOrElse(dStart, 0) + 1
            val nEnd = fromEnd.getOrElse(dEnd, 0) + 1
            (unalnCount, (fromStart + (dStart -> nStart), fromEnd + (dEnd -> nEnd)))
          } else { ((unalnCount + 1), (fromStart, fromEnd)) }
        }
        acc + (pos -> (newUnaln -> (newFromStart -> newFromEnd)))
      } }
    }
    talliesBySite.toMap
  }

  private[this] def newTally: Tally = Map.empty[Int, Int]

}
