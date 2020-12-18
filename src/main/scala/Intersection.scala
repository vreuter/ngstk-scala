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
  import htsjdk.samtools.{ SAMRecord }

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

}
