package ngstk

/**
 * Tools for working with BAM files
 *
 * @author Vince Reuter
 */
object BAMTools {
  import java.io.{ BufferedWriter, File, FileWriter }
  import htsjdk.samtools.{ SamReaderFactory, SAMRecord }

  def listTags(
    tagSpecs: List[(String, (String, Object => String))])(
    bamfile: File)(outfile: File, sep: String = "\t"): File = {
    require(bamfile.getPath != outfile.getPath, s"Input and output file match: ${bamfile}")
    val reader = SamReaderFactory.makeDefault().open(bamfile)
    val reads = reader.iterator()
    val writer = new BufferedWriter(new FileWriter(outfile))
    try {
      while (reads.hasNext) {
        val r = reads.next()
        val data = tagSpecs map { case (t, (filler, mapper)) => {
          val x = r.getAttribute(t)
          if (x == null) filler else mapper(x)
        } }
        writer.write(data mkString sep)
        writer.newLine()
      }
    }
    finally {
      reads.close()
      reader.close()
      writer.close()
    }
    outfile
  }

}
