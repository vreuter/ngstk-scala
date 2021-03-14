package ngstk

/**
 * A type that attempts to parse a piece of metadata from each row of a table-like collection.
 * @author Vince Reuter
 */
trait MetadataColumnParser {
  /** The key from a metadata mapping, whose value this parser will operate on */
  def key: String
  /** Determine whether a particular column name indicates that a field 'applies' to this parser. */
  def applies: (String, String) => Boolean
  /** Process a single value from the collection operated on */
  def parseValue: String => String
}

/**
 * Helpers associated with metadata column parsing
 *
 * @author Vince Reuter
 */
object MetadataColumnParser {
  import java.io.File
  import scala.io.Source
  import RGAttributes._
  
  /**
   * Create a new column parser.
   *
   * @param k The relevant key from a table-like file with header
   * @param use Whether a column name (second arg) 'matches' the key (first arg)
   * @param parse How to parse each value from a selected column
   * @return A new parser instance, which considers column names, decides which to use, and 
   *         then parses values
   */
  def apply(k: String, use: (String, String) => Boolean, parse: String => String): MetadataColumnParser = {
    new MetadataColumnParser {
      def key: String = k
      def applies: (String, String) => Boolean = use
      def parseValue: String => String = parse
    }
  }

  /**
   * Perhaps the most basic parser: predicate on equality, and simply return each value as-is (no-op).
   *
   * @param k The key (column name) the parser should look for
   * @return A new parser instance, testing column name for equality with the given key, and 
   *         simply returning each value from the selected column as-is
   */
  def eqIdParse(k: String): MetadataColumnParser = new MetadataColumnParser {
    def key: String = k
    def applies: (String, String) => Boolean = {
      import cats.instances.string._, cats.syntax.eq._
      (k, that) => k === that
    }
    def parseValue: String => String = p => p
  }

  /**
   * Attempt to parse a particular metadata item from a file.
   * @tparam A The type of value encoded in the conceptual column ("conceptual" because 
   *           the parsers may use different keys, if we're unsure how columns are named).
   * @param parsers The attempts to try out
   * @param f The file to read
   * @param sep Delimiter between fields on each line of the file
   * @param lift How to make an {@code A} from a raw {@code String}
   * @return Either {@code Left}-wrapped fail message, or a {@code Right}-wrapped list of parsed values
   */
  def tryParseMetadata[A](parsers: List[MetadataColumnParser])(
    f: File, sep: String)(lift: String => A): Either[String, List[A]] = {
    import scala.annotation.tailrec
    val (header, dataLines) = Source.fromFile(f).getLines().map(_.split(sep)).toList match {
      case h :: t => h.toList -> t
      case Nil => throw new Exception(s"No lines: $f")
    }
    @tailrec
    def go(readers: List[MetadataColumnParser]): Either[String, List[String]] = readers match {
      case Nil => Left("No match")
      case read :: rest => {
        header.zipWithIndex filter { case (col, _) => read.applies(read.key, col) } match {
          case (_, idx) :: Nil => Right(dataLines.map(fields => fields(idx)))
          case _ => go(rest)
        }
      }
    }
    go(parsers).map(_.map(lift))
  }

}
