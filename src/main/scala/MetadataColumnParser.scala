package ngstk

trait MetadataColumnParser {
  def key: String
  def applies: (String, String) => Boolean
  def parseValue: String => String
}

object MetadataColumnParser {
  import java.io.File
  import scala.io.Source
  import RGAttributes._
  
  def apply(k: String, use: (String, String) => Boolean, parse: String => String): MetadataColumnParser = {
    new MetadataColumnParser {
      def key: String = k
      def applies: (String, String) => Boolean = use
      def parseValue: String => String = parse
    }
  }

  def basicParse(k: String): MetadataColumnParser = new MetadataColumnParser {
    def key: String = k
    def applies: (String, String) => Boolean = {
      import cats.instances.string._, cats.syntax.eq._
      (k, that) => k === that
    }
    def parseValue: String => String = p => p
  }

  def attemptSampleSheetRead(
    parsers: List[MetadataColumnParser])(f: File, sep: String): Either[String, List[String]] = {
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
    go(parsers)
  }

  def tryParseMetadata[A](
    parsers: List[MetadataColumnParser])(
    f: File, sep: String)(lift: String => A): Either[String, List[A]] = 
    attemptSampleSheetRead(parsers)(f, sep).map(_.map(lift))

}
