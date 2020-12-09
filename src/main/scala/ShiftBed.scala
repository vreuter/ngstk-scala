package ngstk

import com.typesafe.scalalogging.StrictLogging

object ShiftBed extends StrictLogging {
  import java.io.{ BufferedWriter, File, FileWriter }
  import scala.io.Source
  import cats.{ Alternative, Eq }
  import mouse.boolean._
  import scopt._
  import ngstkinfo.BuildInfo
  
  case class Config(
    infile: File = new File(""), 
    outfile: File = new File(""), 
    shift: Int = 0
  )

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser[Config](BuildInfo.name) {
      head(s"${BuildInfo.name}.ShiftBed", s"${BuildInfo.version}")
      arg[File]("infile")
        .required()
        .action((f, c) => c.copy(infile = f))
        .validate(f => f.isFile.either(s"Missing input BED: $f", ()))
        .text("Path to BED to shift")
      arg[File]("outfile")
        .required()
        .action((f, c) => c.copy(outfile = f))
        .text("Path to BED to write")
      opt[Int]('S', "shift")
        .required()
        .action((d, c) => c.copy(shift = d))
        .text(s"Amount to shift by")
    }
    implicit val eqFile: Eq[File] = {
      import cats.instances.string._
      Eq.by(_.getPath)
    }
    parser.parse(args, Config()).fold(
      throw new Exception("Illegal program usage")){ conf => 
      import cats.instances.either._, cats.syntax.bifunctor._, cats.syntax.eq._
      val tryShift: Tuple2[Int, Int] => Either[String, (Int, Int)] = shiftCoordPair(conf.shift) _
      if (conf.infile === conf.outfile) throw new Exception(s"Outfile and infile match: ${conf.infile}")
      val parsed = Source.fromFile(conf.infile).getLines().toList.zipWithIndex map {
        case (l, i) => readBedLine(l).flatMap(
          chrPosDat3Tup => tryShift(chrPosDat3Tup._2).map(
            coords => (chrPosDat3Tup._1, coords, chrPosDat3Tup._3))).leftMap(msg => msg -> i)
      }
      val (bads, goods) = Alternative[List].separate(parsed.toList)
      if (bads.nonEmpty) throw new Exception(s"${bads.size} lines improperly parsed. First one: ${bads.head}")
      val writer = new BufferedWriter(new FileWriter(conf.outfile))
      try { goods foreach { case (chr, coords, data) => 
        writer.write(makeLine(chr, coords, data)); writer.newLine() } }
      finally { writer.close() }
    }
  }

  def makeLine(chr: String, coords: (Int, Int), data: Array[String]): String = 
    (Array(chr, coords._1.toString, coords._2.toString) ++ data) mkString "\t"

  def parseCoord: String => Either[String, Int] = s => {
    val unvalidated = {
      try { Right(s.toInt) }
      catch { case _: NumberFormatException => Left(s"Non-integral coordinate: $s") }
    }
    unvalidated.flatMap(x => (x > -1).either(s"Negative coordinate: $x", x))
  }

  def shiftCoord(shift: Int)(coord: Int): Either[String, Int] = {
    val x = coord + shift
    (x > -1).either(s"Negative coordinate when shifting $coord by $shift", x)
  }

  def shiftCoordPair(shift: Int)(pair: (Int, Int)): Either[String, (Int, Int)] = for {
    x1 <- shiftCoord(shift)(pair._1)
    x2 <- shiftCoord(shift)(pair._2)
  } yield x1 -> x2

  private[this] def readBedLine: String => Either[String, (String, (Int, Int), Array[String])] = s => {
    val fields = s.split("\t")
    val rawCoords = (fields.length > 2).either(s"Too few fields: ${fields.size}", fields(1) -> fields(2))
    val validCoords = rawCoords flatMap { case (start, end) => for {
      s <- parseCoord(start)
      e <- parseCoord(end)
    } yield s -> e }
    validCoords.map(pair => (fields(0), pair, fields.drop(3)))
  }

}
