package processing

import fs2.{io, text, Task, Stream}
import java.nio.file.{Paths, Path, Files}

object Main extends App {
  val englishDir   = Paths.get("../data/Universal Dependencies 1.3/ud-treebanks-v1.3/UD_English")
  val processedDir = Paths.get("../data/processed/")
  
  val enDev     = englishDir.resolve("en-ud-dev.conllu")
  val enDevProc = processedDir.resolve("en-ud-keywords.txt")

  val allowedCharsSet = Set('.', ',', ' ')

  def allowedChars(c: Char): Boolean =
    c.isLetter || c.isDigit || allowedCharsSet(c)

  def converter(from: Path, to: Path): Stream[Task, Unit] =
    io.file.readAll[Task](from, 4096)
      .through(text.utf8Decode)
      .map(_
        .split("\n\n")
        .map(_
          .split("\n")
          .map(_.split("\\s"))
          .collect {
            case elems if elems.length > 7 =>
              val word = elems(1)
              val tpe  = elems(3)  // NOUN
              val tpe2 = elems(7)  // nsubj
              (word, tpe, tpe2)
          }
          .foldLeft(("", List.empty[String])) { case ((sentence, keywords), (w, tpe, tpe2)) =>
            val newSentence =
              if (w.length == 1 && w.forall(!_.isLetter)) sentence + w
              else sentence + " " + w

            val newKeywords =
              if (tpe == "NOUN" && tpe2 == "nsubj") keywords :+ w
              else keywords

            (newSentence, newKeywords)
          }
        )
        .map { case (sentence, keywords) =>
          sentence + "\t" + keywords.mkString("\t")
        }
        .mkString("\n")
        .trim
      )
      .intersperse("")
      .through(text.utf8Encode)
      .through(io.file.writeAll(to))

  converter(enDev, enDevProc).run.unsafeRun
}
