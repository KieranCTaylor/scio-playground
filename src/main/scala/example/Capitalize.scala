package example

import com.spotify.scio._

/**
 * Simple demo application for scio
 * opens a textfile, capitalizes everything
 * then saves it back to gcs
 */
object Capitalize {

  def main(cliArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cliArgs)

    val input = args.getOrElse("input", "gs://k-scio-dev/wordCount.txt")
    val output = args.getOrElse("output", "gs://k-scio-dev/capitalizeOut")

    sc.textFile(input)
      .map(_.toUpperCase)
      .saveAsTextFile(output)

    val result = sc.run().waitUntilFinish()

    println("Errythang capitalized")
  }

}
