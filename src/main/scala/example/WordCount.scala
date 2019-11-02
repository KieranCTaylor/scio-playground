package example

import com.spotify.scio._

/**
  * Sample code for running word count on text file in GCS
  * sbt "runMain [PACKAGE].WordCount
  * --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  * --input=gs://dataflow-samples/shakespeare/kinglear.txt
  * --output=gs://[BUCKET]/[PATH]/wordcount"
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val (sc, cliArgs) = ContextAndArgs(args)

    val exampleData = "gs://k-scio-dev/wordCount.txt"
    val input = cliArgs.getOrElse("input", exampleData)
    val output = cliArgs("output")

    sc.textFile(input)
      .map(_.trim)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    val result: ScioResult = sc.close().waitUntilFinish()

    println("Finished")
  }

}
