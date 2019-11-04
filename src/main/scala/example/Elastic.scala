package example

import com.spotify.scio._
import com.spotify.scio.elasticsearch._
import org.apache.http.HttpHost
import com.spotify.scio.values.SCollection
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType

/**
  * Sample code for elasticsearch with scio
  * sbt "runMain example.WordCount
  * --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  * --input=gs://dataflow-samples/shakespeare/kinglear.txt
  * --output=gs://[BUCKET]/[PATH]/wordcount"
  */
object Elastic {

  val indexR = (s: String) => {
    val request = new IndexRequest("posts")
    request.id("1")
    val jsonString
      : String = "{" + "\"user\":\"" + s.substring(2, 5) + "\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"trying out Elasticsearch\"" + "}"
    Iterable(request.source(jsonString, XContentType.JSON))
  }

  def main(cliArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cliArgs)
    val clus = new HttpHost("localhost", 9200)
    val servers: Seq[HttpHost] = Seq(clus)
    val esClusterOptions = ElasticsearchOptions(servers)
    val es = ElasticsearchIO(esClusterOptions)

    val input = args.getOrElse("input", "gs://k-scio-dev/wordCount.txt")

    val myText: SCollection[String] = sc.textFile(input)

    val coll = ElasticsearchSCollection(myText)

    coll.saveAsElasticsearch(esClusterOptions)(indexR)

    val result = sc.run().waitUntilFinish()

    println("Woo hoo")

  }

}