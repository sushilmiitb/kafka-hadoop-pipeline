import com.chymeravr.dfs.records.{HourlyDimension, Metrics}
import com.chymeravr.schemas.kafka.AttributedEvent

import scalaj.http.{Http, HttpOptions}

/**
  * Created by rubbal on 10/2/17.
  */
object PlacementEventAggregator extends AbstractAggregator {
  override def getId(attributedEvent: AttributedEvent): String = {
    attributedEvent.servingLog.placementId
  }

  override def process(records: Iterator[(HourlyDimension, Metrics)]): Unit = {
    val result = Http("https://requestb.in/qqcr4vqq").postData(s"""${records.mkString}""")
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .option(HttpOptions.readTimeout(10000)).asString
  }
}
