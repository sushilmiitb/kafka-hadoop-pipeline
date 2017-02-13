import com.chymeravr.dfs.records.{HourlyDimension, Metrics}
import com.chymeravr.schemas.kafka.AttributedEvent
import com.chymeravr.schemas.serving.ImpressionInfo

/**
  * Created by rubbal on 10/2/17.
  */
object PlacementEventAggregator extends AbstractAggregator {
  override def getId(attributedEvent: AttributedEvent): String = {
    attributedEvent.servingLog.placementId
  }

  override def process(records: Iterator[(HourlyDimension, Metrics)]): Unit = {
    val processor = new DbProcessor("13.93.217.168", 5432, "analytics", "analytics", "an@lytics")
    processor.insertOrUpdate(records, "placement_metrics")
  }

  override def getAmount(impressionInfo: ImpressionInfo): Double = impressionInfo.sellingPrice
}
