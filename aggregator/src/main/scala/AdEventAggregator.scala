import com.chymeravr.dfs.records.{HourlyDimension, Metrics}
import com.chymeravr.schemas.kafka.AttributedEvent
import com.chymeravr.schemas.serving.ImpressionInfo

/**
  * Created by rubbal on 10/2/17.
  */
object AdEventAggregator extends AbstractAggregator {
  override def getId(attributedEvent: AttributedEvent): String = {
    attributedEvent.servingLog.impressionInfo.adId
  }

  override def process(records: Iterator[(HourlyDimension, Metrics)]): Unit = {
    val processor = new DbProcessor("13.93.217.168", 5432, "analytics", "analytics", "an@lytics")
    processor.insertOrUpdate(records, "ad_metrics")
  }

  override def getAmount(impressionInfo: ImpressionInfo): Double = impressionInfo.costPrice

}
