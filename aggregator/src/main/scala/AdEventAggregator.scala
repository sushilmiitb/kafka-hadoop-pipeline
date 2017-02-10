import com.chymeravr.schemas.kafka.AttributedEvent

/**
  * Created by rubbal on 10/2/17.
  */
object AdEventAggregator extends AbstractAggregator {
  override def getId(attributedEvent: AttributedEvent): String = {
    attributedEvent.servingLog.impressionInfo.adId
  }
}
