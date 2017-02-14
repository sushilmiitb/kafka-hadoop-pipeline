package com.chymeravr.pipeline.aggregator

import com.chymeravr.dfs.records.{HourlyDimension, Metrics}
import com.chymeravr.pipeline.DbProcessor
import com.chymeravr.pipeline.propogate.SupplyAggregator
import com.chymeravr.schemas.kafka.AttributedEvent
import com.chymeravr.schemas.serving.ImpressionInfo

/**
  * Created by rubbal on 10/2/17.
  */
object PlacementEventAggregator extends AbstractAggregator {
  val dbHost = "13.93.217.168"
  val port = 5432

  override def getId(attributedEvent: AttributedEvent): String = {
    attributedEvent.servingLog.placementId
  }

  override def processParallel(records: Iterator[(HourlyDimension, Metrics)]): Unit = {
    val processor = new DbProcessor(dbHost, port, "analytics", "analytics", "an@lytics")
    processor.insertOrUpdate(records, "placement_metrics")
  }

  override def getAmount(impressionInfo: ImpressionInfo): Double = impressionInfo.sellingPrice

  override def postProcessing(): Unit = {
    val supplyAggregator = new SupplyAggregator(dbHost, port, "analytics", "analytics", "an@lytics",
      dbHost, port, "ciportal", "ciportaluser", "ciportal")
    supplyAggregator.propogatePlacementMetrics()
  }
}
