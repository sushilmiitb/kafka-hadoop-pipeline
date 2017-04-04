package com.chymeravr.pipeline.propogate

import java.sql.{BatchUpdateException, Connection, DriverManager}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by rubbal on 13/2/17.
  */
class DemandAggregator(analyticsDbHost: String, analyticsDbPort: Int, analyticsDbName: String, analyticsUserName: String, analyticsPassword: String, ciDbHost: String, ciDbPort: Int, ciDbName: String, ciDbuserName: String, ciDbPassword: String) {

  class AdObject(val adId: String, val adgroupId: String, val campaignId: String, val userId: String)

  class MetricObject(val impressions: Int, val clicks: Int, val errors: Int, val closes: Int, val burn: Double)

  def propagateAdAggregations(): Unit = {
    Try {
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(f"jdbc:postgresql://$analyticsDbHost:$analyticsDbPort/$analyticsDbName", analyticsUserName, analyticsPassword)
      val adMetricMap: mutable.HashMap[AdObject, MetricObject] = getAdMetrics(connection)

      def metricReduceFunc(a: MetricObject, b: MetricObject): MetricObject = {
        new MetricObject(a.impressions + b.impressions,
          a.clicks + b.clicks,
          a.errors + b.errors,
          a.closes + b.closes,
          a.burn + b.burn)
      }

      // Group by relevant key
      // Map to key, list(metrics).reduce()
      var adMetric = adMetricMap.toList.groupBy(x => x._1.adId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))
      var adgroupMetric = adMetricMap.toList.groupBy(x => x._1.adgroupId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))
      var campaignMetric = adMetricMap.toList.groupBy(x => x._1.campaignId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))
      var advertiserMetrics = adMetricMap.toList.groupBy(x => x._1.userId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))

      updateMetrics(adMetric, "advertiser_ad")
      updateMetrics(adgroupMetric, "advertiser_adgroup")
      updateMetrics(campaignMetric, "advertiser_campaign")
    } match {
      case Success(_) =>
      case Failure(ex: BatchUpdateException) => ex.getNextException.printStackTrace()
      case Failure(ex) => ex.printStackTrace()
    }
  }

  private def getAdMetrics(connection: Connection) = {
    var selectMetrics = connection.prepareStatement(
      """
        SELECT
          id,
          sum(impressions) as impressions,
          sum(clicks) as clicks,
          sum(errors) as errors,
          sum(closes) as closes,
          sum(amount) as burn
        FROM ad_metrics
        GROUP BY
          id
      """.stripMargin)

    val resultSet = selectMetrics.executeQuery()
    val adMetricMap = new mutable.HashMap[AdObject, MetricObject]()
    val adMeta = fetchAdMeta()
    while (resultSet.next()) {
      try {
        adMetricMap.put(
          adMeta(resultSet.getString("id")),
          new MetricObject(
            resultSet.getInt("impressions"),
            resultSet.getInt("clicks"),
            resultSet.getInt("errors"),
            resultSet.getInt("closes"),
            resultSet.getDouble("burn")
          ))
      } catch {
        case e: Throwable => e.printStackTrace();
      }
    }
    adMetricMap
  }

  private def updateMetrics(metrics: Iterable[(String, MetricObject)], tableName: String): Unit = {
    val connection = createCiDbConnection()
    connection.setAutoCommit(false)
    val statement = connection.prepareStatement(
      s"""
        UPDATE $tableName
        SET
          impressions = ?,
          clicks = ?,
          "totalBurn" = ?
        WHERE
          id::text = ?
      """
    )
    metrics.foreach(x => {
      val metrics = x._2
      statement.setLong(1, metrics.impressions)
      statement.setLong(2, metrics.clicks)
      statement.setDouble(3, metrics.burn)
      statement.setString(4, x._1)
      statement.addBatch()
    })
    statement.executeBatch()
    connection.commit()
  }

  private def fetchAdMeta(): Map[String, AdObject] = {
    val connection = createCiDbConnection()
    val selectMeta = connection.prepareStatement(
      """
        SELECT chym_user.id AS user_id, ad.id AS ad_id, ad.adgroup_id AS adgroup_id, cmp.id AS campaign_id
        FROM advertiser_ad ad, advertiser_adgroup ag, advertiser_campaign cmp, chym_user_profile chym_user
        WHERE (ad.status or ad.modified_date > now() - interval '10 days') AND ad.adgroup_id = ag.id AND ag.campaign_id = cmp.id AND cmp.user_id = chym_user.user_id
      """

    )
    val resultSet = selectMeta.executeQuery()
    val adMetaMap = new mutable.HashMap[String, AdObject]()
    while (resultSet.next()) {
      adMetaMap.put(resultSet.getString("ad_id"),
        new AdObject(
          resultSet.getString("ad_id"),
          resultSet.getString("adgroup_id"),
          resultSet.getString("campaign_id"),
          resultSet.getString("user_id")
        )
      )
    }
    adMetaMap.toMap
  }

  private def createCiDbConnection(): Connection = {
    DriverManager.getConnection(f"jdbc:postgresql://$ciDbHost:$ciDbPort/$ciDbName", ciDbuserName, ciDbPassword)
  }
}
