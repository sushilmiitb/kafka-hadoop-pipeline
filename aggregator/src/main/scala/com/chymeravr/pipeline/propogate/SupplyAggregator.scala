package com.chymeravr.pipeline.propogate

import java.sql.{BatchUpdateException, Connection, DriverManager}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by rubbal on 13/2/17.
  */
class SupplyAggregator(analyticsDbHost: String, analyticsDbPort: Int, analyticsDbName: String, analyticsUserName: String, analyticsPassword: String,
                       ciDbHost: String, ciDbPort: Int, ciDbName: String, ciDbuserName: String, ciDbPassword: String) {

  class PlacementObject(val placementId: String, val appId: String, val userId: String)

  class MetricObject(val impressions: Int, val clicks: Int, val earnings: Double)

  def propogatePlacementMetrics(): Unit = {
    Try {
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(f"jdbc:postgresql://$analyticsDbHost:$analyticsDbPort/$analyticsDbName", analyticsUserName, analyticsPassword)
      val placementMetricsMap: mutable.HashMap[PlacementObject, MetricObject] = getPlacementMetrics(connection)

      def metricReduceFunc(a: MetricObject, b: MetricObject): MetricObject = {
        new MetricObject(a.impressions + b.impressions,
          a.clicks + b.clicks,
          a.earnings + b.earnings)
      }

      // Group by relevant key
      // Map to key, list(metrics).reduce()
      var placementMetrics = placementMetricsMap.toList.groupBy(x => x._1.placementId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))
      var appMetrics = placementMetricsMap.toList.groupBy(x => x._1.appId).
        map(x => (x._1, x._2.map(x => x._2).reduce(metricReduceFunc)))

      updateMetrics(placementMetrics, "publisher_placement")
      updateMetrics(appMetrics, "publisher_app")
      
    } match {
      case Success(_) =>
      case Failure(ex: BatchUpdateException) => ex.getNextException.printStackTrace()
      case Failure(ex) => ex.printStackTrace()
    }
  }

  private def getPlacementMetrics(connection: Connection) = {
    var selectMetrics = connection.prepareStatement(
      """
        SELECT
          id,
          sum(impressions) as impressions,
          sum(clicks) as clicks,
          sum(amount) as earnings
        FROM placement_metrics
        GROUP BY
          id
      """)

    val resultSet = selectMetrics.executeQuery()
    val placementMetricMap = new mutable.HashMap[PlacementObject, MetricObject]()
    val placementMeta = fetchPlacementMeta()
    while (resultSet.next()) {
      placementMetricMap.put(
        placementMeta(resultSet.getString("id")),
        new MetricObject(
          resultSet.getInt("impressions"),
          resultSet.getInt("clicks"),
          resultSet.getDouble("earnings")
        ))
    }
    placementMetricMap
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
          earnings = ?
        WHERE
          id::text = ?
      """
    )
    metrics.foreach(x => {
      val metrics = x._2
      statement.setLong(1, metrics.impressions)
      statement.setLong(2, metrics.clicks)
      statement.setDouble(3, metrics.earnings)
      statement.setString(4, x._1)
      statement.addBatch()
    })
    statement.executeBatch()
    connection.commit()
  }

  private def fetchPlacementMeta(): Map[String, PlacementObject] = {
    val connection = createCiDbConnection()
    val selectMeta = connection.prepareStatement(
      """
       SELECT app.id as app_id, pl.id as placement_id, chym_user.id as user_id
       FROM publisher_app app, publisher_placement pl, chym_user_profile chym_user
       WHERE pl.app_id = app.id AND app.user_id = chym_user.user_id;
      """

    )
    val resultSet = selectMeta.executeQuery()
    val placementMetaMap = new mutable.HashMap[String, PlacementObject]()
    while (resultSet.next()) {
      placementMetaMap.put(resultSet.getString("placement_id"),
        new PlacementObject(
          resultSet.getString("placement_id"),
          resultSet.getString("app_id"),
          resultSet.getString("user_id")
        )
      )
    }
    placementMetaMap.toMap
  }

  private def createCiDbConnection(): Connection = {
    DriverManager.getConnection(f"jdbc:postgresql://$ciDbHost:$ciDbPort/$ciDbName", ciDbuserName, ciDbPassword)
  }
}
