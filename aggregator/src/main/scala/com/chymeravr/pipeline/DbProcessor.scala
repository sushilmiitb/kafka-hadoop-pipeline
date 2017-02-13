package com.chymeravr.pipeline

import java.sql.{BatchUpdateException, DriverManager}

import com.chymeravr.dfs.records.{HourlyDimension, Metrics}

import scala.util.{Failure, Try}

/**
  * Created by rubbal on 13/2/17.
  */
class DbProcessor(dbHost: String, dbPort: Int, dbName: String, userName: String, password: String) {

  def insertOrUpdate(records: Iterator[(HourlyDimension, Metrics)], tableName: String): Unit = {
    Try {
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(f"jdbc:postgresql://$dbHost:$dbPort/$dbName", userName, password)
      connection.setAutoCommit(false)
      val statement = connection.prepareStatement(
        f"""
        INSERT INTO
          $tableName(id, year, month, day, hour, impressions, clicks, errors, closes, amount, modified_on)
          VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
          ON CONFLICT(id, year, month, day, hour) DO
            UPDATE
            SET impressions = ?,
                clicks = ?,
                errors = ?,
                closes = ?,
                amount = ?,
                modified_on = now()
      """.stripMargin)

      records.foreach(x => {
        val dimension = x._1
        val metrics = x._2
        statement.setString(1, dimension.getObjectId)
        statement.setShort(2, dimension.getTs.year)
        statement.setShort(3, dimension.getTs.month)
        statement.setShort(4, dimension.getTs.day)
        statement.setShort(5, dimension.getTs.hour)
        statement.setLong(6, metrics.impressions)
        statement.setLong(7, metrics.clicks)
        statement.setLong(8, metrics.errors)
        statement.setLong(9, metrics.close)
        statement.setDouble(10, metrics.amount)

        statement.setLong(11, metrics.impressions)
        statement.setLong(12, metrics.clicks)
        statement.setLong(13, metrics.errors)
        statement.setLong(14, metrics.close)
        statement.setDouble(15, metrics.amount)

        statement.addBatch()
      })

      statement.executeBatch()
      connection.commit()
    } match {
      case Failure(ex: BatchUpdateException) =>
        val exceptions = ex.iterator()
        while (exceptions.hasNext) {
          println(exceptions.next())
        }
      case x => x
    }
  }
}
