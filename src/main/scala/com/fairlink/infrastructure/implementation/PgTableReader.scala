package com.fairlink.infrastructure.implementation

import com.fairlink.infrastructure.interface.TableReader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class PgTableReader(sparkSession: SparkSession, host: String, db: String, user: String, pass: String) extends TableReader {
  private val format = "jdbc"
  private val connStr = s"jdbc:postgresql://${host}:5432/${db}"
  private val driver = "org.postgresql.Driver"

  override def read(tableName: String): DataFrame = {
    sparkSession.sqlContext
      .read
      .format(format)
      .option("url", connStr)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", pass)
      .option("driver", driver)
      .load
  }
}
