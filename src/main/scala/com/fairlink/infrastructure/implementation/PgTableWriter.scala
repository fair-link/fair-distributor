package com.fairlink.infrastructure.implementation

import com.fairlink.infrastructure.exception.PgTableException
import com.fairlink.infrastructure.interface.TableWriter
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class PgTableWriter(sparkSession: SparkSession, host: String, db: String, user: String, pass: String) extends TableWriter {
  private val format = "jdbc"
  private val connStr = s"jdbc:postgresql://${host}:5432/${db}"

  override def write(df: DataFrame, tn: String): Unit = {
    try {
      df.write
        .format(format)
        .option("url", connStr)
        .option("dbtable", tn)
        .option("user", user)
        .option("password", pass)
        .mode(SaveMode.Append)
        .save()
    } catch {
      case bue: SparkException => throw PgTableException("One or more lines of the dataframe violate primary key (artigo_disp, supermercado, expiration_date, disponibilidade_date)")
    }
  }
}
