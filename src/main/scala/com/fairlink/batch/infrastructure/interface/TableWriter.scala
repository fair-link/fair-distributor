package com.fairlink.batch.infrastructure.interface

import org.apache.spark.sql.DataFrame

trait TableWriter {
  def write(df: DataFrame, tn: String): Unit
}
