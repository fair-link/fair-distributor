package com.fairlink.core.interface

import org.apache.spark.sql.DataFrame

trait Processor {
  def process(dfs: Map[String,DataFrame]): DataFrame
}
