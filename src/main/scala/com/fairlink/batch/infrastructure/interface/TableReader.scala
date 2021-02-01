package com.fairlink.batch.infrastructure.interface

import org.apache.spark.sql.DataFrame

trait TableReader {

  def read(tn: String): DataFrame

}
