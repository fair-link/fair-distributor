package com.fairlink.infrastructure.interface

import org.apache.spark.sql.DataFrame

trait TableReader {

  def read(tn: String): DataFrame

}
