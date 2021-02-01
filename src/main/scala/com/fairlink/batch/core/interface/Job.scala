package com.fairlink.batch.core.interface

import com.fairlink.batch.infrastructure.interface.{TableReader, TableWriter}

trait Job {
  val reader: TableReader
  val processor: Processor
  val writer: TableWriter
  def run(): Unit
}
