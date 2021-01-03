package com.fairlink.core.interface

import com.fairlink.infrastructure.interface.{TableReader, TableWriter}

trait Job {
  val reader: TableReader
  val processor: Processor
  val writer: TableWriter
  def run(): Unit
}
