package com.fairlink.batch.infrastructure.exception

case class PgTableException(msg: String) extends Exception(msg)
