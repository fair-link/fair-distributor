package com.fairlink.infrastructure.exception

case class PgTableException(msg: String) extends Exception(msg)
