package com.fairlink.batch.core.implementation

import com.fairlink.batch.constants.Constants
import com.fairlink.batch.core.interface.{Job, Processor}
import com.fairlink.batch.infrastructure.implementation.{PgTableReader, PgTableWriter}
import com.fairlink.batch.infrastructure.interface.{TableReader, TableWriter}
import org.apache.spark.sql.SparkSession

case class FairDistributorJob(private val odate: String) extends Job {
  private val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  override val reader: TableReader = PgTableReader(spark, Constants.DB_HOST, Constants.DB_NAME, Constants.DB_USER, Constants.DB_PASS)
  override val processor: Processor = DonativosProcessor(spark, odate)
  override val writer: TableWriter = PgTableWriter(spark, Constants.DB_HOST, Constants.DB_NAME, Constants.DB_USER, Constants.DB_PASS)
  private val propostasProcessor: PropostaProcessor = PropostaProcessor(spark)

  override def run(): Unit = {

    val dfs = Map(
      Constants.CATALOGO_DF_KEY -> reader.read(Constants.TABLE_NAME_CATALOGO),
      Constants.CABAZ_DF_KEY -> reader.read(Constants.TABLE_NAME_CABAZ),
      Constants.CANDIDATOS_DF_KEY -> reader.read(Constants.TABLE_NAME_CANDIDATOS),
      Constants.ARTIGOS_DISPONIVEIS_DF_KEY -> reader.read(Constants.TABLE_NAME_ARTIGOS_DISPONIVEIS)
    )

    val donativos = processor.process(dfs)

    writer.write(donativos, Constants.DONATIVO_TABLE_NAME)

    val propostas = propostasProcessor.process(Map(Constants.DONATIVOS_DF_KEY -> donativos))

    writer.write(propostas, Constants.PROPOSTA_TABLE_NAME)
  }
}
