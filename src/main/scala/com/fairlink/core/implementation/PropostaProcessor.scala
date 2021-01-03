package com.fairlink.core.implementation

import com.fairlink.constants.Constants
import com.fairlink.core.interface.Processor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{current_date, lit, monotonically_increasing_id, sum}

case class PropostaProcessor(spark: SparkSession) extends Processor {
  override def process(dfs: Map[String, DataFrame]): DataFrame = {

    import spark.implicits._
    val donativos = dfs.apply(Constants.DONATIVOS_DF_KEY)

    val propostasGsIpss = donativos
      .groupBy("id_ipss", "id_gs", "artigo_disp", "expiration_date")
      .agg(sum("quantidade").as("qtd"))
      .withColumn("data_referencia", current_date())
      .withColumn("estado_prop",lit("No Armazém"))
      .select($"artigo_disp".as("artigo_proposta"), $"qtd", $"id_gs".as("gs_id"), $"expiration_date",$"data_referencia", $"estado_prop".as("estado"), $"id_ipss")


    val propIds = propostasGsIpss.select("gs_id", "id_ipss").distinct()
      .withColumn("id_proposta", monotonically_increasing_id())

    propostasGsIpss.
      join(propIds, Seq("gs_id", "id_ipss"), "inner")
  }
}
