package com.fairlink.batch.core.implementation

import com.fairlink.batch.constants.Constants
import com.fairlink.batch.core.interface.Processor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DonativosProcessor(spark: SparkSession, odate: String) extends Processor {
  override def process(dfs: Map[String, DataFrame]): DataFrame = {

    val artigosDisponiveis = dfs.apply(Constants.ARTIGOS_DISPONIVEIS_DF_KEY)
    val cabaz = dfs.apply(Constants.CABAZ_DF_KEY)
    .withColumnRenamed("quantidade", "quantidade_cabaz")
    val catalogo = dfs.apply(Constants.CATALOGO_DF_KEY)
    val candidatos = dfs.apply(Constants.CANDIDATOS_DF_KEY)

    val windowSumCat = Window.partitionBy("categoria").orderBy("artigo_disp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val artigosDisponiveisDia = artigosDisponiveis
      .where(col("disponibilidade_date") === lit(odate))
      .join(catalogo, col("artigo_disp") === col("artigo_catalogo"), "inner")
      .orderBy(col("categoria").asc, col("artigo_disp"))
      .join(cabaz, Seq("categoria"), "inner")
      .withColumn("soma_quantidade_categoria", sum("quantidade").over(windowSumCat))

    val windowRend = Window.orderBy("rendimento_per_capita")
    val windowSumDemand = Window.partitionBy("categoria").orderBy("ordem").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val somaDemandasCandidato = candidatos
      .withColumn("rendimento_per_capita", col("rendimento_agregado").divide(col("numero_elementos")))
      .withColumn("ordem", row_number.over(windowRend))
      .withColumn("id_donativo", monotonically_increasing_id())
      .select(col("nome"), col("telefone"), col("rendimento_per_capita"), col("numero_elementos"), col("ordem"), col("id_donativo"), col("id_ipss"))
      .join(cabaz)
      .withColumn("demanda", col("numero_elementos").multiply(col("quantidade_cabaz")))
      .withColumn("soma_demanda", sum("demanda").over(windowSumDemand))
      .withColumn("soma_demanda_antes", col("soma_demanda").minus(col("demanda")))

    val candidatosArtigosDisponiveis = artigosDisponiveisDia.join(somaDemandasCandidato, Seq("categoria"))

    import spark.implicits._

    val donativos = candidatosArtigosDisponiveis
      .withColumn("atribuido",
        when(col("soma_quantidade_categoria").minus(col("soma_demanda")).geq(lit(0)) and col("soma_quantidade_categoria").minus(col("quantidade")).leq(col("soma_demanda")), lit("yes"))
          .when(col("soma_quantidade_categoria").between(col("soma_demanda_antes"), col("soma_demanda")), lit("yes"))
          .otherwise(lit("no")))
      .where("atribuido = 'yes'")
      .withColumn("quantidade_atribuida",
        when(col("soma_quantidade_categoria").minus(col("quantidade")).between(col("soma_demanda_antes"), col("soma_demanda")) and col("demanda").gt(col("quantidade")), col("quantidade")) // pessego josé
          .when(col("soma_quantidade_categoria").minus(col("quantidade")).between(col("soma_demanda_antes"), col("soma_demanda")), col("soma_demanda").minus(col("soma_quantidade_categoria").minus(col("quantidade")))) // pessego josé
          .when(col("soma_quantidade_categoria").between(col("soma_demanda_antes"), col("soma_demanda")), col("soma_quantidade_categoria").minus(col("soma_demanda_antes"))) // antonio jose maçã ou robalo manuel
          .otherwise(lit(0)))
      .withColumn("estado", lit("Ativo"))
      .orderBy("ordem", "categoria")
      .select($"id_donativo", $"nome".as("ind_carenciado_nome"), $"telefone", $"id_ipss", $"supermercado".as("id_gs"), $"estado", $"artigo_disp", $"quantidade_atribuida".as("quantidade"), $"expiration_date", $"disponibilidade_date")

    donativos
  }
}
