package com.fairlink.batch.constants

object Constants {

  val DATE_FORMAT: String = "yyyy-MM-dd"

  //Db_configurations
  val DB_HOST: String = "0.0.0.0"
  val DB_NAME: String = "postgres"
  val DB_USER: String =  "admin"
  val DB_PASS: String = "secret"

  //Table names
  val TABLE_NAME_ARTIGOS_DISPONIVEIS: String = "disponibilidade"
  val TABLE_NAME_CABAZ: String = "categoria_cabaz"
  val TABLE_NAME_CATALOGO: String = "catalogo"
  val TABLE_NAME_CANDIDATOS: String = "individuo_carenciado"
  val PROPOSTA_TABLE_NAME: String = "proposta"
  val DONATIVO_TABLE_NAME: String = "donativo"

  // DF KEYS
  val DONATIVOS_DF_KEY = "donativo"
  val CATALOGO_DF_KEY = "catalogo"
  val CABAZ_DF_KEY = "cabaz"
  val ARTIGOS_DISPONIVEIS_DF_KEY = "artigos_disponiveis"
  val CANDIDATOS_DF_KEY = "candidatos"
}
