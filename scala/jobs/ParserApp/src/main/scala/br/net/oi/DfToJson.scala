package br.net.oi
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DfToJson extends App {
  val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
  val conf = new SparkConf().setAppName("ParserAppMobile")
  val sc = new SparkContext(conf)
  val hc = new HiveContext(sc)

  val nameSave = "resposta-"

  val today = Calendar.getInstance.getTime
  val dataFormat = new SimpleDateFormat("yyyy-MM-dd")
  val currentData = dataFormat.format(today)

  //PATCH
  //HDFS
  val pathHiveIn = "/oi/user_work/hub/hive/wrk_hub_database/saida/export_history_posh_app/"
  val pathHiveOut = "/oi/user_work/hub/hive/wrk_hub_database/saida/export_history_posh_app/"
  //Name file Input
  val nameFileIn1 = "NASA_access_log_Aug95"
  val nameFileIn2 = "NASA_access_log_Jul95"
  //Name file output
  val nameFileIn1Out = "NASA_access_log_Aug95_Out-"
  val nameFileIn2Out = "NASA_access_log_Jul95_Out-"

  val file_name1 = pathHiveIn+nameFileIn1
  val file_name2 = pathHiveIn+nameFileIn2
  val file_name1_out = pathHiveIn+nameFileIn1Out+currentData
  val file_name2_out = pathHiveIn+nameFileIn2Out+currentData

  //Escolha qual file a ser utilziado.
  val env = "Aug95"
  var file_name_in = ""
  var file_name_out = ""

  env match {
    case "Aug95" =>
      file_name_in = file_name1
      file_name_out = file_name1_out
    case "Jul95" =>
      file_name_in = file_name2
      file_name_out = file_name2_out
  }

  //inicio
  //NASA_access_log_Aug95
  //Carregando os dados do HDFS
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  var hdfs_lines = sc.textFile(file_name_in)
  hdfs_lines = hdfs_lines.map(x => x.replace("|", "__"))
  hdfs_lines = hdfs_lines.map(x => x.replace("\" HTTP", "HTTP"))
  hdfs_lines = hdfs_lines.map(x => x.replace(" - - [", "|"))
  hdfs_lines = hdfs_lines.map(x => x.replace("] \"", "|"))
  hdfs_lines = hdfs_lines.map(x => x.replace("\" ", "|"))
  hdfs_lines = hdfs_lines.map(x => x.replace("alyssa.p", ""))
  val squidHeader = "Host Datahora Requisicao Codigo_http_bytes"
  val schema = StructType(squidHeader.split(" ").map(fieldName => StructField(fieldName,StringType, true)))
  val rowRDD = hdfs_lines.map(_.split("|")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
  val squidDF = sqlContext.createDataFrame(rowRDD, schema)
  squidDF.registerTempTable("nasa")
  val df_full = sqlContext.sql("select * from nasa")

  //Criando Df
  val df1 = df_full.withColumn("Codigo_http", split(col("Codigo_http_bytes"), " ").getItem(0)
  ).withColumn("Total_bytes", split(col("Codigo_http_bytes")," ").getItem(1)
  ).select(
    col("Host"),
    col("Datahora"),
    col("Requisicao"),
    col("Codigo_http"),
    col("Total_bytes")
  ).withColumn("Total_bytes", when(col("Total_bytes")==="-","0"
  ).when(col("Total_bytes")==="","0"
  ).when(col("Total_bytes")===" ","0"
  ).when(col("Total_bytes").isNull(),"0"
  ).otherwise(col("Total_bytes"))
  )

  //Número de hosts únicos.
  val df_host_unicos = df1.select(col("Host")).distinct().count()

  //Gravando o arquivo no hdfs
  var finalRDD = sc.parallelize(Seq(df_host_unicos))
  var saida1 = file_name_out+"-df_host_unicos"
  finalRDD.coalesce(numPartitions = 1, shuffle = true).saveAsTextFile(saida1)

  //O total de erros 404.
  val df_error_404 = df1.select(
    col("Codigo_http")
  ).filter(
    col("Codigo_http")==="404"
  ).groupBy(
    col("Codigo_http")
  ).agg(
    count(col("Codigo_http")).alias("qtd_error")
  )

  //Gravando o arquivo no hdfs
  saida1 = file_name_out+"-df_error_404"
  df_error_404.coalesce(1).write.format("json").mode("overwrite").save(saida1)

  //Os 5 URLs que mais causaram erro 404.
   val df_url_erro = df1.filter(
     col("Codigo_http")==="404"
   ).select(
     col("Host")
   ).groupBy(
     col("Host")
   ).agg(
     count(col("Host")).alias("qtd_host")
   ).sort(
     col("qtd_host").desc
   ).limit(5)

  //Gravando o arquivo no hdfs
  saida1 = file_name_out+"-df_url_erro"
  df_url_erro.coalesce(1).write.format("json").mode("overwrite").save(saida1)

  //Quantidade de erros 404 por dia.
  val df_trunc_date = df1.filter(
    col("Codigo_http")==="404"
  ).withColumn(
    "data", unix_timestamp(col("Datahora"),"dd/MMM/yyyy"
    ).cast(
      "double"
    ).cast(
      "timestamp")
  ).select(
    col("data"),
    col("Codigo_http")
  ).groupBy(
    col("data")
  ).agg(
    count(col("Codigo_http")).alias("qtd_erro")
  )

  //Gravando o arquivo no hdfs
  saida1 = file_name_out+"-df_trunc_date"
  df_trunc_date.coalesce(1).write.format("json").mode("overwrite").save(saida1)


  //O total de bytes retornados.
  var df_total_bytes = df1.select(
    col("Total_bytes")
  ).agg(sum(
    col("Total_bytes")
  ).alias("Qtd_bytes"))

  //Gravando o arquivo no hdfs
  saida1 = file_name_out+"-df_total_bytes"
  df_total_bytes.coalesce(1).write.format("json").mode("overwrite").save(saida1)

}