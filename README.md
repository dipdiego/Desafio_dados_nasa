# Projeto
Qual o objetivo do comando cache em Spark?
Ajudar na performace do código, pois permite que os resultados de oparações lazy possam ser armazenados e reutilizados repetidamente.

O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?
Sim, devido os códigos em spark rodarem na memoria e mapreduce grava em disco gerando mais tempo de I/O.

Qual é a função do SparkContext?
Permite a aplicativo Spark acesse o Spark Cluster com a ajuda do Resource Manager. 

Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
São conjutos de objetos distribuido entre os nós do cluster que são geralmente executado em memória.

GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
Sim, devido a necessidade de utilizar mais dados do que reduceByKey que cria partições com valores iguais e disponibilizando apenas uma valor por partição.

Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...") 
#Lê o arquivo que está no HDFS
val counts = textFile.flatMap(line => line.split(" "))
#Separa com a "espaço" como delimitador
.map(word => (word, 1))
#Grava todas a palavras 
.reduceByKey(_ + _)
#Agrega as palavras em partição com apenas uma saida de cada palavra, diminuindo a quantidade de dados trafegados.
counts.saveAsTextFile("hdfs://...")
#Salvando o conteúdo no HDFS.




############NASA_access_log_Aug95
1. Número de hosts únicos
75060

2. O total de erros 404.
{"Codigo_http":"404","qtd_error":10056}

3. Os 5 URLs que mais causaram erro 404.
{"Host":"dialip-217.den.mmc.com","qtd_host":62}
{"Host":"piweba3y.prodigy.com","qtd_host":47}
{"Host":"155.148.25.4","qtd_host":44}
{"Host":"maz3.maz.net","qtd_host":39}
{"Host":"gate.barr.com","qtd_host":38}

4. Quantidade de erros 404 por dia.
{"data":"1995-08-01 00:00:00.0","qtd_erro":243}
{"data":"1995-08-03 00:00:00.0","qtd_erro":304}
{"data":"1995-08-04 00:00:00.0","qtd_erro":346}
{"data":"1995-08-05 00:00:00.0","qtd_erro":236}
{"data":"1995-08-06 00:00:00.0","qtd_erro":373}
{"data":"1995-08-07 00:00:00.0","qtd_erro":537}
{"data":"1995-08-08 00:00:00.0","qtd_erro":391}
{"data":"1995-08-09 00:00:00.0","qtd_erro":279}
{"data":"1995-08-10 00:00:00.0","qtd_erro":315}
{"data":"1995-08-11 00:00:00.0","qtd_erro":263}
{"data":"1995-08-12 00:00:00.0","qtd_erro":196}
{"data":"1995-08-13 00:00:00.0","qtd_erro":216}
{"data":"1995-08-14 00:00:00.0","qtd_erro":287}
{"data":"1995-08-15 00:00:00.0","qtd_erro":327}
{"data":"1995-08-16 00:00:00.0","qtd_erro":259}
{"data":"1995-08-17 00:00:00.0","qtd_erro":271}
{"data":"1995-08-18 00:00:00.0","qtd_erro":256}
{"data":"1995-08-19 00:00:00.0","qtd_erro":209}
{"data":"1995-08-20 00:00:00.0","qtd_erro":312}
{"data":"1995-08-21 00:00:00.0","qtd_erro":305}
{"data":"1995-08-22 00:00:00.0","qtd_erro":288}
{"data":"1995-08-23 00:00:00.0","qtd_erro":345}
{"data":"1995-08-24 00:00:00.0","qtd_erro":420}
{"data":"1995-08-25 00:00:00.0","qtd_erro":415}
{"data":"1995-08-26 00:00:00.0","qtd_erro":366}
{"data":"1995-08-27 00:00:00.0","qtd_erro":370}
{"data":"1995-08-28 00:00:00.0","qtd_erro":410}
{"data":"1995-08-29 00:00:00.0","qtd_erro":420}
{"data":"1995-08-30 00:00:00.0","qtd_erro":571}
{"data":"1995-08-31 00:00:00.0","qtd_erro":526}

5. O total de bytes retornados.
{"Qtd_bytes":2.6828341424E10}






############NASA_access_log_Jul95
1. Número de hosts únicos
81983

2. O total de erros 404.
{"Codigo_http":"404","qtd_error":10845}

3. Os 5 URLs que mais causaram erro 404.
{"Host":"hoohoo.ncsa.uiuc.edu","qtd_host":251}
{"Host":"jbiagioni.npt.nuwc.navy.mil","qtd_host":131}
{"Host":"piweba3y.prodigy.com","qtd_host":110}
{"Host":"piweba1y.prodigy.com","qtd_host":92}
{"Host":"phaelon.ksc.nasa.gov","qtd_host":64}

4. Quantidade de erros 404 por dia.
{"data":"1995-07-01 00:00:00.0","qtd_erro":316}
{"data":"1995-07-02 00:00:00.0","qtd_erro":291}
{"data":"1995-07-03 00:00:00.0","qtd_erro":474}
{"data":"1995-07-04 00:00:00.0","qtd_erro":359}
{"data":"1995-07-05 00:00:00.0","qtd_erro":497}
{"data":"1995-07-06 00:00:00.0","qtd_erro":640}
{"data":"1995-07-07 00:00:00.0","qtd_erro":570}
{"data":"1995-07-08 00:00:00.0","qtd_erro":302}
{"data":"1995-07-09 00:00:00.0","qtd_erro":348}
{"data":"1995-07-10 00:00:00.0","qtd_erro":398}
{"data":"1995-07-11 00:00:00.0","qtd_erro":471}
{"data":"1995-07-12 00:00:00.0","qtd_erro":471}
{"data":"1995-07-13 00:00:00.0","qtd_erro":532}
{"data":"1995-07-14 00:00:00.0","qtd_erro":413}
{"data":"1995-07-15 00:00:00.0","qtd_erro":254}
{"data":"1995-07-16 00:00:00.0","qtd_erro":257}
{"data":"1995-07-17 00:00:00.0","qtd_erro":406}
{"data":"1995-07-18 00:00:00.0","qtd_erro":465}
{"data":"1995-07-19 00:00:00.0","qtd_erro":639}
{"data":"1995-07-20 00:00:00.0","qtd_erro":428}
{"data":"1995-07-21 00:00:00.0","qtd_erro":334}
{"data":"1995-07-22 00:00:00.0","qtd_erro":192}
{"data":"1995-07-23 00:00:00.0","qtd_erro":233}
{"data":"1995-07-24 00:00:00.0","qtd_erro":328}
{"data":"1995-07-25 00:00:00.0","qtd_erro":461}
{"data":"1995-07-26 00:00:00.0","qtd_erro":336}
{"data":"1995-07-27 00:00:00.0","qtd_erro":336}
{"data":"1995-07-28 00:00:00.0","qtd_erro":94}

5. O total de bytes retornados.
{"Qtd_bytes":3.8695973491E10}
