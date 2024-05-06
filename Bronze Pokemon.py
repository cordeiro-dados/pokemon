# Databricks notebook source
(spark.read
 .json("/datalake/pokemon/pokemon")
 .createOrReplaceTempView("pokemons"))

# COMMAND ----------

query = '''
select
  ingestion_date,
  poke.*
from
  pokemons
lateral view explode(results) as poke --lateral view, com explode, ele quebra a result, trazendo toda informacao do dicionario da variavel results
QUALIFY row_number() OVER (PARTITION BY poke.name ORDER BY ingestion_date DESC) = 1 -- isso particiona para cada pokemon e ordenando pela data mais recente da ingestao dos dados
'''

df = spark.sql(query).coalesce(1)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("hive_metastore.pokemon.pokemon")

# COMMAND ----------

df = spark.read.json("/datalake/pokemon/pokemon_details")
df.display()
