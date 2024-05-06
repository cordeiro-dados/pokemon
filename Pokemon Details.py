# Databricks notebook source
import requests
import datetime
import json
from multiprocessing import Pool

# COMMAND ----------

url = "https://pokeapi.co/api/v2/pokemon/1/"

resp = requests.get(url)


def save_pokemon(data):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    data['data_ingestion'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    filename = f"/dbfs/datalake/pokemon/pokemon_details/{data['id']}_{now}.json"
    with open(filename, "w") as open_file:
        json.dump(data, open_file)

def get_and_save(url):
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()
        save_pokemon(data)
    else:
        print(f'nao foi possivel obeter os dados da url {url}')

urls = (spark.table("hive_metastore.pokemon.pokemon")
             .select("url")
             .distinct()
             .toPandas()["url"]
             .tolist())

with Pool(4) as p:
  p.map(get_and_save, urls)
