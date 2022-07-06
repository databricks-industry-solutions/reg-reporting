# Databricks notebook source
# MAGIC %pip install -r requirements.txt

# COMMAND ----------

import yaml
with open('config/application.yaml', 'r') as f:
  config = yaml.safe_load(f)

# COMMAND ----------

import requests, zipfile
from io import BytesIO
import os
  
def get_fire_model():
    model_dir = '/dbfs{}'.format(config['fire']['model']['dir'])
    if not os.path.exists(model_dir) or len(os.listdir(model_dir)) == 0:
        print('Downloading FIRE model')
        url = 'https://github.com/SuadeLabs/fire/archive/refs/heads/master.zip'
        req = requests.get(url)
        zfl = zipfile.ZipFile(BytesIO(req.content))
        zfl.extractall(model_dir)
    return '{}/fire-master/v1-dev'.format(model_dir)

# COMMAND ----------

import os
model_dir = get_fire_model()
assert(len(os.listdir(model_dir)) > 0)
model_dir

# COMMAND ----------

db_name = config['delta']['db']['name']
db_path = config['delta']['db']['path']
_ = sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))

# COMMAND ----------

def generate_data(): 
  import os
  import uuid
  import tempfile
  from waterbear.generator import JsonRecordGenerator
  wb = JsonRecordGenerator(model_dir)
  event_dir = config['fire']['events']['dir']
  fire_entity = config['fire']['entity']
  fire_entity_dir = f'{event_dir}/{fire_entity}'
  dbutils.fs.mkdirs(fire_entity_dir)
  with tempfile.TemporaryDirectory() as temp_dir:
    for i in range(0, config['files']['num']):
      print('Generating file {} for entity {}'.format(i+1, fire_entity))
      unique_file_name = '{}_{}.json'.format(fire_entity, str(uuid.uuid4()))
      entity_file = f'{temp_dir}/{unique_file_name}'
      with open((entity_file), 'w') as f:
        for x in wb.generate(fire_entity, config['files']['records']):
          f.write(x)
      dbutils.fs.cp(f'file:{entity_file}', f'dbfs:{fire_entity_dir}/{unique_file_name}') 
