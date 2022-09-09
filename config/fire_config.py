# Databricks notebook source
# MAGIC %pip install dbl-waterbear

# COMMAND ----------

import re
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username = useremail.split('@')[0]
username_sql_compatible = re.sub('\W', '_', username)
fire_home = f"/FileStore/fire"
dbutils.fs.mkdirs(fire_home)

# COMMAND ----------

def get_fire_model():
  import requests, zipfile
  from io import BytesIO
  import os
  model_dir = f'/dbfs{fire_home}/model/fire-master/v1-dev'
  if not os.path.exists(model_dir) or len(os.listdir(model_dir)) == 0:
    url = 'https://github.com/SuadeLabs/fire/archive/refs/heads/master.zip'
    req = requests.get(url)
    zfl = zipfile.ZipFile(BytesIO(req.content))
    zfl.extractall(f'/dbfs{fire_home}/model')
  return model_dir

import os
model_dir = get_fire_model()
assert(len(os.listdir(model_dir)) > 0)
model_dir

# COMMAND ----------

config = {
  'fire.model.dir': model_dir,
  'fire.entity': 'collateral',
  'fire.events.dir': f'{fire_home}/events',
  'fire.pipeline.dir': f'{fire_home}/dlt',
  'num.files': 10,
  'record.per.files': 100,
  'delta.sharing.db.path': f'{fire_home}/dmz',
  'delta.sharing.db.name': f'fire_{username_sql_compatible}',
  'delta.sharing.metrics': 'metrics'
}

# COMMAND ----------

def generate_data(): 
  import os
  import uuid
  import tempfile
  from waterbear.generator import JsonRecordGenerator
  wb = JsonRecordGenerator(config['fire.model.dir'])
  event_dir = config['fire.events.dir']
  fire_entity = config['fire.entity']
  fire_entity_dir = f'{event_dir}/{fire_entity}'
  dbutils.fs.mkdirs(fire_entity_dir)
  with tempfile.TemporaryDirectory() as temp_dir:
    for i in range(0, config['num.files']):
      print('Generating file {} for entity {}'.format(i+1, fire_entity))
      unique_file_name = '{}_{}.json'.format(fire_entity, str(uuid.uuid4()))
      entity_file = f'{temp_dir}/{unique_file_name}'
      with open((entity_file), 'w') as f:
        for x in wb.generate(fire_entity, config['record.per.files']):
          f.write(x)
      dbutils.fs.cp(f'file:{entity_file}', f'dbfs:{fire_entity_dir}/{unique_file_name}') 

# COMMAND ----------

def tear_down():
  dbutils.fs.rm(fire_home, True)
  _ = sql("DROP DATABASE IF EXISTS {} CASCADE".format(config['delta.sharing.db.name']))
