# Databricks notebook source
# MAGIC %pip install dbl-waterbear

# COMMAND ----------

try:
  # the name of the fire entity we want to process
  fire_entity = spark.conf.get("fire.entity")
except:
  raise Exception("Please provide [fire.entity] as job configuration")
  
try:
  # where new data file will be received
  fire_event_dir = spark.conf.get("fire.events.dir")
  dbutils.fs.mkdirs(fire_event_dir)
except:
  raise Exception("Please provide [fire.events.dir] as job configuration")
  
try:
  # where we can find fire data model
  fire_model = spark.conf.get("fire.model.dir")
  dbutils.fs.mkdirs(fire_model)
except:
  raise Exception("Please provide [fire.model.dir] as job configuration")

# COMMAND ----------

# MAGIC %md
# MAGIC We retrieve the name of the entity to get the FIRE data model for as well as the directory (distributed file storage) where we expect new raw files to land. These parameters are passed to the delta live table notebook via job configuration as per the screenshot above.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schematizing
# MAGIC Even though records may sometimes "look" structured (e.g. JSON files), enforcing a schema is not just a good practice; in enterprise settings, and especially relevant in the space of regulatory compliance, it guarantees any missing field is still expected, unexpected fields are discarded and data types are fully evaluated (e.g. a date should be treated as a date object and not a string). Using FIRE pyspark module, we retrieve the spark schema required to process a given FIRE entity (e.g. collateral) that we apply on a stream of raw records. This process is called data schematization.

# COMMAND ----------

from waterbear.convertor import JsonSchemaConvertor
fire_schema, fire_constraints = JsonSchemaConvertor(fire_model).convert(fire_entity)

# COMMAND ----------

# MAGIC %md
# MAGIC Our first step is to retrieve files landing to a distributed file storage using Spark auto-loader (though this framework can easily be extended to read different streams, using a Kafka connector for instance). In continuous mode, files will be processed as they land, `max_files` at a time. In triggered mode, only new files will be processed since last run. Using Delta Live Tables, we ensure the execution and processing of delta increments, preventing organizations from having to maintain complex checkpointing mechanisms to understand what data needs to be processed next; delta live tables seamlessly handles records that haven't yet been processed, first in first out.

# COMMAND ----------

@dlt.create_table()
def bronze():
  return (
    spark
      .readStream
      .format('json')
      .schema(fire_schema)
      .load(f'{fire_event_dir}/{fire_entity}')
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations
# MAGIC Applying a schema is one thing, enforcing its constraints is another. Given the schema definition of a FIRE entity, we can detect if a field is required or not. Given an enumeration object, we ensure its values consistency (e.g. country code). In addition to the technical constraints derived from the schema itself, the FIRE model also reports business expectations using e.g. `minimum`, `maximum`, `maxItems` JSON parameters. All these technical and business constraints will be programmatically retrieved from the FIRE model and interpreted as a series of SQL expressions. 

# COMMAND ----------

# MAGIC %md
# MAGIC Our pipeline will evaluate our series of SQL rules against our schematized dataset (i.e. reading from Bronze), dropping record breaching any of our expectations through the `expect_all_or_drop` pattern and reporting on data quality in real time (note that one could simply flag records or fail an entire pipeline using resp. `expect_all` or `expect_all_or_fail`). At any point in time, we have clear visibility in how many records were dropped prior to landing on our silver layer.

# COMMAND ----------

@dlt.create_table()
@dlt.expect_all_or_drop(fire_constraints)
def silver():
  return dlt.read_stream("bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Invalid records
# MAGIC In this example, we made the choice to explicitly isolate invalid from valid records to ensure 100% data quality of regulatory data being transmitted. But in order to ensure full compliance (quality AND volume), we should also redirect invalid records to a quarantine table that can be further investigated and replayed if needed.

# COMMAND ----------

@udf('array<string>')
def violations(xs, ys):
    return [ys[i] for i, x in enumerate(xs) if not x]
  
constraints_expr = F.array([F.expr(x) for x in fire_constraints.values()])
constraints_name = F.array([F.lit(x) for x in fire_constraints.keys()])

# COMMAND ----------

# MAGIC %md
# MAGIC Using a simple user defined function, we add an additional field to our original table with the name(s) of failed SQL expressions. The filtered output is sent to a quarantine table so that the union of quarantine and silver equals the volume expected from our bronze layer.

# COMMAND ----------

@dlt.create_table()
def quarantine():
  return (
      dlt
        .read_stream("bronze")
        .withColumn("_fire", violations(constraints_expr, constraints_name)) \
        .filter(F.size("_fire") > 0)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Take away
# MAGIC Finally, our pipeline has been orchestrated between Bronze, Silver and Quarantine, ensuring reliability in the transmission and validation of regulatory reports as new records unfold. As represented in the screenshot below, risk analysts have full visibility around number of records being processed in real time. In this specific example, we ensured that our collateral entity is exactly 92.2% complete (quarantine handles the remaining 8%).

# COMMAND ----------

# MAGIC %md
# MAGIC In the next section, we will demonstrate how organizations can create a simple operation data store to consume delta live tables metrics in real time, as new regulatory data is transmitted. Finally, we will demonstrate how delta sharing capability can ensure integrity in the reports beeing exchanged between FSIs and regulatory bodies.
