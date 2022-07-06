from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import json


event_schema = StructType([
  StructField('timestamp', TimestampType(), True),
  StructField('step', StringType(), True),
  StructField('expectation', StringType(), True),
  StructField('passed', IntegerType(), True),
  StructField('failed', IntegerType(), True),
])


def events_to_dataframe(df):
    d = []
    group_key = df['timestamp'].iloc[0]
    for i, r in df.iterrows():
        json_obj = json.loads(r['details'])
        try:
            expectations = json_obj['flow_progress']['data_quality']['expectations']
            for expectation in expectations:
                d.append([group_key, expectation['dataset'], expectation['name'], expectation['passed_records'], expectation['failed_records']])
        except:
            pass
    return pd.DataFrame(d, columns=['timestamp', 'step', 'expectation', 'passed', 'failed'])

  
def extract_metrics(df):
    return df \
      .filter(F.col("event_type") == "flow_progress") \
      .groupBy("timestamp").applyInPandas(events_to_dataframe, schema=event_schema) \
      .select("timestamp", "step", "expectation", "passed", "failed")