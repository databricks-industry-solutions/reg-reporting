# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator with. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are user-specific, so you can alter the workflow and cluster via UI without affecting other users. Running this script again after modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS databricks_solacc LOCATION '/databricks_solacc/'")
spark.sql(f"CREATE TABLE IF NOT EXISTS databricks_solacc.dlt (path STRING, pipeline_id STRING, solacc STRING)")
dlt_config_table = "databricks_solacc.dlt"

# COMMAND ----------


pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 3
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"01_fire_dlt"
                  }
              }
          ],
          "name": "fire_collateral_dlt",
          "storage": "/FileStore/fire/dlt/collateral",
          "configuration": {
              "fire.entity": "collateral",
              "fire.events.dir": f"/FileStore/fire/events",
              "fire.model.dir": f"/dbfs/FileStore/fire/model/fire-master/v1-dev"
          },
          "target": f"SOLACC_fire",
          "allow_duplicate_names": "true"
      }

# COMMAND ----------

pipeline_id = NotebookSolutionCompanion().deploy_pipeline(pipeline_json, dlt_config_table, spark)

# COMMAND ----------

job_json = {
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "FSI"
        },
        "tasks": [
            {
                "task_key": "RR_01",
                "notebook_task": {
                    "notebook_path": f"00_fire_context"
                },
                "job_cluster_key": "RR_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "RR_02",
                "depends_on": [
                    {
                        "task_key": "RR_01"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            },
            {
                "task_key": "RR_03",
                "depends_on": [
                    {
                        "task_key": "RR_02"
                    }
                ],
                "notebook_task": {
                    "notebook_path": f"02_fire_metrics"
                },
                "job_cluster_key": "RR_cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "description": ""
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "RR_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "num_workers": 2,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
            "usage": "solacc_testing",
            "group": "FSI"
        }
                }
            }
        ]
    }


# COMMAND ----------

# DBTITLE 1,Companion job and cluster(s) definition
dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------


