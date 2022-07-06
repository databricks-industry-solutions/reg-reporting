# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">
# MAGIC 
# MAGIC [![DBR](https://img.shields.io/badge/DBR-10.4-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
# MAGIC [![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
# MAGIC [![POC](https://img.shields.io/badge/POC-3_days-green?style=for-the-badge)](https://databricks.com/try-databricks)
# MAGIC 
# MAGIC 
# MAGIC *In today’s interconnected world, managing risk and regulatory compliance is an increasingly complex and costly endeavour.
# MAGIC Regulatory change has increased 500% since the 2008 global financial crisis and boosted regulatory costs in the process. 
# MAGIC Given the fines associated with non-compliance and SLA breaches (banks hit an all-time high in fines of $10 billion in 2019 for AML), 
# MAGIC processing reports has to proceed even if data is incomplete. On the other hand, a track record of poor data quality is also "fined" because of "insufficient controls". 
# MAGIC As a consequence, many FSIs are often left battling between poor data quality and strict SLA, balancing between data reliability and data timeliness. 
# MAGIC In this solution accelerator, we demonstrate how [Delta Live Tables](https://databricks.com/product/delta-live-tables) 
# MAGIC can guarantee the acquisition and processing of regulatory data in real time to accommodate regulatory SLAs and, 
# MAGIC coupled with [Delta Sharing](https://databricks.com/blog/2021/05/26/introducing-delta-sharing-an-open-protocol-for-secure-data-sharing.html), 
# MAGIC to provide analysts with a real time confidence in regulatory data being transmitted. 
# MAGIC Through these series notebooks and underlying code, we will demonstrate the benefits of a standardized data model for regulatory 
# MAGIC data coupled the flexibility of Delta Lake to guarantee both **reliability** and **timeliness** in the transmission, 
# MAGIC acquisition and calculation of data between regulatory systems in finance*
# MAGIC 
# MAGIC 
# MAGIC ___
# MAGIC <antoine.amend@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src=https://raw.githubusercontent.com/databricks-industry-solutions/reg-reporting/main/images/reference_architecture.png width=800>

# COMMAND ----------

# MAGIC %md
# MAGIC ## FIRE data model
# MAGIC 
# MAGIC The Financial Regulatory data standard (FIRE) defines a common specification for the transmission of granular data between regulatory systems in finance. Regulatory data refers to data that underlies regulatory submissions, requirements and calculations and is used for policy, monitoring and supervision purposes. The [FIRE data standard](https://suade.org/fire/) is supported by the European Commission, the Open Data Institute and the Open Data Incubator FIRE data standard for Europe via the Horizon 2020 funding programme. As part of this solution, we contributed a PySpark module that can interpret FIRE data models into Apache Spark™ operating pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize environment
# MAGIC For the purpose of that solution, we first have to download a FIRE data model from Suade Labs [github](https://github.com/SuadeLabs/fire) as well as generating sample dataset for delta live tables. The below configuration will be passed to a delta live table via its spark configuration context. 

# COMMAND ----------

# MAGIC %run ./config/configure_notebook

# COMMAND ----------

generate_data()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC | library                                | description             | license    | source                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | FIRE                                   | Regulatory models       | Apache v2  | https://github.com/SuadeLabs/fire                   |
# MAGIC | waterbear                              | data model lib         | Databricks | https://github.com/databrickslabs/waterbear          |
# MAGIC | PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |
