<img src=https://d1r5llqwmkrl74.cloudfront.net/notebooks/fs-lakehouse-logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-3_days-green?style=for-the-badge)](https://databricks.com/try-databricks)


*In today’s interconnected world, managing risk and regulatory compliance is an increasingly complex and costly endeavour.
Regulatory change has increased 500% since the 2008 global financial crisis and boosted regulatory costs in the process. 
Given the fines associated with non-compliance and SLA breaches (banks hit an all-time high in fines of $10 billion in 2019 for AML), 
processing reports has to proceed even if data is incomplete. On the other hand, a track record of poor data quality is also "fined" because of "insufficient controls". 
As a consequence, many FSIs are often left battling between poor data quality and strict SLA, balancing between data reliability and data timeliness. 
In this solution accelerator, we demonstrate how [Delta Live Tables](https://databricks.com/product/delta-live-tables) 
can guarantee the acquisition and processing of regulatory data in real time to accommodate regulatory SLAs and, 
coupled with [Delta Sharing](https://databricks.com/blog/2021/05/26/introducing-delta-sharing-an-open-protocol-for-secure-data-sharing.html), 
to provide analysts with a real time confidence in regulatory data being transmitted. 
Through these series notebooks and underlying code, we will demonstrate the benefits of a standardized data model for regulatory 
data coupled the flexibility of Delta Lake to guarantee both **reliability** and **timeliness** in the transmission, 
acquisition and calculation of data between regulatory systems in finance*


___
<antoine.amend@databricks.com>

___

<img src=https://github.com/databricks-industry-solutions/reg-reporting/blob/master/images/reference_architecture.png?raw=true width="800px">

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Yfinance                               | Yahoo finance           | Apache2    | https://github.com/ranaroussi/yfinance              |
| tempo                                  | Timeseries library      | Databricks | https://github.com/databrickslabs/tempo             |
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |
