# AvroToBigQuery2
Service to processing Avro files from GCP bucket to tables on BigData

AvroParser Application to proceed *.avro data files into BigQuery tables on GCP
Application (as a Service executed on CloudRun) looking for new *.avro files in specified bucket and proceed data from new files into 2 tables in BigQuery.

Note:
'resources' folder contains examples of *.avro files (one fake) for selected schema (received using avro-tool-xxx.jar)

 App may be executed locally and packaged in into docker file and run from Cloud run.
- checks for new *.avro files using scheduler
- import data(if it corresponds to schema) from avro files into 2 tables: ALL columns and only MUST columns
- if data from file imported successfully file renamed to *.avro.imported
- if data from file imported successfully file renamed to *.avro.failed
