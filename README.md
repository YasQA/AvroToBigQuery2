# AvroToBigQuery2
Service to processing Avro files from GCP bucket to tables on BigData. 

AvroParser Application to proceed *.avro data files into BigQuery tables on GCP
Application (as a Service executed on CloudRun).
Service listen for Push PubSub notifications for new *.avro files appearing in specified bucket and proceed data from the files into 2 tables in BigQuery.

 App may be executed locally and packaged into docker file and run from Cloud run.
- checks for new *.avro files by listen to PubSub Push notifications.
- import data(if it corresponds to schema) from avro files into 2 tables: ALL columns and only MUST columns
- if data from file imported successfully file renamed to *.avro.imported
- if data from file imported successfully file renamed to *.avro.failed
