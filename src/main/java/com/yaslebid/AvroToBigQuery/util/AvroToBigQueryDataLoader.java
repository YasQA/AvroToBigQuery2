package com.yaslebid.AvroToBigQuery.util;

import com.google.cloud.bigquery.*;

import com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication;
import com.yaslebid.AvroToBigQuery.config.BigQueryConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AvroToBigQueryDataLoader implements BigQueryDataLoader {

    static final Logger logger =
            LoggerFactory.getLogger(AvroToBigQueryApplication.class);

    @Autowired
    BigQuery bigQuery;

    TableId tableId;

    public boolean loadData(String blobName, String tableName) {
        try {
            tableId = TableId.of(BigQueryConfiguration.DATASET_NAME, tableName);

            JobConfiguration jobConfig = LoadJobConfiguration
                    .newBuilder(tableId, BigQueryConfiguration.BUCKET_ID.concat(blobName))
                    .setIgnoreUnknownValues(true)
                    .setFormatOptions(FormatOptions.avro())
                    .build();

            Job loadJob = bigQuery.create(JobInfo.of(jobConfig));
            Job completedJob = loadJob.waitFor();

            logger.error("File: " + blobName + " Processed into the table: " + tableName + " successfully!");
            return completedJob.isDone();

        } catch (BigQueryException | InterruptedException exception) {
            logger.error("File: " + blobName + " Failed to load into the table: " + tableName + " due to an error! :\n" + exception.getMessage());
            return false;
        }
    }
}
