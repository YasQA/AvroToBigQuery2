package com.yaslebid.AvroToBigQuery.repository;

import com.google.cloud.bigquery.*;

import com.yaslebid.AvroToBigQuery.config.GCPResources;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Component
public class AvroFileToBigQueryLoader implements FileToBigQueryLoader {

    @Autowired
    BigQuery bigQuery;

    public boolean loadDataToTable(String fileName, String tableName) {
        TableId tableId;
        Job loadJob;
        Job completedJob;

        try {
            tableId = TableId.of(GCPResources.DATASET_NAME, tableName);

            JobConfiguration jobConfig = LoadJobConfiguration
                    .newBuilder(tableId, GCPResources.BUCKET_ID.concat(fileName))
                    .setIgnoreUnknownValues(true)
                    .setFormatOptions(FormatOptions.avro())
                    .build();

            loadJob = bigQuery.create(JobInfo.of(jobConfig));
            completedJob = loadJob.waitFor();

            LOGGER.info("File: '" + fileName + "' Processed into the table: '" + tableName + "' successfully!");
            return completedJob.isDone();

        } catch (BigQueryException | InterruptedException exception) {
            LOGGER.error("File: '" + fileName + "' Failed to load into the table: '"
                    + tableName + "' due to an error! :\n" + exception.getMessage());
            return false;
        }
    }
}
