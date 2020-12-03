package com.yaslebid.AvroToBigQuery.repository;

import com.google.cloud.bigquery.*;

import java.util.*;

import com.google.cloud.bigquery.BigQuery;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.avro.Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Component
public class BigQueryDataOperator implements DBDataOperator {

    private final BigQuery bigQuery;

    @Autowired
    public BigQueryDataOperator(BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    public boolean insertRow(Client client, boolean isMandatoryFieldsTable) {
        String tableName = isMandatoryFieldsTable ? GCPResources.TABLE_CLIENT_MANDATORY : GCPResources.TABLE_CLIENT;
        TableId tableId = TableId.of(GCPResources.DATASET_NAME, tableName);

        try {
            Map<String, Object> rowContent = new HashMap<>();
            rowContent.put(GCPResources.COLUMN_ID, client.getId());
            rowContent.put(GCPResources.COLUMN_NAME, client.getName().toString());

            if (!isMandatoryFieldsTable) {
                rowContent.put(GCPResources.COLUMN_PHONE, client.getPhone().toString());
                rowContent.put(GCPResources.COLUMN_ADDRESS, client.getAddress().toString());
            }

            InsertAllResponse response = bigQuery.insertAll(
                    InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());

            if (response.hasErrors()) {
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    LOGGER.error("Response errors:" + entry.getValue().toString());
                }
                return false;
            }
            LOGGER.info("Row inserted into '" + tableName + "' table");
            return true;

        } catch (BigQueryException exception) {
            LOGGER.error("Insert operation not performed \n" + exception.toString());
            return false;
        }
    }

}
