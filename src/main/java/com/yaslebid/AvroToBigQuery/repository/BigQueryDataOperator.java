package com.yaslebid.AvroToBigQuery.repository;

import com.google.cloud.bigquery.*;

import com.google.cloud.bigquery.BigQuery;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BigQueryRowInsertException;
import com.yaslebid.avro.Client;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@AllArgsConstructor
@Slf4j
public class BigQueryDataOperator implements DBDataOperator {

    private final BigQuery bigQuery;

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
                    log.error("Response errors:" + entry.getValue().toString());
                }
                return false;
            }
            log.info("Row inserted into '" + tableName + "' table");
            return true;

        } catch (BigQueryException | NullPointerException exception) {
            log.error("Insert operation failed: " + exception.getMessage());
            throw new BigQueryRowInsertException(client, exception);
        }
    }

    public List<FieldValueList> simpleSelect(String query) {
        List<FieldValueList> selectResultList = new ArrayList<>();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            for (FieldValueList row : bigQuery.query(queryConfig).iterateAll()) {
                selectResultList.add(row);
            }
        } catch (BigQueryException | InterruptedException exception) {
            log.error("SELECT query execution fails: \n" + exception.toString());
        }
        return selectResultList;
    }

    public void simpleDelete(String query) {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            bigQuery.query(queryConfig);

        } catch (BigQueryException | InterruptedException exception) {
            log.error("DELETE query execution fails: \n" + exception.toString());
        }
    }
}
