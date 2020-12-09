package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.google.cloud.bigquery.*;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.AvroToBigQuery.repository.BigQueryDataOperator;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
class BigQueryRowInsertExceptionTest {

    @MockBean
    private BigQuery bigQuery;
    @Autowired
    private BigQueryDataOperator bigQueryDataOperator;

    @Test
    public void bigQueryRowInsertException_whenBigQueryExceptionThrown() {
        String tableName = GCPResources.TABLE_CLIENT_MANDATORY;
        TableId tableId = TableId.of(GCPResources.DATASET_NAME, tableName);
        Map<String, Object> rowContent = new HashMap<>();
        InsertAllRequest insertAllRequest = InsertAllRequest.newBuilder(tableId).addRow(rowContent).build();

        when(bigQuery.insertAll(insertAllRequest))
                .thenThrow(BigQueryException.class);

        Exception exception = assertThrows(BigQueryRowInsertException.class, () -> {
            bigQueryDataOperator.insertRow(TestData.clientFirst, true);
        });
    }

    @Test
    public void bigQueryRowInsertException_whenWrongBigQueryConfig() {
        Exception exception = assertThrows(BigQueryRowInsertException.class, () -> {
            bigQueryDataOperator.insertRow(TestData.clientFirst, true);;
        });
    }
}