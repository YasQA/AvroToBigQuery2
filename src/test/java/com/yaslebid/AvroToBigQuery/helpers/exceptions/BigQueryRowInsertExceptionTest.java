package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.google.cloud.bigquery.BigQuery;
import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;


@ActiveProfiles("test2")
@RunWith(SpringRunner.class)
@SpringBootTest
class BigQueryRowInsertExceptionTest {

    @Autowired
    private FileToBigQueryJobProcessor avroToBigQueryJobProcessor;

    @Autowired
    private FileDeserializerForClient avroFileDeserializerForClient;

    @Autowired
    private BigQuery bigQuery;


    @Test
    public void bigQueryRowInsertExceptionTest_() {
        //TODO

    }
}