package com.yaslebid.AvroToBigQuery.service;

import com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.repository.___FileToBigQueryLoader;
import org.junit.jupiter.api.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AvroToBigQueryApplication.class)
class AvroToFileToBigQueryJobProcessorTest {
    @Autowired
    ___FileToBigQueryLoader fileToBigQueryLoader;

    @Autowired
    AvroToBigQueryJobProcessor avroToBigQueryJobProcessor;

    @Test
    public void whenCorrectAvroFileProvided() {
        Mockito.when(fileToBigQueryLoader.loadDataToTable("correct.avro", GCPResources.TABLE_CLIENT)).thenReturn(true);
        Mockito.when(fileToBigQueryLoader.loadDataToTable("correct.avro", GCPResources.TABLE_CLIENT_MANDATORY)).thenReturn(true);

        boolean result = avroToBigQueryJobProcessor.executeTasks("correct.avro");

        Assert.assertTrue(result);
    }

    @Test
    public void whenWrongAvroFileProvided() {
        Mockito.when(fileToBigQueryLoader.loadDataToTable("wrong.avro", GCPResources.TABLE_CLIENT)).thenReturn(false);
        Mockito.when(fileToBigQueryLoader.loadDataToTable("wrong.avro", GCPResources.TABLE_CLIENT_MANDATORY)).thenReturn(true);

        boolean result = avroToBigQueryJobProcessor.executeTasks("correct.avro");

        Assert.assertFalse(result);
    }

}