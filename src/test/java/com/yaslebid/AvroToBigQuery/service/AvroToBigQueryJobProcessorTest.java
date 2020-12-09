package com.yaslebid.AvroToBigQuery.service;

import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.repository.DBDataOperator;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
class AvroToBigQueryJobProcessorTest {

    @Autowired
    FileToBigQueryJobProcessor avroToBigQueryJobProcessor;
    @MockBean
    FileDeserializerForClient avroFileDeserializerForClient;
    @MockBean
    DBDataOperator bigQueryDataOperator;
    @MockBean
    GCPObjectsOperator blobObjectsOperator;

    @Test
    public void executeTasks_returnsTrue_whenBothInsertsSuccessful() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(true);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        boolean jobProcessorResult = avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);
        Assert.assertTrue(jobProcessorResult);
    }

    @Test
    public void executeTasks_returnsFalse_whenFirstInsertsFails() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(false);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(true);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        boolean jobProcessorResult = avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);
        Assert.assertFalse(jobProcessorResult);
    }

    @Test
    public void executeTasks_returnsFalse_whenSecondInsertsFails() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(false);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        boolean jobProcessorResult = avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);
        Assert.assertFalse(jobProcessorResult);
    }

    @Test
    public void executeTasks_returnsFalse_whenClientListFromFileEmpty() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(new ArrayList<>());
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        boolean jobProcessorResult = avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);
        Assert.assertFalse(jobProcessorResult);
    }

    @Test
    public void getClientListFromFile_isCalledOnce_forOneNewFile() {
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(avroFileDeserializerForClient).getClientListFromFile(TestData.correctFileName);
    }

    @Test
    public void insertRow_callsOnceForEachTable_forOneNewClient() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                 .thenReturn(TestData.listOfOneClient);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(bigQueryDataOperator).insertRow(TestData.clientFirst, true);
        verify(bigQueryDataOperator).insertRow(TestData.clientFirst, false);
        verifyNoMoreInteractions(bigQueryDataOperator);
    }

    @Test
    public void insertRow_callsTwiceForEachTable_forTwoNewClients() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfTwoClients);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(bigQueryDataOperator).insertRow(TestData.clientFirst, true);
        verify(bigQueryDataOperator).insertRow(TestData.clientFirst, false);
        verify(bigQueryDataOperator).insertRow(TestData.clientSecond, true);
        verify(bigQueryDataOperator).insertRow(TestData.clientSecond, false);
        verifyNoMoreInteractions(bigQueryDataOperator);
    }

    @Test
    public void insertRow_callsNone_forEmptyClientsList() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(new ArrayList<>());
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verifyNoMoreInteractions(bigQueryDataOperator);
    }

    @Test
    public void renameBlobObject_callsOnce_withCorrectArgs_whenBothInsertSuccessful() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(blobObjectsOperator).renameBlobObject(TestData.correctFileName, true);
        verifyNoMoreInteractions(blobObjectsOperator);
    }

    @Test
    public void renameBlobObject_callsOnce_withCorrectArgs_whenOnlyFirstInsertSuccessful() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(false);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(true);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(blobObjectsOperator).renameBlobObject(TestData.correctFileName, false);
        verifyNoMoreInteractions(blobObjectsOperator);
    }

    @Test
    public void renameBlobObject_callsOnce_withCorrectArgs_whenOnlySecondInsertSuccessful() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(false);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(blobObjectsOperator).renameBlobObject(TestData.correctFileName, false);
        verifyNoMoreInteractions(blobObjectsOperator);
    }

    @Test
    public void renameBlobObject_callsOnce_withCorrectArgs_whenBothInsertFails() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(TestData.listOfOneClient);
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, true))
                .thenReturn(true);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, true))
                .thenReturn(false);
        when(bigQueryDataOperator.insertRow(TestData.clientFirst, false))
                .thenReturn(false);

        avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(blobObjectsOperator).renameBlobObject(TestData.correctFileName, false);
        verifyNoMoreInteractions(blobObjectsOperator);
    }

    @Test
    public void renameBlobObject_callsOnce_renameToFalse_forEmptyClientsList() {
        when(avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName))
                .thenReturn(new ArrayList<>());
        when(blobObjectsOperator.renameBlobObject(TestData.correctFileName, false))
                .thenReturn(true);

            avroToBigQueryJobProcessor.executeTasks(TestData.correctFileName);

        verify(blobObjectsOperator).renameBlobObject(TestData.correctFileName, false);
        verifyNoMoreInteractions(blobObjectsOperator);
    }
}