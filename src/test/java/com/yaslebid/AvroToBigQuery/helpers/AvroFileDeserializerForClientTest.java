package com.yaslebid.AvroToBigQuery.helpers;

import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.avro.Client;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
class AvroFileDeserializerForClientTest {

    @Autowired
    private GCPObjectsOperator blobObjectsOperator;
    @Autowired
    private FileDeserializerForClient avroFileDeserializerForClient;

    @BeforeEach
    public void uploadRealAvroFile() throws IOException {
        blobObjectsOperator.uploadFileToBucket(TestData.correctFileName);
    }

    @Test
    public void avroFileDeserializerForClient_realFileDeserialize() {
        List<Client> clientListDeserialized
                = avroFileDeserializerForClient.getClientListFromFile(TestData.correctFileName);
        Assert.assertEquals(TestData.listOfTwoClients, clientListDeserialized);
    }

    @AfterEach
    public void dropRealAvroFile() {
        blobObjectsOperator.dropFileOnBucket(TestData.correctFileName);
    }

}