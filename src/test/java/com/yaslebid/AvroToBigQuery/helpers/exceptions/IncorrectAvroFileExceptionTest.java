package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import org.junit.jupiter.api.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
class IncorrectAvroFileExceptionTest {

    @Autowired
    private FileToBigQueryJobProcessor avroToBigQueryJobProcessor;
    @Autowired
    private GCPObjectsOperator blobObjectsOperator;

    @BeforeEach
    public void uploadFakeAvroFile() throws IOException {
        blobObjectsOperator.uploadFileToBucket(TestData.fakeFileName);
    }

    @Test
    public void incorrectAvroFileException_whenIncorrectAvroFile() {
        Exception exception = assertThrows(IncorrectAvroFileException.class, () -> {
            avroToBigQueryJobProcessor.executeTasks(TestData.fakeFileName);
        });
    }

    @AfterEach
    public void dropFakeAvroFile() {
        blobObjectsOperator.dropFileOnBucket(TestData.fakeFileName);
    }
}