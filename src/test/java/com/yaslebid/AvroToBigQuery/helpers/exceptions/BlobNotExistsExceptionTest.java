package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
class BlobNotExistsExceptionTest {

    @Autowired
    private FileToBigQueryJobProcessor avroToBigQueryJobProcessor;

    @Test
    public void BlobNotExistsException_whenFileNotAvailableOnStorageBucket() {
        Exception exception = assertThrows(BlobNotExistsException.class, () -> {
            avroToBigQueryJobProcessor.executeTasks("NotExistentFile.avro");
        });
    }
}