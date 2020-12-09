package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.repository.DBDataOperator;
import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
class BlobRenameExceptionTest {

    @Autowired
    private FileToBigQueryJobProcessor avroToBigQueryJobProcessor;
    @MockBean
    private FileDeserializerForClient avroFileDeserializerForClient;
    @MockBean
    private DBDataOperator bigQueryDataOperator;

    @Test
    public void blobRenameException_whenFileNotExists() {
        Exception exception = assertThrows(BlobRenameException.class, () -> {
            avroToBigQueryJobProcessor.executeTasks("NotExistentFile.avro");
        });
    }
}