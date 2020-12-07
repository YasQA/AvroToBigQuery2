package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
class IncorrectAvroFileExceptionTest {

    @Autowired
    private FileToBigQueryJobProcessor avroToBigQueryJobProcessor;

    static String fakeFileName = "fakeFile.avro";

    @BeforeAll
    public static void uploadFakeAvroFile() throws IOException {
        BlobId blobId = BlobId.of(GCPResources.BUCKET_NAME, fakeFileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GCPResources.STORAGE.create(blobInfo, Files.readAllBytes(Paths.get("src/main/resources/fakeFile.avro")));
    }

    @Test
    public void incorrectAvroFileExceptionTest_withIncorrectAvroFile() {

        Exception exception = assertThrows(IncorrectAvroFileException.class, () -> {
            avroToBigQueryJobProcessor.executeTasks(fakeFileName);
        });
    }

    @AfterAll
    public static void dropFakeAvroFile() {
        Blob sourceBlob = GCPResources.BUCKET.get(fakeFileName);
        sourceBlob.delete();
    }
}