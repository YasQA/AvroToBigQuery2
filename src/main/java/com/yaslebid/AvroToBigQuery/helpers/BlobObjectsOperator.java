package com.yaslebid.AvroToBigQuery.helpers;

import com.google.cloud.storage.*;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobRenameException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Component
@Slf4j
public class BlobObjectsOperator implements GCPObjectsOperator {
    public boolean renameBlobObject(String fileName, boolean isSuccess) {
        CopyWriter copyWriter;
        Blob copiedBlob;
        Blob sourceBlob = GCPResources.BUCKET.get(fileName);

        try {
            String fileExtension = isSuccess ? ".processed" : ".failed";
            String targetBlob = sourceBlob.getName().concat(fileExtension);
            copyWriter = sourceBlob.copyTo(GCPResources.BUCKET_NAME, targetBlob);

            copiedBlob = copyWriter.getResult();
            sourceBlob.delete();
            log.info("Renamed object: '" + sourceBlob.getName() + "' to: '" + copiedBlob.getName() + "'");
            return true;

        } catch (Exception exception) {
            log.error("Failed to rename blob: " + fileName, exception.getMessage());
            throw new BlobRenameException(fileName, exception);
        }
    }

    public void uploadFileToBucket(String fileName) throws IOException {
        BlobId blobId = BlobId.of(GCPResources.BUCKET_NAME, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        GCPResources.STORAGE.create(blobInfo, Files.readAllBytes(Paths.get("src/main/resources/".concat(fileName))));
    }

    public void dropFileOnBucket(String fileName) {
        Blob sourceBlob = GCPResources.BUCKET.get(fileName);
        sourceBlob.delete();
    }
}
