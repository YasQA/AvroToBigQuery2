package com.yaslebid.AvroToBigQuery.helpers;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import org.springframework.stereotype.Component;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Component
public class BlobObjectsOperator implements GCPObjectsOperator {
    public boolean renameBlobObject(String fileName, boolean isSuccess) {
        CopyWriter copyWriter;
        Blob copiedBlob;
        Blob sourceBlob = GCPResources.BUCKET.get(fileName);

        try {
            if (isSuccess) {
                copyWriter = sourceBlob.copyTo(GCPResources.BUCKET_NAME, sourceBlob.getName().concat(".processed"));
            } else {
                copyWriter = sourceBlob.copyTo(GCPResources.BUCKET_NAME, sourceBlob.getName().concat(".failed"));
            }
            copiedBlob = copyWriter.getResult();
            sourceBlob.delete();
            LOGGER.info("Renamed object: '" + sourceBlob.getName() + "' to: '" + copiedBlob.getName() + "'");
            return true;
        } catch (Exception exception) {
            LOGGER.error("Blob not exist in the bucket and cannot be renamed");
            return false;
        }
    }
}
