package com.yaslebid.AvroToBigQuery.helpers;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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
            log.error("Blob not exist in the bucket and cannot be renamed");
            return false;
        }
    }
}
