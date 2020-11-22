package com.yaslebid.AvroToBigQuery.services;

import com.google.cloud.storage.*;
import com.yaslebid.AvroToBigQuery.config.BigQueryConfiguration;
import com.yaslebid.AvroToBigQuery.util.FileToBigQueryLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Service
public class AvroToBigQueryJobProcessor {

    @Autowired
    FileToBigQueryLoader avroLoader;

    public void execute(String fileName) {
        boolean loadResultTable1;
        boolean loadResultTable2;
        if (fileName.endsWith(".avro")) {
            loadResultTable1 = avroLoader.loadData(fileName, BigQueryConfiguration.TABLE_CLIENT);
            loadResultTable2 = avroLoader.loadData(fileName, BigQueryConfiguration.TABLE_CLIENT_MANDATORY);
            renameObject(BigQueryConfiguration.BUCKET.get(fileName), loadResultTable1 && loadResultTable2);
        }
    }

    private void renameObject(Blob sourceBlob, boolean isSuccess) {
        CopyWriter copyWriter;
        Blob copiedBlob;

        if (isSuccess) {
            copyWriter = sourceBlob.copyTo(BigQueryConfiguration.BUCKET_NAME, sourceBlob.getName().concat(".processed"));
        } else {
            copyWriter = sourceBlob.copyTo(BigQueryConfiguration.BUCKET_NAME, sourceBlob.getName().concat(".failed"));
        }
        copiedBlob = copyWriter.getResult();
        sourceBlob.delete();
        LOGGER.info("Renamed object: '" + sourceBlob.getName() + "' to: '" + copiedBlob.getName() + "'");
    }
}