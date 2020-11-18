package com.yaslebid.AvroToBigQuery.services;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication;
import com.yaslebid.AvroToBigQuery.config.BigQueryConfiguration;
import com.yaslebid.AvroToBigQuery.util.AvroToBigQueryDataLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class AvroToBigQueryJobProcessor {

    static final Logger logger =
            LoggerFactory.getLogger(AvroToBigQueryApplication.class);

    @Autowired
    AvroToBigQueryDataLoader avroLoader;

    @Bean
    @Scheduled(fixedRate = 5000)
    public void run() {
        String blobName;
        boolean loadResultTable1;
        boolean loadResultTable2;
        Page<Blob> blobs = BigQueryConfiguration.BUCKET.list();

        for (Blob blob : blobs.iterateAll()) {
            blobName = blob.getName();
            if (blobName.endsWith(".avro")) {
                loadResultTable1 = avroLoader.loadData(blobName, BigQueryConfiguration.TABLE_CLIENT);
                loadResultTable2 = avroLoader.loadData(blobName, BigQueryConfiguration.TABLE_CLIENT_MANDATORY);
                renameObject(blob, loadResultTable1 && loadResultTable2);
            }
        }
    }

    private void renameObject(Blob sourceBlob, boolean isSuccess) {
        CopyWriter copyWriter;

        if (isSuccess) {
            copyWriter = sourceBlob.copyTo(BigQueryConfiguration.BUCKET_NAME, sourceBlob.getName().concat(".processed"));
            Blob copiedBlob = copyWriter.getResult();
            sourceBlob.delete();
            logger.info("Renamed object: " + sourceBlob.getName() + " to: " + copiedBlob.getName());
        } else {
            copyWriter = sourceBlob.copyTo(BigQueryConfiguration.BUCKET_NAME, sourceBlob.getName().concat(".failed"));
            Blob copiedBlob = copyWriter.getResult();
            sourceBlob.delete();
            logger.info("Renamed object: " + sourceBlob.getName() + " to: " + copiedBlob.getName());
        }

    }
}