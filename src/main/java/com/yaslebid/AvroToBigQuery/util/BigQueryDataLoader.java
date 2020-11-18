package com.yaslebid.AvroToBigQuery.util;

public interface BigQueryDataLoader {
    boolean loadData(String blobName, String tableName);
}
