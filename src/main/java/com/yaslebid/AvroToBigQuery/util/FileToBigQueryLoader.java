package com.yaslebid.AvroToBigQuery.util;

public interface FileToBigQueryLoader {
    boolean loadData(String blobName, String tableName);
}
