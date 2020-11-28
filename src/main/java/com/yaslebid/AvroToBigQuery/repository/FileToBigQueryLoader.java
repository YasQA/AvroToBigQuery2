package com.yaslebid.AvroToBigQuery.repository;

public interface FileToBigQueryLoader {
    boolean loadDataToTable(String fileName, String tableName);
}
