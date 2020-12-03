package com.yaslebid.AvroToBigQuery.repository;

public interface ___FileToBigQueryLoader {
    boolean loadDataToTable(String fileName, String tableName);
}
