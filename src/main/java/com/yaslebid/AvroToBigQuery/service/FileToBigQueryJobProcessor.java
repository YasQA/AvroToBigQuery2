package com.yaslebid.AvroToBigQuery.service;

public interface FileToBigQueryJobProcessor {
    boolean executeTasks(String fileName);
}
