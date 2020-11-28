package com.yaslebid.AvroToBigQuery.helpers;

public interface GCPObjectsOperator {
    boolean renameBlobObject(String fileName, boolean isSuccess);
}
