package com.yaslebid.AvroToBigQuery.helpers;

import java.io.IOException;

public interface GCPObjectsOperator {
    boolean renameBlobObject(String fileName, boolean isSuccess);
    void uploadFileToBucket(String fileName) throws IOException;

    void dropFileOnBucket(String fileName);
}
