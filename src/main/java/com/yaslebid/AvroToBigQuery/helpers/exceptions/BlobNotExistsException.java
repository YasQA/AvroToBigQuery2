package com.yaslebid.AvroToBigQuery.helpers.exceptions;

public class BlobNotExistsException extends RuntimeException {

    public BlobNotExistsException (String fileName, Throwable exception) {
        super("File/Blob is not available: " + fileName + " || Initial Storage message: " + exception.getMessage(), exception);
    }
}
