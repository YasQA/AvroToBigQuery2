package com.yaslebid.AvroToBigQuery.helpers.exceptions;

public class BlobRenameException extends RuntimeException {

    public BlobRenameException (String fileName, Throwable exception) {
        super("Failed to rename Blob: " + fileName + " || Initial Storage message: " + exception.getMessage(), exception);
    }
}
