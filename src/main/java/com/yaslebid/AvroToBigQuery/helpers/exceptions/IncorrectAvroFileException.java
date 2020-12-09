package com.yaslebid.AvroToBigQuery.helpers.exceptions;

public class IncorrectAvroFileException extends RuntimeException {

    public IncorrectAvroFileException(String fileName, Throwable exception) {
        super("Wrong Avro file: " + fileName + " || Initial IOException message: " + exception.getMessage(), exception);
    }
}
