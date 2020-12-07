package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.yaslebid.avro.Client;

public class BigQueryRowInsertException extends RuntimeException {
    public BigQueryRowInsertException (Client client, Throwable exception) {
        super("Error inserting row to BQ: " + client.toString() + " || BQ error message: " + exception.getMessage(), exception);
    }
}
