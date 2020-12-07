package com.yaslebid.AvroToBigQuery.repository;

import com.yaslebid.avro.Client;

public interface DBDataOperator {
    boolean insertRow(Client client, boolean isMandatoryFieldsTable);
}
