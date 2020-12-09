package com.yaslebid.AvroToBigQuery.repository;

import com.google.cloud.bigquery.FieldValueList;
import com.yaslebid.avro.Client;

import java.util.List;

public interface DBDataOperator {
    boolean insertRow(Client client, boolean isMandatoryFieldsTable);

    List<FieldValueList> simpleSelect(String query);

    public void simpleDelete(String query);
}
