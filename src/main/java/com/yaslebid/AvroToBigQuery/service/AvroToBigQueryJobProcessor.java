package com.yaslebid.AvroToBigQuery.service;

import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.repository.DBDataOperator;
import com.yaslebid.avro.Client;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class AvroToBigQueryJobProcessor implements FileToBigQueryJobProcessor {

    private final FileDeserializerForClient avroFileDeserializer;
    private final DBDataOperator bigQueryDataOperator;
    private final GCPObjectsOperator blobObjectsOperator;

    public boolean executeTasks(String fileName) {
        boolean insertResultAllFields = false;
        boolean insertResultMandatoryFields = false;
        boolean insertResultSummary;
        List<Client> clientList;

        clientList = avroFileDeserializer.getClientListFromFile(fileName);

        for (Client client : clientList) {
            insertResultAllFields = bigQueryDataOperator.insertRow(client, false);
            insertResultMandatoryFields = bigQueryDataOperator.insertRow(client, true);
        }

        insertResultSummary = insertResultAllFields && insertResultMandatoryFields;
        blobObjectsOperator.renameBlobObject(fileName, insertResultSummary);
        return insertResultSummary;
    }

}