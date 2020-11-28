package com.yaslebid.AvroToBigQuery.service;

import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.repository.FileToBigQueryLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AvroToBigQueryJobProcessor implements FileToBigQueryJobProcessor {

    @Autowired
    FileToBigQueryLoader fileToBigQueryLoader;

    @Autowired
    GCPObjectsOperator gcpObjectsOperator;

    public boolean executeTasks(String fileName) {
        boolean importResultTable1;
        boolean importResultTable2;
        boolean importResultSummary;

        importResultTable1 = fileToBigQueryLoader.loadDataToTable(fileName, GCPResources.TABLE_CLIENT);
        importResultTable2 = fileToBigQueryLoader.loadDataToTable(fileName, GCPResources.TABLE_CLIENT_MANDATORY);
        importResultSummary = importResultTable1 && importResultTable2;

        gcpObjectsOperator.renameBlobObject(fileName, importResultSummary);

        return importResultSummary;
    }

}