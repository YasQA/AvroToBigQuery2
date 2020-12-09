package com.yaslebid.AvroToBigQuery.repository;

import com.google.cloud.bigquery.FieldValueList;
import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
class BigQueryDataOperatorTest {

    @Autowired
    DBDataOperator bigQueryDataOperator;

    static final String selectMandatoryFieldsTable
            = "SELECT * FROM `" + GCPResources.TABLE_CLIENT_MANDATORY_ID + "` WHERE id = 1;";
    static final String selectAllFieldsTable
            = "SELECT * FROM `" + GCPResources.TABLE_CLIENT_ID + "` WHERE id = 1;";

    @Test
    void insertRow_intoTableWithMandatoryFields() {
        int numberOfTestRowsBeforeInsert = bigQueryDataOperator.simpleSelect(selectMandatoryFieldsTable).size();
        bigQueryDataOperator.insertRow(TestData.clientFirst, true);
        List<FieldValueList> selectResultList = bigQueryDataOperator.simpleSelect(selectMandatoryFieldsTable);

        Assert.assertEquals(numberOfTestRowsBeforeInsert + 1, selectResultList.size());
    }

    @Test
    void insertRow_intoTableWithAllFields() {
        int numberOfTestRowsBeforeInsert = bigQueryDataOperator.simpleSelect(selectAllFieldsTable).size();
        bigQueryDataOperator.insertRow(TestData.clientFirst, false);
        List<FieldValueList> selectResultList = bigQueryDataOperator.simpleSelect(selectAllFieldsTable);

        Assert.assertEquals(numberOfTestRowsBeforeInsert + 1, selectResultList.size());
    }

    @Test
    void insertRow_intoTableWithMandatoryFields_thenTableWithAllFieldsNotChanged() {
        int numberOfTestRowsBeforeInsert = bigQueryDataOperator.simpleSelect(selectAllFieldsTable).size();
        bigQueryDataOperator.insertRow(TestData.clientFirst, true);
        List<FieldValueList> selectResultList = bigQueryDataOperator.simpleSelect(selectAllFieldsTable);

        Assert.assertEquals(numberOfTestRowsBeforeInsert, selectResultList.size());
    }

    @Test
    void insertRow_intoTableWithAllFields_thenTableWithMandatoryFieldsNotChanged() {
        int numberOfTestRowsBeforeInsert = bigQueryDataOperator.simpleSelect(selectMandatoryFieldsTable).size();
        bigQueryDataOperator.insertRow(TestData.clientFirst, false);
        List<FieldValueList> selectResultList = bigQueryDataOperator.simpleSelect(selectMandatoryFieldsTable);

        Assert.assertEquals(numberOfTestRowsBeforeInsert, selectResultList.size());
    }

}