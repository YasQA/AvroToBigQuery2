package com.yaslebid.AvroToBigQuery.controller;

import com.yaslebid.AvroToBigQuery.TestConfigutationsAndData.TestData;
import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BigQueryRowInsertException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobNotExistsException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobRenameException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.IncorrectAvroFileException;
import com.yaslebid.AvroToBigQuery.repository.DBDataOperator;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
class WebControllerTest {
    @Autowired
    private MockMvc mvc;
    @SpyBean
    private FileDeserializerForClient avroFileDeserializerForClient;
    @SpyBean
    private DBDataOperator bigQueryDataOperator;
    @SpyBean
    private GCPObjectsOperator blobObjectsOperator;

    @Test
    public void getFileNameFromPubSubMessage() throws Exception {
        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubTestMessageCorrect))
                .andDo(print())
                .andExpect(status().is2xxSuccessful()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(TestData.correctFileName, content);
    }

    @Test
    public void getFileNameFromPubSubMessage_fileNameEmpty() throws Exception {
        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubWrongTestMessageEmptyName))
                .andDo(print())
                .andExpect(status().isBadRequest()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals("Bad Request: Invalid PubSub message: " +
                "data is not valid base64 encoded JSON or incorrect", content);
    }

    @Test
    public void getFileNameFromPubSubMessage_wrongMessageFormat() throws Exception {
        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubWrongTestMessageWrongFormat))
                .andDo(print())
                .andExpect(status().isBadRequest()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals("Bad Request: Invalid PubSub message format", content);
    }

    @Test
    public void controllerBehaviour_whenIncorrectAvroFileException() throws Exception {
        doThrow(IncorrectAvroFileException.class)
                .when(avroFileDeserializerForClient).getClientListFromFile(anyString());
        doReturn(true).when(blobObjectsOperator).renameBlobObject(TestData.correctFileName,false);

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubTestMessageCorrect))
                .andDo(print())
                .andExpect(status().isNoContent()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(TestData.correctFileName, content);
    }

    @Test
    public void controllerBehaviour_whenBlobNotExistsException() throws Exception {
        doThrow(BlobNotExistsException.class)
                .when(avroFileDeserializerForClient).getClientListFromFile(anyString());

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubTestMessageCorrect))
                .andDo(print())
                .andExpect(status().isNoContent()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(TestData.correctFileName, content);
    }

    @Test
    public void controllerBehaviour_whenBigQueryRowInsertException() throws Exception {
        doReturn(TestData.listOfOneClient)
                .when(avroFileDeserializerForClient).getClientListFromFile(anyString());
        doReturn(true).when(blobObjectsOperator).renameBlobObject(TestData.correctFileName,false);

        doThrow(BigQueryRowInsertException.class)
                .when(bigQueryDataOperator).insertRow(TestData.clientFirst, true);

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubTestMessageCorrect))
                .andDo(print())
                .andExpect(status().isNoContent()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(TestData.correctFileName, content);
    }

    @Test
    public void controllerBehaviour_whenBlobRenameException() throws Exception {
        doThrow(BlobRenameException.class)
                .when(avroFileDeserializerForClient).getClientListFromFile(anyString());

        doReturn(true)
                .when(bigQueryDataOperator).insertRow(TestData.clientFirst, true);
        doReturn(true)
                .when(bigQueryDataOperator).insertRow(TestData.clientFirst, false);

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestData.pubSubTestMessageCorrect))
                .andDo(print())
                .andExpect(status().isNoContent()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals(TestData.correctFileName, content);
    }
}