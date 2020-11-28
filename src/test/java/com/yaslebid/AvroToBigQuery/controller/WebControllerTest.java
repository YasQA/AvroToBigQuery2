package com.yaslebid.AvroToBigQuery.controller;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.http.StreamingHttpOutputMessage;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
class WebControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void getFileNameFromPubSubMessage() throws Exception {
        // not converted 'data' is: {"name": "testFileX.avro", "bucket": "pubSubBucket"}
        String pubSubTestMessage = "{\n" +
                "    \"message\": {\n" +
                "        \"attributes\": {\n" +
                "            \"key\": \"value\"\n" +
                "        },\n" +
                "        \"data\": \"eyAibmFtZSI6ICJ0ZXN0RmlsZVguYXZybyIsICJidWNrZXQiOiAicHViU3ViQnVja2V0In0=\",\n" +
                "        \"messageId\": \"136969346945\"\n" +
                "    },\n" +
                "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
                "}";

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(pubSubTestMessage))
                .andDo(print())
                .andExpect(status().is2xxSuccessful()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals("testFileX.avro", content);
    }

    @Test
    void getFileNameFromPubSubMessage_fileNameEmpty_negativeTest() throws Exception {
        // not converted 'data' is: {"name": "", "bucket": "pubSubBucket"}
        String pubSubTestMessage = "{\n" +
                "    \"message\": {\n" +
                "        \"attributes\": {\n" +
                "            \"key\": \"value\"\n" +
                "        },\n" +
                "        \"data\": \"Im5hbWUiOiAiIiwgImJ1Y2tldCI6ICJwdWJTdWJCdWNrZXQi\",\n" +
                "        \"messageId\": \"136969346945\"\n" +
                "    },\n" +
                "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
                "}";

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(pubSubTestMessage))
                .andDo(print())
                .andExpect(status().is4xxClientError()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals("Bad Request: Invalid PubSub message: data is not valid base64 encoded JSON or incorrect", content);
    }

    @Test
    void getFileNameFromPubSubMessage_wrongFormat_negativeTest() throws Exception {
        String pubSubTestMessage = "{\n" +
                "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
                "}";

        MvcResult result = this.mvc.perform(post("/")
                .contentType(MediaType.APPLICATION_JSON)
                .content(pubSubTestMessage))
                .andDo(print())
                .andExpect(status().is4xxClientError()).andReturn();

        String content = result.getResponse().getContentAsString();
        Assert.assertEquals("Bad Request: Invalid PubSub message format", content);
    }
}

