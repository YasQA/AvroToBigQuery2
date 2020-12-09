package com.yaslebid.AvroToBigQuery.TestConfigutationsAndData;

import com.yaslebid.avro.Client;
import java.util.List;

public class TestData {
    public static final Client clientFirst =
            new Client(1L, "John", "111-111-1111", "NY, Street 1, 111");
    public static final Client clientSecond =
            new Client(2L, "Jim", "222-222-222", "London, Street 2, 2222");

    public static final List<Client> listOfOneClient = List.of(clientFirst);
    public static final List<Client> listOfTwoClients = List.of(clientFirst, clientSecond);

    //both files located in resources
    public static final String correctFileName = "testFileX.avro";
    public static final String fakeFileName = "fakeFile.avro";

    // not converted to base64 'data' is: {"name": "testFileX.avro", "bucket": "pubSubBucket"}
    public static final String pubSubTestMessageCorrect = "{\n" +
            "    \"message\": {\n" +
            "        \"attributes\": {\n" +
            "            \"key\": \"value\"\n" +
            "        },\n" +
            "        \"data\": \"eyAibmFtZSI6ICJ0ZXN0RmlsZVguYXZybyIsICJidWNrZXQiOiAicHViU3ViQnVja2V0In0=\",\n" +
            "        \"messageId\": \"136969346945\"\n" +
            "    },\n" +
            "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
            "}";

    // not converted  to base64 'data' is: {"name": "", "bucket": "pubSubBucket"}
    public static final String pubSubWrongTestMessageEmptyName = "{\n" +
            "    \"message\": {\n" +
            "        \"attributes\": {\n" +
            "            \"key\": \"value\"\n" +
            "        },\n" +
            "        \"data\": \"Im5hbWUiOiAiIiwgImJ1Y2tldCI6ICJwdWJTdWJCdWNrZXQi\",\n" +
            "        \"messageId\": \"136969346945\"\n" +
            "    },\n" +
            "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
            "}";

    public static final String pubSubWrongTestMessageWrongFormat = "{\n" +
            "   \"subscription\": \"projects/avroparserapplication/subscriptions/pubSubSubscription\"\n" +
            "}";

}
