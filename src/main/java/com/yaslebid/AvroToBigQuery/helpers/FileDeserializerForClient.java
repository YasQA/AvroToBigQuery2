package com.yaslebid.AvroToBigQuery.helpers;

import com.yaslebid.avro.Client;
import java.util.List;

public interface FileDeserializerForClient {
    List<Client> getClientListFromFile(String fileName);
}
