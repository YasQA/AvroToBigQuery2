package com.yaslebid.AvroToBigQuery.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.google.cloud.storage.Blob;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.avro.Client;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.stereotype.Component;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Component
public class AvroFileDeserializerForClient implements FileDeserializerForClient {

    public List<Client> getClientListFromFile(String fileName) {
        List<Client> clientsList = new ArrayList<>();

        try {
            Blob sourceBlob = GCPResources.BUCKET.get(fileName);

            SeekableByteArrayInput input = new SeekableByteArrayInput(sourceBlob.getContent());
            DatumReader<Client> datumReader = new SpecificDatumReader<Client>(Client.class);
            DataFileReader<Client> dataFileReader = new DataFileReader<Client>(input, datumReader);

            while (dataFileReader.hasNext()) {
                Client client = dataFileReader.next();
                clientsList.add(client);
            }
        } catch (IOException exception) {
            LOGGER.error("File not found \n" + exception.getMessage());
        }
        return clientsList;
    }
}