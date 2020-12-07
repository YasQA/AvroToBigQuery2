package com.yaslebid.AvroToBigQuery.helpers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.google.cloud.storage.Blob;
import com.yaslebid.AvroToBigQuery.config.GCPResources;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobNotExistsException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.IncorrectAvroFileException;
import com.yaslebid.avro.Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class AvroFileDeserializerForClient implements FileDeserializerForClient {

    public List<Client> getClientListFromFile(String fileName) {
        List<Client> clientsList = new ArrayList<>();

        try {
            Blob sourceBlob = GCPResources.BUCKET.get(fileName);
            SeekableByteArrayInput input = new SeekableByteArrayInput(sourceBlob.getContent());
            DatumReader<Client> datumReader = new SpecificDatumReader<>(Client.class);
            DataFileReader<Client> dataFileReader = new DataFileReader<>(input, datumReader);

            while (dataFileReader.hasNext()) {
                Client client = dataFileReader.next();
                clientsList.add(client);
            }
            log.info("File '" + fileName + "' processed, list of Clients collected");
        } catch (IOException exception) {
            log.error("File/Blob '" + fileName + "' is not correct Avro file. Error message: " + exception.getMessage());
            throw new IncorrectAvroFileException(fileName, exception);
        } catch (NullPointerException exception) {
            log.error("File/Blob is not available. Error message: " + exception.getMessage());
            throw new BlobNotExistsException(fileName, exception);
        }

        return clientsList;
    }
}