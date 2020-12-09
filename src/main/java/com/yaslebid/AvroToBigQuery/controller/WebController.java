package com.yaslebid.AvroToBigQuery.controller;

import java.util.Base64;
import com.yaslebid.AvroToBigQuery.controller.model.Body;
import com.yaslebid.AvroToBigQuery.controller.model.Message;
import com.yaslebid.AvroToBigQuery.helpers.GCPObjectsOperator;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BigQueryRowInsertException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobNotExistsException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.BlobRenameException;
import com.yaslebid.AvroToBigQuery.helpers.exceptions.IncorrectAvroFileException;
import com.yaslebid.AvroToBigQuery.service.FileToBigQueryJobProcessor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@RestController
@AllArgsConstructor
@Slf4j
public class WebController {

    private final FileToBigQueryJobProcessor fileToBigQueryJobProcessor;
    private final GCPObjectsOperator blobObjectsOperator;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity<String> receiveMessage(@RequestBody Body body) {
        String decodedPubSubMessage;
        String fileName = "";
        JsonObject data;
        Message message;
        boolean isSuccess = false;

        // Check if PubSub message not empty
        message = body.getMessage();
        if (message == null) {
            String msg = "Bad Request: Invalid PubSub message format";
            log.error(msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        // Get uploaded file name from PubSub message
        try {
            decodedPubSubMessage = new String(Base64.getDecoder().decode(message.getData()));
            data = JsonParser.parseString(decodedPubSubMessage).getAsJsonObject();
            fileName = data.get("name").getAsString();
            log.info("PubSub message received. File '" + fileName + "' uploaded to the bucket");
        } catch (Exception exception) {
            String msg = "Bad Request: Invalid PubSub message: data is not valid base64 encoded JSON or incorrect";
            log.error(msg, exception.getMessage());
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        if (fileName.endsWith(".avro")) {
            try {
                isSuccess = fileToBigQueryJobProcessor.executeTasks(fileName);
            } catch (IncorrectAvroFileException | BigQueryRowInsertException exception) {
                log.error("WC exception message: " + exception.getMessage());
                blobObjectsOperator.renameBlobObject(fileName, false);
            } catch (BlobNotExistsException | BlobRenameException exception) {
                log.error("WC exception message: " + exception.getMessage());
            }
        }

        // OK when file processed, NO_CONTENT if file not .avro or failed to be processed
        return isSuccess ? new ResponseEntity<>(fileName, HttpStatus.OK)
                : new ResponseEntity<>(fileName, HttpStatus.NO_CONTENT);

    }
}
