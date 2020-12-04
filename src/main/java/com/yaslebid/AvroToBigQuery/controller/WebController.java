package com.yaslebid.AvroToBigQuery.controller;

import java.util.Base64;
import com.yaslebid.AvroToBigQuery.controller.model.Body;
import com.yaslebid.AvroToBigQuery.controller.model.Message;
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

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity<String> receiveMessage(@RequestBody Body body) {
        String decodedPubSubMessage;
        String fileName;
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
        } catch (Exception e) {
            String msg = "Bad Request: Invalid PubSub message: data is not valid base64 encoded JSON or incorrect";
            log.error(msg);
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        if (fileName.endsWith(".avro")) {
            isSuccess = fileToBigQueryJobProcessor.executeTasks(fileName);
        }

        return isSuccess ? new ResponseEntity<>(fileName, HttpStatus.OK) // file processed
                : new ResponseEntity<>(HttpStatus.NO_CONTENT); // if file not .avro or failed to process

    }

}