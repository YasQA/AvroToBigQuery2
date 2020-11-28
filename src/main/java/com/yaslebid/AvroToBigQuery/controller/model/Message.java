package com.yaslebid.AvroToBigQuery.controller.model;

public class Message {

    private String messageId;
    private String publishTime;
    private String data;

    public Message() {}

    public Message(String messageId, String publishTime, String data) {
        this.messageId = messageId;
        this.publishTime = publishTime;
        this.data = data;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getPublishTime() {
        return publishTime;
    }

    public String getData() {
        return data;
    }
}
