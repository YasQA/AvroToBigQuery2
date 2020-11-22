package com.yaslebid.AvroToBigQuery.services;

import com.yaslebid.AvroToBigQuery.config.BigQueryConfiguration;
import com.google.cloud.pubsub.v1.Subscriber;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import static com.yaslebid.AvroToBigQuery.AvroToBigQueryApplication.LOGGER;

@Service
public class PubSubSubscriptionFileProcessor {
    @Autowired
    PubSubTemplate pubSubTemplate;

    @Autowired
    AvroToBigQueryJobProcessor avroProcessor;

    @Bean
    public void subscribeAndProcess() {
        Subscriber subscriber = pubSubTemplate.subscribe(BigQueryConfiguration.SUBSCRIPTION_NAME, (message) -> {

            String fileName = message
                    .getPubsubMessage()
                    .getAttributesMap()
                    .get("objectId");

            LOGGER.info("Message received from " + BigQueryConfiguration.SUBSCRIPTION_NAME
                    + " subscription for new file: " + fileName);

            avroProcessor.execute(fileName);
            message.ack();
        });

    }
}
