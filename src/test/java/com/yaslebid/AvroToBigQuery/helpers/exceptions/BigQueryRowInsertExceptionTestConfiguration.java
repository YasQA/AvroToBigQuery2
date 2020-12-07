package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.google.cloud.bigquery.BigQuery;
import com.yaslebid.AvroToBigQuery.helpers.AvroFileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Profile("test2")
@Configuration
public class BigQueryRowInsertExceptionTestConfiguration {

    @Bean
    @Primary
    public FileDeserializerForClient avroFileDeserializerForClientM() {
        return Mockito.mock(AvroFileDeserializerForClient.class);
    }

    @Bean
    @Primary
    public BigQuery bigQuery() {
        return Mockito.mock(BigQuery.class);
    }
}
