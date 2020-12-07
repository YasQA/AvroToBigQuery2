package com.yaslebid.AvroToBigQuery.helpers.exceptions;

import com.yaslebid.AvroToBigQuery.helpers.AvroFileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.helpers.FileDeserializerForClient;
import com.yaslebid.AvroToBigQuery.repository.BigQueryDataOperator;
import com.yaslebid.AvroToBigQuery.repository.DBDataOperator;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Profile("test1")
@Configuration
public class BlobRenameExceptionTestConfiguration {

    @Bean
    @Primary
    public FileDeserializerForClient avroFileDeserializerForClientM() {
        return Mockito.mock(AvroFileDeserializerForClient.class);
    }

    @Bean
    @Primary
    public DBDataOperator bigQueryDataOperatorM() {
        return Mockito.mock(BigQueryDataOperator.class);
    }
}
