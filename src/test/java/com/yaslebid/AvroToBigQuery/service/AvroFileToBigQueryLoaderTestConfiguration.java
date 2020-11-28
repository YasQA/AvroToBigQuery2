package com.yaslebid.AvroToBigQuery.service;

import com.yaslebid.AvroToBigQuery.repository.AvroFileToBigQueryLoader;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Profile("test")
@Configuration
public class AvroFileToBigQueryLoaderTestConfiguration {
    @Bean
    @Primary
    public AvroFileToBigQueryLoader nameService() {
        return Mockito.mock(AvroFileToBigQueryLoader.class);
    }
}


