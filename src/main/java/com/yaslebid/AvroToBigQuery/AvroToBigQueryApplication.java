package com.yaslebid.AvroToBigQuery;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class AvroToBigQueryApplication {

	public static void main(String[] args) {
		SpringApplication.run(AvroToBigQueryApplication.class, args);
	}

}
