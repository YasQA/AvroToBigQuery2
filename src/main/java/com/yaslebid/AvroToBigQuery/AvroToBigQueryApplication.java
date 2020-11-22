package com.yaslebid.AvroToBigQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AvroToBigQueryApplication {
	public static final Logger LOGGER =
			LoggerFactory.getLogger(AvroToBigQueryApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AvroToBigQueryApplication.class, args);
	}

}
