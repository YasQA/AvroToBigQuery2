package com.yaslebid.AvroToBigQuery.services;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebController {

    public WebController() {
    }

    @GetMapping(path = "/")
    public String simpleResponse() {
        return "OK";
    }
}