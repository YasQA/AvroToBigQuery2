package com.yaslebid.AvroToBigQuery.controller.model;

import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class Body {
    @Getter
    private Message message;
}