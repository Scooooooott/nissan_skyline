package com.ebay.challenge.streamprocessor.infrastructure;


public class InvalidKafkaRecordException extends RuntimeException {

    public InvalidKafkaRecordException(String message) {
        super(message);
    }

    public InvalidKafkaRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}