package com.ebay.challenge.streamprocessor.infrastructure;


public class OutputSinkException extends RuntimeException {

    public OutputSinkException(String message) {
        super(message);
    }

    public OutputSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}