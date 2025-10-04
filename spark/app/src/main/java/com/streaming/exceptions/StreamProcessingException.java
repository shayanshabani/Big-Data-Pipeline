// src/main/java/com/streaming/exceptions/StreamProcessingException.java
package com.streaming.exceptions;

/**
 * Exception thrown when stream processing encounters errors.
 */
public class StreamProcessingException extends RuntimeException {
    
    public StreamProcessingException(String message) {
        super(message);
    }
    
    public StreamProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
