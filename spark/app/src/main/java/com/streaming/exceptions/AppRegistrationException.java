// src/main/java/com/streaming/exceptions/AppRegistrationException.java
package com.streaming.exceptions;

/**
 * Exception thrown when there are issues with application registration.
 */
public class AppRegistrationException extends RuntimeException {
    
    public AppRegistrationException(String message) {
        super(message);
    }
    
    public AppRegistrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
