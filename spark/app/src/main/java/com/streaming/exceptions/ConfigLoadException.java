// src/main/java/com/streaming/exceptions/ConfigLoadException.java
package com.streaming.exceptions;

/**
 * Exception thrown when configuration loading fails.
 */
public class ConfigLoadException extends RuntimeException {
    
    public ConfigLoadException(String message) {
        super(message);
    }
    
    public ConfigLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
