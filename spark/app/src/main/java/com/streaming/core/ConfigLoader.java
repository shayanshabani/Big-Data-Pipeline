// src/main/java/com/streaming/core/ConfigLoader.java
package com.streaming.core;

import com.streaming.exceptions.ConfigLoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class for loading configuration files.
 */
public class ConfigLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String CONFIG_BASE_PATH = "apps/";
    
    /**
     * Loads configuration from a properties file.
     * 
     * @param fileName The name of the configuration file (relative to resources/apps/)
     * @return Properties object containing the configuration
     * @throws ConfigLoadException if the file cannot be loaded
     */
    public static Properties loadConfig(String fileName) {
        Properties properties = new Properties();
        String configPath = CONFIG_BASE_PATH + fileName;
        
        logger.info("Loading configuration from: {}", configPath);
        
        try (InputStream input = ConfigLoader.class.getClassLoader()
                .getResourceAsStream(configPath)) {
            
            if (input == null) {
                throw new ConfigLoadException(
                    "Configuration file not found: " + configPath);
            }
            
            properties.load(input);
            logger.info("Successfully loaded {} configuration properties", 
                properties.size());
            
            // Log loaded properties (excluding sensitive data)
            properties.forEach((key, value) -> {
                if (!key.toString().toLowerCase().contains("password") &&
                    !key.toString().toLowerCase().contains("secret")) {
                    logger.debug("Config: {} = {}", key, value);
                }
            });
            
            return properties;
            
        } catch (IOException e) {
            throw new ConfigLoadException(
                "Failed to load configuration file: " + configPath, e);
        }
    }
    
    /**
     * Gets a required configuration value.
     * 
     * @param properties The properties object
     * @param key The configuration key
     * @return The configuration value
     * @throws ConfigLoadException if the key is not found
     */
    public static String getRequiredProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigLoadException(
                "Required configuration property not found: " + key);
        }
        return value.trim();
    }
    
    /**
     * Gets an optional configuration value with a default.
     * 
     * @param properties The properties object
     * @param key The configuration key
     * @param defaultValue The default value if key is not found
     * @return The configuration value or default
     */
    public static String getProperty(Properties properties, String key, String defaultValue) {
        return properties.getProperty(key, defaultValue).trim();
    }
    
    /**
     * Gets an integer configuration value.
     * 
     * @param properties The properties object
     * @param key The configuration key
     * @param defaultValue The default value if key is not found
     * @return The integer value
     * @throws ConfigLoadException if the value cannot be parsed as integer
     */
    public static int getIntProperty(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigLoadException(
                "Invalid integer value for property " + key + ": " + value, e);
        }
    }
    
    /**
     * Gets a long configuration value.
     * 
     * @param properties The properties object
     * @param key The configuration key
     * @param defaultValue The default value if key is not found
     * @return The long value
     * @throws ConfigLoadException if the value cannot be parsed as long
     */
    public static long getLongProperty(Properties properties, String key, long defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigLoadException(
                "Invalid long value for property " + key + ": " + value, e);
        }
    }
    
    /**
     * Gets a boolean configuration value.
     * 
     * @param properties The properties object
     * @param key The configuration key
     * @param defaultValue The default value if key is not found
     * @return The boolean value
     */
    public static boolean getBooleanProperty(Properties properties, String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
