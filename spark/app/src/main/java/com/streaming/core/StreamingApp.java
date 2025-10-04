// src/main/java/com/streaming/core/StreamingApp.java
package com.streaming.core;

import org.apache.spark.sql.SparkSession;
import java.util.Properties;

/**
 * Base interface for all streaming applications.
 * Each streaming app must implement this interface and will be auto-registered.
 */
public interface StreamingApp {
    
    /**
     * Returns the unique name/identifier for this streaming application.
     * This name will be used to invoke the app via command line.
     * 
     * @return The application name
     */
    String getAppName();
    
    /**
     * Executes the streaming application logic.
     * 
     * @param spark The SparkSession instance
     * @param config The configuration properties loaded from the app's config file
     * @throws Exception if any error occurs during execution
     */
    void run(SparkSession spark, Properties config) throws Exception;
    
    /**
     * Returns the default configuration file name for this app.
     * The file should be located in resources/apps/ directory.
     * 
     * @return The configuration file name
     */
    String getConfigFileName();
}
