// src/main/java/com/streaming/core/StreamingJobSubmitter.java
package com.streaming.core;

import com.streaming.exceptions.AppRegistrationException;
import com.streaming.exceptions.ConfigLoadException;
import com.streaming.exceptions.StreamProcessingException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Main entry point for submitting streaming jobs.
 * Usage: spark-submit --class com.streaming.core.StreamingJobSubmitter <jar-file> <app-name>
 */
public class StreamingJobSubmitter {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobSubmitter.class);
    
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsageAndExit();
        }
        
        String appName = args[0];
        logger.info("Starting Spark Structured Streaming Job: {}", appName);
        
        SparkSession spark = null;
        
        try {
            // Get the streaming app from registry
            StreamingApp app = StreamingAppRegistry.getApp(appName);
            logger.info("Found application: {}", app.getClass().getName());
            
            // Load configuration
            String configFileName = app.getConfigFileName();
            Properties config = ConfigLoader.loadConfig(configFileName);
            
            // Create Spark Session
            spark = createSparkSession(appName, config);
            
            // Run the application
            logger.info("Executing streaming application: {}", appName);
            app.run(spark, config);
            
            // Wait for termination
            spark.streams().awaitAnyTermination();
            
        } catch (AppRegistrationException e) {
            logger.error("Application registration error", e);
            printAvailableApps();
            System.exit(1);
            
        } catch (ConfigLoadException e) {
            logger.error("Configuration loading error", e);
            System.exit(2);
            
        } catch (StreamProcessingException e) {
            logger.error("Stream processing error", e);
            System.exit(3);
            
        } catch (Exception e) {
            logger.error("Unexpected error occurred", e);
            System.exit(4);
            
        } finally {
            if (spark != null) {
                logger.info("Stopping Spark session");
                spark.stop();
            }
        }
    }
    
    /**
     * Creates a SparkSession with optimized settings for large-scale processing.
     */
    private static SparkSession createSparkSession(String appName, Properties config) {
        logger.info("Creating Spark session for: {}", appName);
        
        SparkSession.Builder builder = SparkSession.builder()
            .appName(appName)
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.shuffle.partitions", 
                config.getProperty("spark.sql.shuffle.partitions", "100"))
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")
            // Optimize for large data processing
            .config("spark.executor.memory", 
                config.getProperty("spark.executor.memory", "1g"))
            .config("spark.executor.cores", 
                config.getProperty("spark.executor.cores", "2"))
            .config("spark.driver.memory", 
                config.getProperty("spark.driver.memory", "512m"))
            .config("spark.streaming.backpressure.enabled", "true")
            .config("spark.sql.streaming.fileSource.log.cleanupDelay", "3600000")
            // Parquet optimization
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.files.maxPartitionBytes", "134217728") // 128 MB
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.parquet.int96AsTimestamp", "true");
        
        // Add any custom Spark configurations from config file
        config.forEach((key, value) -> {
            String keyStr = key.toString();
            if (keyStr.startsWith("spark.")) {
                builder.config(keyStr, value.toString());
            }
        });
        
        return builder.getOrCreate();
    }
    
    private static void printUsageAndExit() {
        System.err.println("Usage: spark-submit --class com.streaming.core.StreamingJobSubmitter " +
            "<jar-file> <app-name>");
        System.err.println();
        printAvailableApps();
        System.exit(1);
    }
    
    private static void printAvailableApps() {
        System.err.println("Available applications:");
        StreamingAppRegistry.getRegisteredAppNames().forEach(name -> 
            System.err.println("  - " + name));
    }
}
