// src/main/java/com/streaming/apps/SalesAggregationApp.java
package com.streaming.apps;

import com.streaming.core.ConfigLoader;
import com.streaming.core.StreamingApp;
import com.streaming.exceptions.StreamProcessingException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.functions.*;

/**
 * Sample streaming application that reads sales data from HDFS,
 * performs aggregations, and outputs to console.
 * 
 * Automatically registered and discovered by the StreamingAppRegistry.
 */
public class SalesAggregationApp implements StreamingApp {
    
    private static final Logger logger = LoggerFactory.getLogger(SalesAggregationApp.class);
    
    @Override
    public String getAppName() {
        return "sales-aggregation";
    }
    
    @Override
    public String getConfigFileName() {
        return "sales-aggregation.properties";
    }
    
    @Override
    public void run(SparkSession spark, Properties config) throws Exception {
        logger.info("Starting Sales Aggregation Streaming Application");
        
        try {
            // Load configuration
            String inputPath = ConfigLoader.getRequiredProperty(config, "input.path");
            String checkpointPath = ConfigLoader.getRequiredProperty(config, "checkpoint.path");
            int triggerInterval = ConfigLoader.getIntProperty(config, "trigger.interval.seconds", 10);
            int maxFilesPerTrigger = ConfigLoader.getIntProperty(config, "max.files.per.trigger", 10);
            boolean enableWatermark = ConfigLoader.getBooleanProperty(config, "enable.watermark", true);
            int watermarkDelayMinutes = ConfigLoader.getIntProperty(config, "watermark.delay.minutes", 10);
            
            logger.info("Configuration loaded - Input: {}, Checkpoint: {}", inputPath, checkpointPath);
            
            // Define schema for sales data
            StructType schema = defineSchema();
            
            // Read streaming data from HDFS
            Dataset<Row> salesStream = spark
                .readStream()
                .format("parquet")
                .option("maxFilesPerTrigger", maxFilesPerTrigger)
                .option("latestFirst", "true")
                .option("fileNameOnly", "false")
                .schema(schema)
                .load(inputPath);
            
            logger.info("Created streaming source from: {}", inputPath);
            
            // Apply transformations and aggregations
            Dataset<Row> processedStream = processStream(salesStream, enableWatermark, watermarkDelayMinutes);
            
            // Write to console (you can change to other sinks as needed)
            StreamingQuery query = processedStream
                .writeStream()
                .outputMode("update")  // Use "complete" for aggregations without watermark
                .format("console")
                .option("truncate", "false")
                .option("numRows", "100")
                .trigger(Trigger.ProcessingTime(triggerInterval, TimeUnit.SECONDS))
                .option("checkpointLocation", checkpointPath)
                .queryName("SalesAggregationQuery")
                .start();
            
            logger.info("Streaming query started successfully");
            logger.info("Query ID: {}", query.id());
            logger.info("Query Name: {}", query.name());
            
            // Monitor the query
            monitorQuery(query);
            
        } catch (Exception e) {
            logger.error("Error in Sales Aggregation App", e);
            throw new StreamProcessingException("Failed to process sales stream", e);
        }
    }
    
    /**
     * Defines the schema for sales data.
     */
    private StructType defineSchema() {
        return new StructType(new StructField[]{
            DataTypes.createStructField("transaction_id", DataTypes.StringType, false),
            DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
            DataTypes.createStructField("product_id", DataTypes.StringType, false),
            DataTypes.createStructField("product_name", DataTypes.StringType, false),
            DataTypes.createStructField("category", DataTypes.StringType, false),
            DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
            DataTypes.createStructField("price", DataTypes.DoubleType, false),
            DataTypes.createStructField("customer_id", DataTypes.StringType, false),
            DataTypes.createStructField("customer_country", DataTypes.StringType, false),
            DataTypes.createStructField("payment_method", DataTypes.StringType, false)
        });
    }
    
    /**
     * Processes the sales stream with enrichment and aggregations.
     */
    private Dataset<Row> processStream(Dataset<Row> salesStream, boolean enableWatermark, 
                                       int watermarkDelayMinutes) {
        
        logger.info("Applying transformations and aggregations");
        
        // Add enrichment: calculate total amount
        Dataset<Row> enrichedStream = salesStream
            .withColumn("total_amount", 
                col("quantity").multiply(col("price")))
            .withColumn("processing_time", current_timestamp());
        
        // Apply watermark if enabled (for handling late data)
        if (enableWatermark) {
            enrichedStream = enrichedStream
                .withWatermark("timestamp", watermarkDelayMinutes + " minutes");
            logger.info("Watermark applied: {} minutes", watermarkDelayMinutes);
        }
        
        // Perform multiple aggregations
        
        // 1. Sales by category and country (windowed aggregation)
        Dataset<Row> categoryCountryAgg = enrichedStream
            .groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("category"),
                col("customer_country")
            )
            .agg(
                count("*").as("transaction_count"),
                sum("quantity").as("total_quantity"),
                sum("total_amount").as("total_revenue"),
                avg("total_amount").as("avg_transaction_value"),
                min("total_amount").as("min_transaction"),
                max("total_amount").as("max_transaction")
            )
            .select(
                col("window.start").as("window_start"),
                col("window.end").as("window_end"),
                col("category"),
                col("customer_country"),
                col("transaction_count"),
                col("total_quantity"),
                round(col("total_revenue"), 2).as("total_revenue"),
                round(col("avg_transaction_value"), 2).as("avg_transaction_value"),
                round(col("min_transaction"), 2).as("min_transaction"),
                round(col("max_transaction"), 2).as("max_transaction")
            );
        
        // 2. Top products by revenue
        Dataset<Row> topProducts = enrichedStream
            .groupBy(
                window(col("timestamp"), "10 minutes", "2 minutes"),
                col("product_id"),
                col("product_name"),
                col("category")
            )
            .agg(
                sum("total_amount").as("product_revenue"),
                sum("quantity").as("units_sold")
            )
            .select(
                col("window.start").as("window_start"),
                col("window.end").as("window_end"),
                col("product_id"),
                col("product_name"),
                col("category"),
                round(col("product_revenue"), 2).as("product_revenue"),
                col("units_sold")
            );
        
        // 3. Payment method distribution
        Dataset<Row> paymentMethodAgg = enrichedStream
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("payment_method")
            )
            .agg(
                count("*").as("transaction_count"),
                sum("total_amount").as("total_amount")
            )
            .select(
                col("window.start").as("window_start"),
                col("window.end").as("window_end"),
                col("payment_method"),
                col("transaction_count"),
                round(col("total_amount"), 2).as("total_amount")
            );
        
        // For demonstration, we'll return the category-country aggregation
        // In a real scenario, you might write each to different sinks
        return categoryCountryAgg;
    }
    
    /**
     * Monitors the streaming query and logs progress.
     */
    private void monitorQuery(StreamingQuery query) {
        // Start a monitoring thread
        Thread monitorThread = new Thread(() -> {
            try {
                while (query.isActive()) {
                    Thread.sleep(30000); // Monitor every 30 seconds
                    
                    logger.info("=== Query Status ===");
                    logger.info("Query ID: {}", query.id());
                    logger.info("Is Active: {}", query.isActive());
                    logger.info("Recent Progress: {}", query.lastProgress());
                    
                    if (query.lastProgress() != null) {
                        logger.info("Input Rows: {}", 
                            query.lastProgress().numInputRows());
                        logger.info("Processed Rows: {}", 
                            query.lastProgress().processedRowsPerSecond());
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Monitor thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
        
        monitorThread.setDaemon(true);
        monitorThread.setName("QueryMonitor-" + query.name());
        monitorThread.start();
    }
}
