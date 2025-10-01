// spark/app/src/main/java/com/streaming/ParquetStreamReader.java
package com.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class ParquetStreamReader {
    private static final Logger logger = LoggerFactory.getLogger(ParquetStreamReader.class);
    
    // Configuration constants
    private static final String APP_NAME = "Parquet Stream Reader";
    private static final String HDFS_INPUT_PATH = "hdfs://namenode:8020/input/streaming-data";
    private static final int TRIGGER_INTERVAL_SECONDS = 10;
    
    public static void main(String[] args) {
        logger.info("Starting Parquet Stream Reader Application...");
        
        // Create Spark Session
        SparkSession spark = createSparkSession();
        
        try {
            // Read streaming data
            Dataset<Row> streamingData = readParquetStream(spark);
            
            // Start the streaming query
            StreamingQuery query = startStreamingQuery(streamingData);
            
            // Wait for termination
            query.awaitTermination();
            
        } catch (TimeoutException | StreamingQueryException e) {
            logger.error("Error in streaming query: ", e);
        } finally {
            spark.stop();
            logger.info("Spark session stopped.");
        }
    }
    
    /**
     * Creates and configures Spark Session for YARN
     */
    private static SparkSession createSparkSession() {
        logger.info("Creating Spark Session...");
        
        SparkSession spark = SparkSession.builder()
                .appName(APP_NAME)
                .master("yarn")
                .config("spark.submit.deployMode", "client")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
                .config("spark.yarn.resourcemanager.hostname", "resourcemanager")
                .config("spark.yarn.resourcemanager.address", "resourcemanager:8032")
                .config("spark.executor.memory", "2g")
                .config("spark.executor.cores", "1")
                .config("spark.executor.instances", "2")
                .config("spark.driver.memory", "1g")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("WARN");
        logger.info("Spark Session created successfully.");
        
        return spark;
    }
    
    /**
     * Reads Parquet files as a stream from HDFS
     */
    private static Dataset<Row> readParquetStream(SparkSession spark) {
        logger.info("Setting up Parquet stream reader from: {}", HDFS_INPUT_PATH);
        
        // Define schema inference or provide explicit schema
        // For now, we'll use schema inference
        Dataset<Row> streamingDF = spark
                .readStream()
                .format("parquet")
                .option("path", HDFS_INPUT_PATH)
                .option("maxFilesPerTrigger", "1")  // Process one file at a time
                .option("latestFirst", "false")      // Process oldest files first
                .load();
        
        logger.info("Schema of incoming data:");
        streamingDF.printSchema();
        
        return streamingDF;
    }
    
    /**
     * Starts the streaming query with console output
     */
    private static StreamingQuery startStreamingQuery(Dataset<Row> streamingData) 
            throws TimeoutException {
        logger.info("Starting streaming query...");
        
        StreamingQuery query = streamingData
                .writeStream()
                .format("console")
                .option("truncate", "false")
                .option("numRows", "100")
                .trigger(Trigger.ProcessingTime(TRIGGER_INTERVAL_SECONDS + " seconds"))
                .outputMode("append")
                .start();
        
        logger.info("Streaming query started. Query ID: {}", query.id());
        logger.info("Monitoring path: {}", HDFS_INPUT_PATH);
        logger.info("Trigger interval: {} seconds", TRIGGER_INTERVAL_SECONDS);
        
        return query;
    }
}
