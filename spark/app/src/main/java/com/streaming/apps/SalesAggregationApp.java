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
 * performs aggregations, and outputs to Kafka.
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
            String referencesPath = ConfigLoader.getRequiredProperty(config, "references.path");
            String checkpointPath = ConfigLoader.getRequiredProperty(config, "checkpoint.path");
            int triggerInterval = ConfigLoader.getIntProperty(config, "trigger.interval.seconds", 10);
            int maxFilesPerTrigger = ConfigLoader.getIntProperty(config, "max.files.per.trigger", 1);
            boolean enableWatermark = ConfigLoader.getBooleanProperty(config, "enable.watermark", true);
            int watermarkDelayMinutes = ConfigLoader.getIntProperty(config, "watermark.delay.minutes", 60);
            
            // Kafka configuration
            String kafkaBrokers = ConfigLoader.getRequiredProperty(config, "kafka.bootstrap.servers");
            String kafkaTopic = ConfigLoader.getRequiredProperty(config, "kafka.topic");

            logger.info("Configuration loaded - Input: {}, Checkpoint: {}", inputPath, checkpointPath);
            logger.info("Kafka Configuration - Brokers: {}, Topic: {}", kafkaBrokers, kafkaTopic);

            String fourGPath = referencesPath.concat("/SQM_4GCell.csv");
            String twoGPath = referencesPath.concat("/SQM_2GCell.csv");

            Dataset<Row> fourG = spark.read()
                .option("header", "true")
                .csv(fourGPath)
                .toDF("eNodeB_ID_Hex", "Cell_ID_Hex" ,"Cell_Name");

            fourG = fourG.withColumnRenamed("Cell_Name", "Cell_Name_4G");


            Dataset<Row> twoG = spark.read()
                .option("header", "true")
                .csv(twoGPath)
                .toDF("LAC", "Cell_Code" ,"Cell_Name");

            twoG = twoG.withColumnRenamed("Cell_Name", "Cell_Name_2G");

            // Define schema for sales data
            // StructType schema = defineSchema();

            // Read streaming data from HDFS

            Dataset<Row> dataStream = spark
                .readStream()
                .format("parquet")
                .option("maxFilesPerTrigger", maxFilesPerTrigger)
                .option("datetimeRebaseMode", "CORRECTED")  // Add this
                .option("int96RebaseMode", "CORRECTED")
                // .option("latestFirst", "true")
                // .option("fileNameOnly", "false")
                // .schema(schema)
                .load(inputPath);

            logger.info("Created streaming source from: {}", inputPath);

            // Apply transformations and aggregations
            Dataset<Row> processedStream = processStream(dataStream, enableWatermark, watermarkDelayMinutes, fourG, twoG);

            // Convert to JSON format for Kafka
            Dataset<Row> kafkaOutput = processedStream
                .select(
                    lit(null).cast(DataTypes.StringType).as("key"),  // null key for round-robin partitioning
                    to_json(struct("*")).as("value")
                );

            // Write to Kafka
            StreamingQuery query = kafkaOutput
                .writeStream()
                .outputMode("update")  // Use "complete" for aggregations without watermark
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .option("topic", kafkaTopic)
                .option("checkpointLocation", checkpointPath)
                // Kafka producer configs for reliability
                .option("kafka.acks", "all")  // Wait for all replicas
                .option("kafka.retries", "3")
                .option("kafka.max.in.flight.requests.per.connection", "1")
                .option("kafka.enable.idempotence", "true")
                .option("kafka.compression.type", "snappy")
                .trigger(Trigger.ProcessingTime(triggerInterval, TimeUnit.SECONDS))
                .queryName("SalesAggregationQuery")
                .start();

            logger.info("Streaming query started successfully");
            logger.info("Query ID: {}", query.id());
            logger.info("Query Name: {}", query.name());
            logger.info("Writing to Kafka topic: {}", kafkaTopic);

            // Monitor the query
            monitorQuery(query);

        } catch (Exception e) {
            logger.error("Error in Sales Aggregation App", e);
            throw new StreamProcessingException("Failed to process sales stream", e);
        }
    }
    

    private Dataset<Row> processStream(Dataset<Row> dataStream, boolean enableWatermark,
                                       int watermarkDelayMinutes, Dataset<Row> fourG, Dataset<Row> twoG) {

        logger.info("Applying transformations and aggregations");

        // The Join will be on first 5 characters of ECI matching eNodeB_ID_Hex and its sixth and seventh characters matching Cell_ID_Hex

        Dataset<Row> dataStreamJoined4G = dataStream.join(
            fourG,
            dataStream.col("ECI").substr(1, 5).equalTo(fourG.col("eNodeB_ID_Hex"))
            .and(dataStream.col("ECI").substr(6, 2).equalTo(fourG.col("Cell_ID_Hex"))),
            "left_outer"
        );


        Dataset<Row> dataStreamJoined4GAnd2G = dataStreamJoined4G.join(
            twoG,
            dataStreamJoined4G.col("LAC").equalTo(twoG.col("LAC"))
            .and(dataStreamJoined4G.col("CI").equalTo(twoG.col("Cell_Code"))),
            "left_outer"
        );

        
        Dataset<Row> dataStreamWithCellName = dataStreamJoined4GAnd2G.withColumn(
            "Cell_Name",
            when(
                col("ECI").isNotNull(),
                col("Cell_Name_4G")
            ).otherwise(
                col("Cell_Name_2G")
            )
        );

        // Adding PAGE_DELAY_MSEL column, which is equal to sum of TCP_Connect_RTT_ms and First_DNS_Response_Delay_Within_Page_ms
        // and Page_Browsing_Delay divided by 1000000, if the value of First_Get_Success_or_Failure_Flag is equal to zero. otherwise it will be zero.

        Dataset<Row> dataStreamWithPageDelayMSEL = dataStreamWithCellName.withColumn(
            "PAGE_DELAY_MSEL",
            when(
                col("First_Get_Success_or_Failure_Flag").equalTo(lit(0.0))
                .and(col("Page_Size").geq(lit(0))),
                coalesce(col("TCP_Connect_RTT_ms"), lit(0))
                .plus(coalesce(col("First_DNS_Response_Delay_Within_Page_ms"), lit(0)))
                .plus(coalesce(col("Page_Browsing_Delay"), lit(0)).divide(lit(1000000)))
            ).otherwise(lit(0))
        );

        // Adding RECORD_DATE column, which comes from the Start_Time_s (in seconds) with the granularity of 5 minutes:

        
        Dataset<Row> dataStreamWithTimestamp = dataStreamWithPageDelayMSEL.withColumn(
            "Timestamp", 
            from_unixtime(col("Start_Time_s"))
        );

        // Create the RECORD_DATE column by taking the 'start' field from the window
        Dataset<Row> dataStreamWithRecordDate = dataStreamWithTimestamp.withColumn(
            "RECORD_DATE", 
            window(col("Timestamp"), "5 minutes").getField("start")
        );

        // Adding the watermark of 1 hour

        // TODO: commented this part since data is old, have to change the watermark i guess


        // if (enableWatermark) {
        //     dataStreamWithRecordDate = dataStreamWithRecordDate
        //         .withWatermark("Timestamp", watermarkDelayMinutes + " minutes");
        //     logger.info("Watermark applied: {} minutes", watermarkDelayMinutes);
        // }

        // Aggregation on RECORD_DATE, RAT, First_Get_Success_or_Failure_Flag, and Cell_Name
        // The output should be the number of “PAGE_SUCCEED_FLAG” being 0, Total “PAGE_DELAY_MSEL”, sum(“Page_Size”) when “Page_Size”/1024 > 10, and Total row count.


        Dataset<Row> finalDataStream = dataStreamWithRecordDate
            .groupBy(
                col("RECORD_DATE"),
                col("RAT"),
                col("First_Get_Success_or_Failure_Flag"),
                col("Cell_Name")
            )
            .agg(
                sum(when(col("PAGE_SUCCEED_FLAG").equalTo(lit(0)), lit(1)).otherwise(lit(0))).as("total_page_succeed_flag_zero"),
                sum(col("PAGE_DELAY_MSEL")).as("total_PAGE_DELAY_MSEL"),
                sum(when(col("Page_Size").divide(lit(1024)).gt(lit(10)), col("Page_Size")).otherwise(lit(0))).as("sum_of_page_size"),
                count("*").as("total_rows")
            );


        return finalDataStream;
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

                    if (query.lastProgress() != null) {
                        logger.info("Input Rows: {}", query.lastProgress().numInputRows());
                        logger.info("Processed Rows/sec: {}", query.lastProgress().processedRowsPerSecond());
                        logger.info("Batch ID: {}", query.lastProgress().batchId());
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
