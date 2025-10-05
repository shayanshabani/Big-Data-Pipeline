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


            Dataset<Row> fourG = spark.read()
                .option("header", "true")
                .csv("SQM_4GCell.csv")
                .toDF("eNodeB_ID_Hex", "Cell_ID_Hex" ,"Cell_Name");

            fourG = fourG.withColumnRenamed("Cell_Name", "Cell_Name_4G");


            Dataset<Row> twoG = spark.read()
                .option("header", "true")
                .csv("SQM_2GCell.csv")
                .toDF("LAC", "Cell_Code" ,"Cell_Name");

            twoG = twoG.withColumnRenamed("Cell_Name", "Cell_Name_2G");

            // Define schema for sales data
            // StructType schema = defineSchema();

            // Read streaming data from HDFS
            Dataset<Row> dataStream = spark
                .readStream()
                .format("parquet")
                .option("maxFilesPerTrigger", maxFilesPerTrigger)
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

    /**
     * Defines the schema for sales data.
    //  */
    // private StructType defineSchema() {
    //     return new StructType(new StructField[]{
    //         DataTypes.createStructField("transaction_id", DataTypes.StringType, false),
    //         DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
    //         DataTypes.createStructField("product_id", DataTypes.StringType, false),
    //         DataTypes.createStructField("product_id", DataTypes.StringType, false),
    //         DataTypes.createStructField("product_name", DataTypes.StringType, false),
    //         DataTypes.createStructField("category", DataTypes.StringType, false),
    //         DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
    //         DataTypes.createStructField("price", DataTypes.DoubleType, false),
    //         DataTypes.createStructField("customer_id", DataTypes.StringType, false),
    //         DataTypes.createStructField("product_name", DataTypes.StringType, false),
    //         DataTypes.createStructField("category", DataTypes.StringType, false),
    //         DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
    //         DataTypes.createStructField("price", DataTypes.DoubleType, false),
    //         DataTypes.createStructField("customer_id", DataTypes.StringType, false),
    //         DataTypes.createStructField("customer_country", DataTypes.StringType, false),
    //         DataTypes.createStructField("payment_method", DataTypes.StringType, false)
    //     });
    // }

    /**
     * Processes the sales stream with enrichment and aggregations.// --- 1. Load Static Lookup DataFrames ---

// Load the first CSV for ECI-based lookup
Dataset<Row> firstCsvDf = spark.read()
    .option("header", "false") // Assuming no header in the CSV
    .csv("path/to/your/first.csv")
    .toDF("ECI_Prefix", "ECI_Suffix", "Cell_Name_From_ECI");

     */
    private Dataset<Row> processStream(Dataset<Row> dataStream, boolean enableWatermark,
                                       int watermarkDelayMinutes, Dataset<Row> fourG, Dataset<Row> twoG) {

        logger.info("Applying transformations and aggregations");


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

        // TODO: gotta check the validity of this implementation

        Dataset<Row> dataStreamWithPageDelayMSEL = dataStreamWithCellName.withColumn(
            "PAGE_DELAY_MSEL",
            when(
                col("First_Get_Success_or_Failure_Flag").equalTo(lit(0.0))
                .and(col("Page_Size").geq(lit(0))),
                coalesce(col("TCP_Connect_RTT_ms"), lit(0))
                .plus(coalesce(col("First_DNS_Response_Delay_Within_Page_ms"), lit(0)))
                .plus(coalesce(col("Page_Browsing_Delay").divide(lit(1000000)), lit(0)))
            ).otherwise(lit(0))
        );

        // TODO: gotta add RECORD_DATE column here:



        // Add enrichment: calculate total amount
        // Dataset<Row> enrichedStream = salesStream
        //     .withColumn("total_amount",
        //         col("quantity").multiply(col("price")))
        //     .withColumn("processing_time", current_timestamp());

        // Apply watermark if enabled (for handling late data)

        // TODO: gotta check whether water mark is applied correctly


        if (enableWatermark) {
            dataStreamWithPageDelayMSEL = dataStreamWithPageDelayMSEL
                .withWatermark("timestamp", watermarkDelayMinutes + " minutes");
            logger.info("Watermark applied: {} minutes", watermarkDelayMinutes);
        }

        // TODO: gotta check this part:


        Dataset<Row> finalDataStream = dataStreamWithPageDelayMSEL
            .groupBy(
                col("RECORD_DATE"),
                col("RAT"),
                col("First_Get_Success_or_Failure_Flag"),
                col("Cell_Name")
            )
            .agg(
                count(col("PAGE_SUCCEED_FLAG").equalTo(lit(0))).as("total_page_succeed_flag_zero"),
                count(col("PAGE_DELAY_MSEL").notEqual(lit(0))).as("total_PAGE_DELAY_MSEL"),
                sum(when(col("Page_Size").divide(lit(1000000)).gt(lit(10)), col("Page_Size")).otherwise(lit(0))).as("sum_of_page_size"),
                count("*").as("total_rows")
            )
            .select(
                col("total_page_succeed_flag_zero"),
                col("total_PAGE_DELAY_MSEL"),
                col("sum_of_page_size"),
                col("total_rows")
            );

        // Perform aggregations - Sales by category and country (windowed aggregation)
        // Dataset<Row> categoryCountryAgg = enrichedStream
        //     .groupBy(
        //         window(col("timestamp"), "5 minutes", "1 minute"),
        //         col("category"),
        //         col("customer_country")
        //     )
        //     .agg(
        //         count("*").as("transaction_count"),
        //         sum("quantity").as("total_quantity"),
        //         sum("total_amount").as("total_revenue"),
        //         avg("total_amount").as("avg_transaction_value"),
        //         min("total_amount").as("min_transaction"),
        //         max("total_amount").as("max_transaction")
        //     )
        //     .select(
        //         col("window.start").as("window_start"),
        //         col("window.end").as("window_end"),
        //         col("category"),
        //         col("customer_country"),
        //         col("transaction_count"),
        //         col("total_quantity"),
        //         round(col("total_revenue"), 2).as("total_revenue"),
        //         round(col("avg_transaction_value"), 2).as("avg_transaction_value"),
        //         round(col("min_transaction"), 2).as("min_transaction"),
        //         round(col("max_transaction"), 2).as("max_transaction"),
        //         current_timestamp().as("processed_at")
        //     );

        // return categoryCountryAgg;


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
