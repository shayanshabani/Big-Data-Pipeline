## **Package: Implementing a Data Pipeline**

##### **Time span:** 
6.5 days
### **Deliverable: A Sample Data Pipeline** 
#### Project Overview
This project involves building a data pipeline leveraging modern data engineering tools and best practices. The pipeline will be triggered by the presence of new data files within a specific directory in HDFS, which serves as the data source.

Using Spark Structured Streaming, incoming data will be read, enriched, and aggregated before being published to a Kafka cluster. From Kafka, the processed data will be ingested into ClickHouse, where it can be utilized for analytical purposes.

To ensure observability, all services and the Spark application will be monitored using Prometheus and Grafana.

Throughout the project, data and software engineering best practices—such as clean code principles and the use of design patterns—must be followed rigorously.

**The entire pipeline must be orchestrated and containerized using Docker Compose. Upon startup and successful execution, the data should be read by Spark, published to Kafka and consumed by Clickhouse; and the metrics for the all services become accessible on Grafana.**

#### Project Steps

##### Set up Hadoop and Kafka
Please use the clusters you have configured for the previous packages.

##### Write a Java Project for the Spark Structured Streaming Application
###### a. Read from Source 

Read the data from parquet files in HDFS. The data files are provided in the ‘input.zip’ file from the previous package. Please replicate the data files at least 5 times so that there are more files to read. Set maxFilesPerTrigger config to 1.

###### b. Apply the following instructions to the data 

The incoming data must be enriched and a “Cell_Name” column must be added using two reference files “SQM_2GCELL” and “SQM_4GCELL”: when the record has “ECI”, the “Cell_Name” column is derived from the 4G ref file if the first 5 characters of “ECI” is equal to “eNodeB_ID_Hex” and its last two characters are equal to “Cell_ID_Hex”; when there is no value for “ECI”, but the record has “LAC” and “CI”, the 2G file must be used and “Cell_Name” is derived from the row in the reference file having the same value of “LAC” as our data’s “LAC” and “Cell_code” as our data’s “CI”.
From the “Start_Time_s” column create a “RECORD_DATE” column with the granularity of 5 minutes.
Add column "PAGE_DELAY_MSEL" which only has values other than 0 when "First_GET_Sucess_or_Failure_Flag" is 0 and "Page size" is not negative. The value should be the sum of "TCP_Connect_RTT_ms", "First_DNS_Response_Delay_Within_Page_ms" and ("Page_Browsing_Delay"/1000000).
The app should consider late data that came at most 1 hour late. Aggregate the records by “RECORD_DATE”, “RAT”, “First_GET_Success_or_Failure_Flag”, and “Cell_Name”, and output the following metrics: number of “PAGE_SUCCEED_FLAG” being 0, Total “PAGE_DELAY_MSEL”, sum(“Page_Size”) when “Page_Size”/1024 > 10, and Total row count.
###### c. Write to the Kafka cluster you deployed

###### Key notes: 
a. Suppose you want to add more Structured Streaming applications to the project later, and you run each app by specifying the app class name as a parameter when submitting the job. Each new app must be able to register itself as a new app, without change in other classes (This should not be handled by switch-case/if-else statements on the app names and classes).

b. All of the configs of an app must be read from a config file existing inside the project for that particular app.

c. Handle Exceptions as needed.

##### Set Up ClickHouse
Deploy a clickhouse server and create the appropriate table to store the summary. Clickhouse should read the data from kafka and insert it into the table.

##### Monitoring
Collect and scrape metrics from all containers and streaming apps using Prometheus. Create a Grafana dashboard to visualize the containers’ exposed metrics.