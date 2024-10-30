# depokhouse-etl

## Overview

End-to-end data engineering project streaming pipeline that demonstrates real-time data processing pipeline that can scrape data, process it with an LLM, and stream it through Kafka and Spark, ultimately storing it in Cassandra for future use.

## Diagram

![streaming_pipeline](https://github.com/anggapark/depokhouse-etl/blob/main/asset/diagram.png?raw=true)

## Tools and Technologies

| Tools/Technologies | Function                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| Selenium           | Scraping tool for collecting property data from website                                          |
| Gemini LLM         | Extract information from house's description                                                     |
| Python             | Handle the web scraping and data processing logic                                                |
| Apache Kafka       | Streaming platform that ingests the scraped data                                                 |
| Apache Zookeeper   | Manage configuration and synchronization of Kafka nodes                                          |
| Apache Spark       | Processes the streamed data from Kafka                                                           |
| Control Center     | Provides a dashboard to monitor, manage, and configure Kafka streaming                           |
| Apache Cassandra   | Serves as the final storage layer, where the processed data is stored for retrieval and querying |

## Scripts Overview

- **`scraping.py`**: Main script to scrape data from website and stream it to Kafka
- **`jobs/spark-consumer.py`**: Consume data from kafka, processed it using PySpark, and write the data to Cassandra
- **`jobs/requirements.txt`**: List of Python dependencies used in this project
- **`docker-compose.yml`**: Defines and runs multi-container Docker applications, configuring services for Kafka, Cassandra, zookeeper, control-center, and spark.

## How to Run

1. Clone the Repository

   ```bash
   git clone depokhouse-etl

   # access project root directory
   cd depokhouse-etl
   ```

2. Setup Environment (recommended)

   ```bash
   # create conda env
   conda create -n <env_name> python=3.11

   # activate the env
   conda activate <env_name>

   # install dependencies
   pip install -r jobs/requirements.txt
   ```

3. Run docker container

   ```bash
   docker compose-up -d
   ```

4. Install dependencies inside spark-master and spark-worker services

   ```bash
   # Inside spark-master
   docker exec -it depok-houseprice-etl-spark-master-1 pip install -r jobs/requirements.txt

   # Inside spark-worker
   docker exec -it depok-houseprice-etl-spark-worker-1 pip install -r jobs/requirements.txt
   ```

5. run the scraping program
   ```bash
   python scraping.py
   ```
6. Run a Spark job inside a Docker container
   ```bash
   docker exec -it depok-houseprice-etl-spark-master-1 spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 jobs/spark-consumer.py
   ```

## Reference

[Real Estate End to End Data Engineering using AI](https://youtu.be/Qx6BAVqnMrs?si=UIDZKQRC1HAK9eom)
