import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


def create_keyspace(session):
    # create keyspace for cassandra
    session.execute(
        """
            CREATE KEYSPACE IF NOT EXISTS property_stream
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """
    )
    print("Keyspace successfully created...")


def create_table(session):
    session.execute(
        """ 
            CREATE TABLE IF NOT EXISTS property_stream.properties (
            price text,
            category text,
            subcategory text,
            bedrooms text,
            bathrooms text,
            land_size text,
            building_size text,
            furnished text,
            geo_point text,
            floors text ,
            description text,
            parent_url text,
            page_url text,
            PRIMARY KEY (page_url)
        );
        """
    )
    print("Table successfuly created...")


def insert_data(session, **kwargs):
    print("Inserting data...")
    print("Data before insert: ", kwargs.values())

    session.execute(
        """ 
            INSERT INTO property_stream.properties (
            price,
            category,
            subcategory,
            bedrooms,
            bathrooms,
            land_size,
            building_size,
            furnished,
            geo_point,
            floors,
            description,
            parent_url,
            page_url
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        kwargs.values(),
    )


def create_cassandra_session():
    session = Cluster(["cassandra"]).connect()

    print(f"Session: {session}")

    if session is not None:
        create_keyspace(session)
        create_table(session)

    return session


# connect to kafka -> fetch information -> write to cassandra
def main():
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("RealEstateConsumer")
        .config("spark.cassandra.connection.host", "cassandra")
        .config(
            "spark.jar.packages",
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
        )
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka-broker:29092")
        .option("subscribe", "properties")
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType(
        [
            StructField("price", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("bedrooms", StringType(), True),
            StructField("bathrooms", StringType(), True),
            StructField("land_size", StringType(), True),
            StructField("building_size", StringType(), True),
            StructField("furnished", StringType(), True),
            StructField("geo_point", StringType(), True),
            StructField("floors", StringType(), True),
            StructField("description", StringType(), True),
            StructField("parent_url", StringType(), True),
            StructField("page_url", StringType(), True),
        ]
    )

    kafka_df = (
        kafka_df.selectExpr("CAST(value as STRING) as value")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    cassandra_query = (
        kafka_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: batch_df.foreach(
                lambda row: insert_data(create_cassandra_session(), **row.asDict())
            )
        )
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
