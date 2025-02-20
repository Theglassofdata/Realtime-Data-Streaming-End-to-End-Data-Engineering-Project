import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")

def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master('local[*]') \
            .config('spark.driver.host', '127.0.0.1') \
            .config('spark.driver.bindAddress', '127.0.0.1') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config('spark.cassandra.auth.username', 'cassandra') \
            .config('spark.cassandra.auth.password', 'cassandra') \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to: {e}")
        return None

def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("Cassandra connection established successfully!")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None

def connect_to_kafka(spark):
    try:
        spark_df = spark.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Connected to Kafka topic 'users_created' successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created due to: {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    return spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                logging.info("Starting streaming to Cassandra...")
                query = selection_df.writeStream \
                    .format("org.apache.spark.sql.cassandra") \
                    .option('checkpointLocation', '/tmp/checkpoint') \
                    .option('keyspace', 'spark_streams') \
                    .option('table', 'created_users') \
                    .outputMode("append") \
                    .start()

                query.awaitTermination()

                session.shutdown()
                spark_conn.stop()
            else:
                logging.error("No Cassandra session available. Exiting.")
                spark_conn.stop()
        else:
            logging.error("No Kafka DataFrame available. Exiting.")
            spark_conn.stop()
    else:
        logging.error("No Spark session available. Exiting.")