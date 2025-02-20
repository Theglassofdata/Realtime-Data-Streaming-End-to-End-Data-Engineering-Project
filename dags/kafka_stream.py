import uuid
import json
import time
import logging
import requests
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00),
    'retries': 2,
    'retry_delay':timedelta(minutes=1)
}

def get_data():
    """Fetch random user data from API"""
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    """Format the user data into desired structure"""
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def test_kafka_connection():
    """Verify Kafka connection is working"""
    try:
        admin = KafkaAdminClient(bootstrap_servers=['broker:29092'])
        topics = admin.list_topics()
        logging.info(f"Found Kafka topics: {topics}")
        return True
    except Exception as e:
        logging.error(f"Kafka connection failed: {e}")
        return False

def create_topic():
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=['broker:29092'])
        existing_topics = admin_client.list_topics()
        if 'users_created' not in existing_topics:
            topic_list = [NewTopic(name="users_created", num_partitions=1, replication_factor=1)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logging.info("Topic 'users_created' created successfully")
        else:
            logging.info("Topic 'users_created' already exists")
    except Exception as e:
        logging.error(f"Topic creation failed: {e}")
        raise

def stream_data():
    """Stream formatted user data to Kafka"""
    # First check Kafka connection
    if not test_kafka_connection():
        raise Exception("Cannot connect to Kafka broker")
    
    # Ensure topic exists
    create_topic()
    
    # Create producer with detailed error handling
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000,
        acks='all',  # Wait for all replicas to acknowledge
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    curr_time = time.time()
    message_count = 0

    while True:
        if time.time() > curr_time + 60:  # 1 minute
            break
        try:
            res = get_data()
            formatted_data = format_data(res)
            
            # Send with callback for verification
            future = producer.send('users_created', formatted_data)
            record_metadata = future.get(timeout=10)
            message_count += 1
            
            logging.info(f"Message {message_count} sent to {record_metadata.topic} "
                         f"partition {record_metadata.partition} "
                         f"offset {record_metadata.offset}")
            
            # Small delay to avoid flooding
            time.sleep(0.5)
            
        except Exception as e:
            logging.error(f'An error occurred while streaming data: {e}')
            continue
    
    # Ensure all messages are sent before closing
    producer.flush()
    logging.info(f"Completed streaming {message_count} messages to Kafka")

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False,
         description='Stream random user data to Kafka',
         tags=['kafka', 'streaming']) as dag:

    connection_test_task = PythonOperator(
        task_id='test_kafka_connection',
        python_callable=test_kafka_connection
    )
    
    topic_creation_task = PythonOperator(
        task_id='create_kafka_topic',
        python_callable=create_topic
    )
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    
    # Define task dependencies
    connection_test_task >> topic_creation_task >> streaming_task