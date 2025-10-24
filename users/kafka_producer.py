from confluent_kafka import Producer
import json
import time

from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from django.conf import settings
import logging

logger = logging.getLogger(__name__)



producer = Producer(settings.KAFKA_PRODUCER_CONFIG)

def create_kafka_topic(topic_name, num_partitions=3, replication_factor=1):
    conf = {'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS}
    admin_client = AdminClient(conf)

    # Check if topic exists
    metadata = admin_client.list_topics(timeout=10)
    if topic_name in metadata.topics:
        print(f"Topic '{topic_name}' already exists")
        return True

    # Create new topic
    new_topic = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    fs = admin_client.create_topics([new_topic])

    for topic, f in fs.items():
        try:
            f.result()  # Wait for operation to complete
            print(f"Topic '{topic}' created successfully")
            return True
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")
            return False


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def send_user_update_event(user, retry_count=3):
    topic = 'user_updates'
    if not create_kafka_topic(topic):
        logger.error("Failed to create topic 'user_updates'")
        return
    data = {
        'event_type': 'user_updated',
        'user_id': user.id,
        'name': user.name,
        'email': user.email,
        'address': user.address,
        'phone': user.phone,
        'timestamp': str(time.time())
    }
    for attempt in range(retry_count):
        try:
            producer.produce(
                topic,
                json.dumps(data),
                callback=delivery_report
            )
            producer.flush()
            break
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == retry_count - 1:
                # Last attempt failed, store in DB for later processing
                store_failed_event(data)
            time.sleep(2 ** attempt)  # Exponential backoff


def store_failed_event(event_data):
    from .models import FailedEvent  # We'll create this model
    try:
        FailedEvent.objects.create(
            topic='user_updates',
            event_data=event_data,
            error_message='Failed after retries'
        )
        logger.info("Stored failed event in database for later processing")
    except Exception as e:
        logger.error(f"Failed to store event in DB: {str(e)}")