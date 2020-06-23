import os
import time
import json
import logging
import kafka

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME', 'org.sanfrancisco.crime.police-calls')

class Consumer():
    '''
        Kafka Consumer 
        
    '''
    def __init__(self, topic_name, message_handler, **kwargs):
        self.topic_name = topic_name
        self.handle_message = message_handler
        self.consumer = kafka.KafkaConsumer(
            topic_name,
            bootstrap_servers = KAFKA_BROKER_URL,
            client_id = 'spark_kafka_sink',
            group_id = 'cg_spark_kafka_sink',
            auto_offset_reset = 'earliest',
            value_deserializer = self.deserialize_json,
            **kwargs
        )    
        logger.info('Kafka consumer subscribed to topic: %s', topic_name)

    def deserialize_json(self, value):
        return json.loads(value)
    
    def consume(self):
        
        for message in self.consumer:
            try:
                self.handle_message(message.value)
            except Exception as e:
                logging.error('Error unable to handle message : %', e)


if __name__ == '__main__':
    logging.basicConfig()
    logger.debug('Starting Kafka Consumer')
    
    c = Consumer(KAFKA_TOPIC_NAME, lambda x: print(x))
    c.consume()
