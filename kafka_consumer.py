import os
import time
import json
import logging
import kafka

logger = logging.getLogger('KafkaConsumer')

KAFKA_BROKER_URL = os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

class KafkaConsumer():
    '''Consumes events from a kafka topic and processes the data via a custom message_handler
    
    Arguments:
        topic {string} -- Topic name to send kafka event to
        message_handler {Callable} -- A callback function to that will process incomming messages. 
            Expects positional arguments: `key` and `value`.
        **kwargs -- Additional keyword arguments passed to kafka.KafkaConsumer
    '''  
    def __init__(self, topic, message_handler, **kwargs):
        self.topic_name = topic
        self.handle_message = message_handler
        self.consumer = kafka.KafkaConsumer(
            topic,
            bootstrap_servers = KAFKA_BROKER_URL,
            client_id = 'spark_kafka_sink',
            group_id = 'cg_spark_kafka_sink',
            auto_offset_reset = 'earliest',
            value_deserializer = self.deserialize_json,
            **kwargs
        )    
        logger.info(f'Kafka consumer subscribed to topic: {topic}')

    def deserialize_json(self, value):
        return json.loads(value)
    
    def consume(self):
        
        for message in self.consumer:
            try:
                self.handle_message(message.key, message.value)
            except Exception as error:
                logging.error(error)


if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    logger.debug('Starting KafkaConsumer')
    
    c = KafkaConsumer(KAFKA_TOPIC_NAME, lambda k,v: print(k, v))
    c.consume()
