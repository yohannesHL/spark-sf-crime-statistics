import sys
import os
import time
import logging
from pathlib import Path
from json import load, dumps
from kafka import KafkaProducer

logger = logging.getLogger('KafkaProducerServer')
logger.setLevel(logging.DEBUG)

KAFKA_BROKER_URL= os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

class KafkaProducerServer():
    '''Reads data from an input json file and push to kafka topic as a stream
    Used as an event source for downstream applications
    
    Arguments:
        input_file {string} -- file path (absolute) to json source file.
        topic {string} -- Topic name to send kafka event to
        **kwargs -- Additional keyword arguments passed to KafkaProducer
    '''  
   
    def __init__(self, input_file, topic, **kwargs):   
   
        self.input_file = input_file
        self.topic = topic
        self.producer = KafkaProducer(
            key_serializer = str.encode,
            value_serializer = self.serialize_json,
            **kwargs)

    def serialize_json(self, json_dict):
        return dumps(json_dict).encode('utf-8')
    
    def generate_data(self):
        logger.debug(f'Generating data for topic: {self.topic}')

        with open(self.input_file) as json_file:
            for record in load(json_file):
                logger.debug(f'sent: {record}')

                self.producer.send(
                    self.topic, 
                    key=record.get('crime_id'), 
                    value=record)

                time.sleep(0.05)
    
def run_kafka_stream_source(filepath):
    
    producer = KafkaProducerServer(
        filepath,
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id='producer-app-1'
    )

    producer.generate_data()


if __name__ == '__main__':
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    if len(sys.argv) < 2:
        raise AssertionError(f'Please provide a path to the json file. \n\t\tUsage: {sys.argv[0]} <filename>')
    
    filepath = os.path.join( Path(__file__).parent, sys.argv[1])
    
    logger.debug(f'Using json source file: {filepath}')
    logger.debug('Starting KafkaProducerServer...')

    run_kafka_stream_source(filepath)