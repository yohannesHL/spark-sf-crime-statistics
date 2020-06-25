import sys
import os
from pathlib import Path
import json
import logging
from producer_server import KafkaProducerServer

KAFKA_BROKER_URL= os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')

logger = logging.getLogger(__file__)

def run_kafka_stream_source(filename):
    input_file = os.path.join( Path(__file__).parent, filename)
    
    producer = KafkaProducerServer(
        input_file,
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
    
    filename = sys.argv[1]

    logger.debug(f'Using json source file: {filename}')
    logger.debug('Starting KafkaProducerServer...')

    run_kafka_stream_source(filename)
