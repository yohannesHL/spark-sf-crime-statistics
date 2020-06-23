import producer_server
import os, json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KAFKA_BROKER_URL= os.getenv('KAFKA_BROKER_URL')
KAFKA_TOPIC_NAME = os.getenv('KAFKA_TOPIC_NAME')
DATA_SOURCE_FILE = os.getenv('DATA_SOURCE_FILE')

get_file_path = lambda filename: os.path.join( os.getcwd(), filename)

def run_kafka_stream_source():
    input_file = get_file_path(DATA_SOURCE_FILE)

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id='producer-app-1'
    )

    producer.generate_data()


if __name__ == '__main__':
    logging.basicConfig()
    logger.debug('Starting Kafka Producer...')

    run_kafka_stream_source()
