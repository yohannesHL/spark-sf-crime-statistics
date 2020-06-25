from kafka import KafkaProducer
from json import load, dumps
import time
import io
import logging

logger = logging.getLogger('KafkaProducerServer')
logger.setLevel(logging.DEBUG)

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
    