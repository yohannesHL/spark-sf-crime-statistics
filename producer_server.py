from kafka import KafkaProducer
from json import load, dumps
import time
import io
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def dict_to_binary(self, json_dict):
        return dumps(json_dict).encode('utf-8')
    
    def generate_data(self):
        logger.debug('Generating data for topic: %s', self.topic)

        with open(self.input_file) as file:
            for record in load(file):
                message = self.dict_to_binary(record)

                logger.debug('sending message: %s', message)

                self.send(self.topic, message)
                time.sleep(0.01)
                

            
