from kafka import KafkaProducer
import json, time, io

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def dict_to_binary(self, json_dict):
        buffer = io.BytesIO(json_dict)
        
        return buffer.getvalue()

    def generate_data(self):

        with open(self.input_file, 'rb') as file:
            data = json.load(file)

            for json_dict in data:
                message = self.dict_to_binary(json_dict)

                self.send(self.topic, message)
                
                time.sleep(1)
            
