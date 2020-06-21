import producer_server
import os, json

KAFKA_BROKER_URL= os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "org.sanfrancisco.crime.police-calls")

get_file_path = lambda name: os.path.join( os.getcwd(), name)

def run_kafka_server():
    input_file = get_file_path("police-department-calls-for-service.json")

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        client_id="producer-app-1"
    )

    return producer

def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
