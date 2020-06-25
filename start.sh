
if [ ! -f "$PWD/.env" ]; then
    # Create env file using from example.env as template
    cp example.env .env
fi

# This make the env vars available for use in scripts 
APP_ENVS=$(cat "$PWD/.env" | xargs)
export $APP_ENVS;

CRIME_FILE_PATH="data/police-department-calls-for-service.json"
RADIO_FILE_PATH="data/radio_code.json"

case "$1" in
    spark)
        spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_stream.py
    ;;
    producer)
        python kafka_server.py $CRIME_FILE_PATH
    ;;
    consumer)
        python kafka_consumer.py $RADIO_FILE_PATH
    ;;
    install)
        if [ ! -d "data" ]; then
            echo "Unpacking data ..."
            unzip data.zip
        fi

        echo "Installing dependencies ..."
        conda install -y --file requirements.txt
    ;;
    ""|*h*)
        echo "Usage: $0 {install|spark|producer|consumer|help}"
        exit 1
    ;;
esac
