
#  SF Crime Statistics with Apache Spark

Here we demonstrate using Apache Spark to create a simple streaming ETL pipeline. Using sample data obtained from San Francisco crime reports.
Our setup uses Apache Spark with Apache Kafka to Extract, Load and Transform (ETL) the dataset. 

### Steps

1. First we extract the data from a raw JSON file. This could have easily been retrieved from an API but we have chosen to keep it simple.
2. Then we push and persist data to the kafka server.
3. Finally we run a spark job which:
    * pulls the data from the Kafka stream
    * load the data 
    * run some computations/transformations on the dataset (RDDs)
    * join data with another static dataset (json) 
    * output results to the console 

## Requirements
* Docker

## Installation/Running locally
1. Run `docker-compose up` to bring up the containers. 
2. Install dependancies and unpack data: `./start.sh install`
3. Run kafka data source to simulate events: `./start.sh producer`
4. (optional) Run kafka data sink to consume events (for testing): `./start.sh producer`
5. Run spark task: `./start.sh spark`




