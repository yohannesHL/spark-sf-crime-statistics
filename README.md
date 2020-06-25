
#  SF Crime Statistics with Apache Spark

Here we demonstrate using Apache Spark to create a simple streaming ETL pipeline. Using sample data obtained from San Francisco crime reports.
Our setup uses Apache Spark with Apache Kafka to Extract, Load and Transform (ETL) the dataset and do some processing. 

### Process
1. First we extract the data from a raw JSON file. This could have easily been retrieved from an API but we have chosen to keep it simple.
2. Then we push and persist data to the kafka server.
3. Finally we run a spark job which:
    * pulls the data from the Kafka stream
    * load the data 
    * run some computations/transformations on the dataset (RDDs)
    * join data with another static dataset (json) 
    * output results to the console 

## Screenshots
Spark UI|Spark Executor Log| KafkaProducer Log
-|-|-
![SparkUI](images/SparkUI.png)|![SparkUI](images/SparkExecutorLogs.png)|![SparkUI](images/KafkaProducerLogs.png)



## Requirements
* Docker

## Installation/Running locally
1. Run `docker-compose up` to bring up the containers. 
2. Install dependancies and unpack data: `./start.sh install`
3. Run kafka data source to simulate events: `./start.sh producer`
4. (optional) Run kafka data sink to consume events (for testing): `./start.sh producer`
5. Run spark task: `./start.sh spark`




By adjusting spark session property parameters its possible to optimise the latency and through put of the spark jobs.
I found that adjusting the trigger window to above 30 seconds achives the highest throughput for this aggregation


How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
increasing available cpu and memory 
latency isn't affect
What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Highest throuput anc be achived by simulatanuously increasing executor.memory allocation and setting trigger to once=True to wait 
This will give a snapshot of all the data up upto now.
This may not be feasibly if the data is continuously eveolving.

For long running spark jobs with endless stream of data it makes sence to have a value Instead it