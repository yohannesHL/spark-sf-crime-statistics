
#  SF Crime Statistics with Apache Spark

Here we demonstrate using Apache Spark to create a simple streaming ETL pipeline. Using sample data obtained from San Francisco crime reports.
Our setup uses Apache Spark and Apache to Kafka Extract, Load and Transform (ETL) the dataset. 
Which is then explored (using Apache Spark and Pandas) to identify interesting patterns and build some statistics.

### Steps

1. First we extract the data from a raw JSON file. This could have easily been retrieved from an API but we have chosen to keep it simple.
2. Then we push and persist data to the kafka server.
3. Finally we run a spark job that does some computations.
    In the spark job the following steps take place:
    * We pull the data using Kafka as Streaming Sink for Apache Spark. 
    * We load the data and run some computations on the dataset (RDDs) to generate statistics.


