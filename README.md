This project is data pipeline to peform sentiment analysis on a Yelp reviews using TCP/IP Socket, Apache Spark, OpenAI LLM, Kafka and Elasticsearch. It covers each stage from data acquisition, processing, sentiment analysis with ChatGPT, production to kafka topic and connection to elasticsearch.

The project is designed with the following components:

- **Data Source**: We use `yelp.com` for the dataset.
- **TCP/IP Socket**: Used to stream data over the network in chunks
- **Apache Spark**: For data processing with its master and worker nodes.
- **Confluent Kafka**: Create a cluster in the cloud
- **Control Center and Schema Registry**: Helps in monitoring and schema management of the Kafka streams.
- **Kafka Connect**: For connecting to elasticsearch
- **Elasticsearch**: For indexing and querying

## Skills Involved

- Setting up data pipeline with TCP/IP 
- Real-time data streaming with Apache Kafka
- Data processing techniques with Apache Spark
- Realtime sentiment analysis with OpenAI ChatGPT
- Synchronising data from kafka to elasticsearch
- Indexing and Querying data on elasticsearch

## Technologies Used

- Python
- TCP/IP
- Confluent Kafka
- Apache Spark
- Docker
- Elasticsearch

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/smeltzBrand/yelp_stream.git
    ```

2. Navigate to the project directory:
    ```bash
    cd yelp_stream
    ```

3. Run Docker Compose to spin up the spark cluster:
    ```bash
    docker-compose up
    ```
