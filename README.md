# kafka-python-producer-pool
An implementation of a Producer pool for the kafka-python library as articulated in my two-part article on KafkaProducer connection pool in Python:
- [Part 1](https://blog.lordsarcastic.dev/kafkaproducer-connection-pool-in-python-part-1)
- [Part 2](https://blog.lordsarcastic.dev/kafkaproducer-connection-pool-in-python-part-2)

## Structure
The project contains two folders:
- `generic_producer_pool`: contains the implementation of the generic ProducerPool class. All the other classes in other files in the same folder are implementations of this class.

- `kafka_producer_pool`: contains the implementation of the KafkaProducerPool class, which is a specialization of the ProducerPool class. It uses the kafka-python library to create a pool of Kafka producers.


## Tests
As highlighted in the article, there is only a 50% guarantee the tests will pass. This is because of the unreliability of testing multi-threaded code. However, the tests are there to give you an idea of how to use the ProducerPool class. I'll update this docs when I find a 100% guarantee.

## Usage
- Clone the repository with `git clone git@github.com:lordsarcastic/kafka-python-producer-pool.git`
- Create a virtual environment with `python3 -m venv venv`
- Activate the virtual environment with `source venv/bin/activate`
- Install the dependencies with `pip install -r requirements.txt`
- Run the tests with `pytest .`
