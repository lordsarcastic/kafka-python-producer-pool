from generic_producer_pool.base import ProducerPool
from kafka import KafkaProducer


# ideally, you want to grab this from the settings
KAFKA_BOOTSTRAP_SERVERS = ["128.122.1.1:8900"]

class KafkaProducerPool(ProducerPool[KafkaProducer]):
    producer_class = KafkaProducer

    @classmethod
    def create_instance(cls) -> KafkaProducer:
        return cls.producer_class(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )