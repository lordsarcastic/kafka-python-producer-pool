import threading
import random
from dataclasses import dataclass
from typing import Optional

from kafka import KafkaProducer


# ideally, you want to grab this from the settings
KAFKA_BOOTSTRAP_SERVERS = ["128.122.1.1:8900"]

@dataclass
class KafkaProducerInstance:
    producer: KafkaProducer
    lock: threading.Lock


class KafkaProducerPool:
    # number of maximum instances
    INSTANCE_LIMIT: int = 10
    # holds all KafkaProducer instances
    _instances: dict[int, KafkaProducerInstance] = {}
    # lock on the `_instances` dict to make creation of
    # new instances thread-safe
    _creation_lock: threading.Lock = threading.Lock()

    def __new__(cls):
        if not cls._instances:
            cls._provision_instance()
        instance = cls._get_free_instance()
        if not instance:
            cls._provision_instance()
            random_index = random.randint(1, len(cls._instances))
            instance = cls._get_random_instance(random_index)
        return instance
    
    @classmethod
    def _provision_instance(cls):
        """
        Creates a new instance of the message broker and adds it to the pool.
        This method is thread-safe and is used to create new instances when
        all instances are busy and there is space to create instances.
        """
        with cls._creation_lock:
            if (instance_length := len(cls._instances)) >= cls.INSTANCE_LIMIT:
                # raising an exception is expensive in this context
                return

            producer: KafkaProducer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            )
            instance: KafkaProducerInstance = KafkaProducerInstance(
                producer=producer, lock=threading.Lock()
            )
            cls._instances[instance_length + 1] = instance
    
    @classmethod
    def _get_free_instance(cls) -> Optional[KafkaProducer]:
        """
        Retrieves a free instance of the message broker. If no free instance
        is found, `None` is returned.
        """
        if not cls._instances:
            return None

        for _, instance in cls._instances.items():
            if not instance.lock.locked():
                with instance.lock:
                    return instance.producer
        return None
    
    @classmethod
    def _get_random_instance(cls, index: int) -> KafkaProducer:
        """
        Retrieves a random instance of the message broker. This method is used
        when no free instance is found. The index is used to determine the
        instance to retrieve in case all instances are busy. If the index is
        out of range, the first instance is returned.
        """
        instance = cls._instances.get(index, cls._instances[1])
        with instance.lock:
            return instance.producer