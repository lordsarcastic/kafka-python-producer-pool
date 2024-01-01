import threading
import random
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar


# ideally, you want to grab this from the settings
KAFKA_BOOTSTRAP_SERVERS = ["128.122.1.1:8900"]

T = TypeVar("T")
U = TypeVar("U")

@dataclass
class SingletonInstance(Generic[U]):
    producer: U
    lock: threading.Lock


class ProducerPool(Generic[T]):
    """
    The ProducerPool is a singleton class that is used to manage a collection
    of message brokers. It is thread-safe and manages a number of active
    connections to the message broker. It is also responsible for creating
    new connections to the message broker in case more is needed.

    Params:
    T: This is the actual message broker instance used to produce messages
    producer_class: This is the class of the message broker instance
    INSTANCE_LIMIT: This is the maximum number of broker instances that can
    be created.

    This class is not to be used directly. Instead, it should be inherited and
    the `producer_class` and `create_instance` class methods should be
    overridden.

    Example:
    ```
    class KafkaProducerPool(ProducerPool[KafkaProducer]):
        producer_class = KafkaProducer

        @classmethod
        def create_instance(cls):
            return cls.producer_class(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            )
    ```
    """

    # number of maximum instances
    INSTANCE_LIMIT: int = 10

    # holds the class of the message broker instance
    producer_class: T = None

    # holds all T instances
    _instances: dict[int, T] = {}

    # holds the singleton instance
    _singleton_instance: SingletonInstance[T] = SingletonInstance

    # lock on the `_instances` dict to make creation of
    # new instances thread-safe
    _creation_lock: threading.Lock = threading.Lock()

    def __new__(cls) -> T:
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

            producer = cls.create_instance()
            instance = cls._singleton_instance(
                producer=producer, lock=threading.Lock()
            )
            cls._instances[instance_length + 1] = instance
    
    @classmethod
    def _get_free_instance(cls) -> Optional[T]:
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
    def _get_random_instance(cls, index: int) -> T:
        """
        Retrieves a random instance of the message broker. This method is used
        when no free instance is found. The index is used to determine the
        instance to retrieve in case all instances are busy. If the index is
        out of range, the first instance is returned.
        """
        instance: cls._singleton_instance = cls._instances.get(index, cls._instances[1])
        with instance.lock:
            return instance.producer

    @classmethod
    def create_instance(cls) -> T:
        """
        Creates a new instance of the message broker. This method should be
        overridden and is not thread safe. It is used in a thread-safe
        context.
        """
        raise NotImplementedError
