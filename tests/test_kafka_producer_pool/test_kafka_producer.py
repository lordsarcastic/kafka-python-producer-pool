import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from kafka_producer_pool.kafka_producer import KafkaProducerPool



class MockKafkaProducer:
    def __init__(self, *args, **kwargs):
        pass


def work(sleep: int = 4, thread: int = 1):
    result = KafkaProducerPool()
    # mocking expensive operation
    time.sleep(sleep)
    return result

MAX_INSTANCE_LIMIT = 2

@pytest.mark.xfail
@patch("kafka_producer_pool.kafka_producer.KafkaProducer", MockKafkaProducer)
@patch("kafka_producer_pool.kafka_producer.KafkaProducerPool.INSTANCE_LIMIT", MAX_INSTANCE_LIMIT)
def test_kafka_producer_pool_will_return_same_instance():
    with ThreadPoolExecutor(max_workers=7) as executor:
        producer1 = executor.submit(work, thread=1)
        producer2 = executor.submit(work, thread=2)
        producer3 = executor.submit(work, thread=3)
        producer4 = executor.submit(work, thread=4)
        producer5 = executor.submit(work, thread=5)
        producer6 = executor.submit(work, thread=6)
        producer7 = executor.submit(work, thread=7)

        producer1 = producer1.result()
        producer2 = producer2.result()
        producer3 = producer3.result()
        producer4 = producer4.result()
        producer5 = producer5.result()
        producer6 = producer6.result()
        producer7 = producer7.result()

    assert producer1 in [
        producer2,
        producer3,
        producer4,
        producer5,
        producer6,
        producer7,
    ]

    assert len(KafkaProducerPool._instances) == MAX_INSTANCE_LIMIT
