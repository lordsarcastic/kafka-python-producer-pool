import random
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from generic_producer_pool.base import ProducerPool



class IntProducerPool(ProducerPool[int]):
    producer_class = int
    INSTANCE_LIMIT = 2

    @classmethod
    def create_instance(cls):
        return int(random.randint(1, 100))


def work(sleep: int = 4, thread: int = 1):
    # time.sleep(sleep)
    result = IntProducerPool()

    # mocking expensive operation
    time.sleep(sleep)
    return result


@pytest.mark.xfail
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

    assert len(IntProducerPool._instances) == 2
