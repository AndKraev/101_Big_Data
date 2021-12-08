import json
import logging
from ctypes import c_bool
from multiprocessing import Process, Queue, Value
from pathlib import Path
from queue import Empty
from threading import Thread
from time import time
from typing import List

from kafka import KafkaProducer


class CSVtoKafka:
    """Sends a file to Kafka broker with KafkaProducer asynchronous send() method

    Constructor parameters:
        :param filepath: path to a file that must be sent.
        :type filepath: string
        :param sep: delimiter to use for a row.
        :type sep: string
        :param kafka_host: ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
        the producer should contact to bootstrap initial cluster metadata.
        :param topic: topic where the message will be published.
        :type topic: string
        :param workers: number of threads, default is 1
        :type workers: integer
        :param max_queue: maximum number of rows in queue, default is 1.000.000
        :type max_queue: integer
        :param report_timelag: seconds between progress reporting. Default 5 seconds.
        :type report_timelag: integer
        :return: None
    """

    def __init__(
        self,
        filepath: str,
        sep: str,
        kafka_host: str,
        topic: str,
        workers: int = 1,
        max_queue: int = 1_000_000,
        report_timelag: int = 5,
    ) -> None:
        """Constructor method"""
        self.filepath = Path(filepath)
        self.sep = sep
        self.kafka_host = kafka_host
        self.topic = topic
        self.report_timelag = report_timelag
        self.workers = workers if workers >= 1 else 1
        self._counter = 0
        self._queue = Queue(maxsize=max_queue)
        self._is_reading_file = Value(c_bool, False)

    def send(self) -> None:
        """Run sending messages to Kafka broker. Reads the file and create several
        parallel processes to send messages to Kafka.

        :return: None
        """
        logging.info("Starting daemon...")
        workers_pool = []

        with open(self.filepath, mode="r") as file:
            self._is_reading_file.value = True
            headers = file.readline().rstrip().split(self.sep)

            reporter = Thread(target=self._report_progress)
            reporter.start()

            for _ in range(self.workers):
                worker = Process(target=self._worker, args=(headers,))
                worker.start()
                workers_pool.append(worker)

            for line in file:
                self._queue.put(line)
                self._counter += 1

            self._is_reading_file.value = False

            for worker in workers_pool + [reporter]:
                worker.join()

        logging.info(f"Finished! {self._counter} has been successfully sent to Kafka.")

    def _worker(self, header: List[str]) -> None:
        """Created a producer infinite loop while parent method is reading file or queue
        is not empty. Worker tries to get a row from queue, create dict with a headers
         and send it to Kafka.

         :param header: A list of headers that will be used as keys for a dict
         :type header: list of strings
         :return: None
        """
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_host,
            linger_ms=500,
            batch_size=1638400,
            buffer_memory=335544320,
            compression_type="snappy",
            value_serializer=lambda m: json.dumps(m).encode("ascii"),
        )

        while not self._queue.empty() or self._is_reading_file.value:
            try:
                row = self._queue.get_nowait()
                if row:
                    data = dict(zip(header, row.rstrip().split(self.sep)))
                    producer.send(self.topic, data)
            except Empty:
                continue

        producer.flush()
        producer.close()

    def _report_progress(self) -> None:
        """Simple reporter that logs number of handled rows every n-seconds

        :return: None
        """
        last_report = time()
        while self._is_reading_file.value or not self._queue.empty():
            if time() - last_report >= self.report_timelag:
                last_report = time()
                logging.info(
                    f"{self._counter - self._queue.qsize()} rows has been sent, "
                    f"continue sending..."
                )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    CSVtoKafka(
        filepath="train.csv",
        sep=",",
        kafka_host="localhost:9092",
        topic="hotels",
        workers=6,
        max_queue=1_000_000,
        report_timelag=5,
    ).send()
