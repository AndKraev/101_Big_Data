from pathlib import Path
from unittest.mock import Mock, call, mock_open, patch

from pytest import fixture

from hw_streaming.CSV_Kafka import CSVtoKafka


@fixture
def TestCSVtoKafka():
    return CSVtoKafka(
        filepath="fakepath",
        sep="fakesep",
        kafka_host="fakehost",
        topic="faketopic",
    )


@patch("hw_streaming.CSV_Kafka.Queue")
def test_CSVtoKafka_construction_method(mocked_queue):
    instance = CSVtoKafka(
        filepath="fakepath",
        sep="fakesep",
        kafka_host="fakehost",
        topic="faketopic",
    )
    assert instance.filepath == Path("fakepath")
    assert instance.sep == "fakesep"
    assert instance.kafka_host == "fakehost"
    assert instance.topic == "faketopic"

    # Checking default values
    assert instance.workers == 1
    assert instance.report_timelag == 5
    mocked_queue.assert_called_once_with(maxsize=1_000_000)


@patch("hw_streaming.CSV_Kafka.logging.info")
def test_CSVtoKafka_report_progress(mocked_logging, TestCSVtoKafka):
    mocked_queue = Mock()
    mocked_queue.empty.side_effect = [False, True]
    mocked_queue.qsize.return_value = 3
    TestCSVtoKafka._counter = 15
    TestCSVtoKafka._queue = mocked_queue
    TestCSVtoKafka.report_timelag = 0
    TestCSVtoKafka._report_progress()
    expected_text = "12 rows has been sent, continue sending..."
    mocked_logging.assert_called_with(expected_text)


@patch("hw_streaming.CSV_Kafka.logging.info")
def test_CSVtoKafka_not_report_progress(mocked_logging, TestCSVtoKafka):
    mocked_queue = Mock()
    mocked_queue.empty.side_effect = [False, True]
    TestCSVtoKafka._queue = mocked_queue
    TestCSVtoKafka.report_timelag = 1000
    TestCSVtoKafka._report_progress()
    mocked_logging.assert_not_called()


@patch("hw_streaming.CSV_Kafka.KafkaProducer")
def test_CSVtoKafka__worker_send_to_producer(mocked_producer, TestCSVtoKafka):
    mocked_queue = Mock()
    mocked_queue.empty.side_effect = [False, False, True]
    mocked_queue.get_nowait.side_effect = ["value1,value2", "value3,value4"]
    mocked_producer.return_value = mocked_producer
    TestCSVtoKafka._queue = mocked_queue
    TestCSVtoKafka.sep = ","

    TestCSVtoKafka._worker(["key1", "key2"])
    expected_args = [
        call("faketopic", {"key1": "value1", "key2": "value2"}),
        call("faketopic", {"key1": "value3", "key2": "value4"}),
    ]
    mocked_producer.send.assert_has_calls(expected_args)


@patch(
    "hw_streaming.CSV_Kafka.open",
    new_callable=mock_open,
    read_data="header1,header2\nvalue1,value2\nvalue3,value4",
)
@patch("hw_streaming.CSV_Kafka.Thread")
@patch("hw_streaming.CSV_Kafka.Process")
def test_CSVtoKafka_send(mocked_process, mocked_thread, mocked_open, TestCSVtoKafka):
    mocked_queue = Mock()
    mocked_queue.return_value = mocked_queue
    TestCSVtoKafka._queue = mocked_queue
    TestCSVtoKafka.sep = ","

    TestCSVtoKafka.send()
    mocked_process.assert_called_once_with(
        target=TestCSVtoKafka._worker, args=(["header1", "header2"],)
    )
    mocked_thread.assert_called_once_with(target=TestCSVtoKafka._report_progress)
    mocked_queue.put.assert_has_calls([call("value1,value2\n"), call("value3,value4")])
