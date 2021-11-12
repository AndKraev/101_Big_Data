from pathlib import Path
from unittest.mock import patch

import pandas as pd

from hw_hdfs.csv_to_parquet import CSVToParquet, arg_parser


def test_arg_parser_input_and_output_folders():
    parser = arg_parser(["path1", "path2"])
    assert parser.input_folder == "path1"
    assert parser.output_folder == "path2"


def test_csvtoparquet_input_and_output_folders():
    c = CSVToParquet(input_folder="./test1", output_folder="./test2")
    assert c.input_folder == Path("./test1")
    assert c.output_folder == Path("./test2")


def test_csvtoparquet_is_csv_true(tmp_path):
    with open(Path(tmp_path / "test.csv"), mode="w") as f:
        f.write("1")
    assert CSVToParquet.is_csv(Path(tmp_path / "test.csv"))


def test_csvtoparquet_is_csv_false(tmp_path):
    with open(Path(tmp_path / "test.txt"), mode="w") as f:
        f.write("1")
    assert not CSVToParquet.is_csv(Path(tmp_path / "test.csv"))


def test_csvtoparquet_convert(tmp_path):
    df = pd.DataFrame(
        {
            "one": [-1, 7, 2.5],
            "two": ["foo", "bar", "baz"],
            "three": [True, False, True],
        },
    )
    df.to_csv(Path(tmp_path / "test.csv"), index=False)
    CSVToParquet.convert(Path(tmp_path / "test.csv"), tmp_path)
    assert df.equals(pd.read_parquet(Path(tmp_path / "test.parquet"), engine="pyarrow"))


def test_csvtoparquet_convert_fail(tmp_path):
    with open(Path(tmp_path / "test.csv"), mode="w") as f:
        f.write(r"аы")
    CSVToParquet.convert(Path(tmp_path / "test.csv"), tmp_path)
    assert not Path(tmp_path / "test.parquet").is_file()


@patch("hw_hdfs.csv_to_parquet.CSVToParquet.is_csv")
@patch("hw_hdfs.csv_to_parquet.CSVToParquet.convert")
def test_csvtoparquet_convert_run_check_folder_creation(
    mocked_convert, mocked_is_csv, tmp_path
):
    CSVToParquet(tmp_path, tmp_path / "output").run()
    assert Path(tmp_path / "output").is_dir()


@patch("hw_hdfs.csv_to_parquet.CSVToParquet.is_csv")
@patch("hw_hdfs.csv_to_parquet.CSVToParquet.convert")
def test_csvtoparquet_convert_run(mocked_convert, mocked_is_csv, tmp_path):
    with open(Path(tmp_path / "test1.csv"), mode="w") as f:
        f.write(r"1")

    with open(Path(tmp_path / "test2.csv"), mode="w") as f:
        f.write(r"2")

    mocked_is_csv.return_value = True
    CSVToParquet(tmp_path, tmp_path).run()
    assert mocked_is_csv.call_count == 2
    assert mocked_convert.call_count == 2
