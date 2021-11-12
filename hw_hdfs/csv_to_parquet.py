import argparse
import sys
from pathlib import Path

import pandas as pd


def arg_parser(args) -> argparse:
    """Function to obtain parameters from console to run the converter

    :param args: args for testing
    :return: parameters from console
    :type: argparse.parse_args
    """
    parser = argparse.ArgumentParser(
        prog="Converter of CSV files to Parquet",
        description="""Converts an input folder with CSV files to parquet files to
        an output folder""",
    )
    parser.add_argument(
        "input_folder", type=str, help="a path to an input folder with csv files"
    )
    parser.add_argument("output_folder", type=str, help="a path to an output folder")
    return parser.parse_args(args)


class CSVToParquet:
    """Converts all CSV files in the input folder to parquet files to an output folder

    Constructor arguments:
        :param input_folder: Path to an folder with CSV files
        :type input_folder: String
        :param output_folder: Path to an output folder to store parquet files
        :type output_folder: String
    """

    def __init__(self, input_folder: str, output_folder: str) -> None:
        """Constructor method

        :return: None
        :rtype: NoneType
        """
        self.input_folder = Path(input_folder)
        self.output_folder = Path(output_folder)

    def run(self) -> None:
        """Creates output folder and converts files one by one

        :return: None
        :rtype: NoneType
        """
        self.output_folder.mkdir(parents=True, exist_ok=True)
        for file in self.input_folder.iterdir():
            if self.is_csv(file):
                self.convert(file, self.output_folder)

        print("Completed!")

    @staticmethod
    def is_csv(input_file: Path) -> bool:
        """Checks if a file is a csv file or not.

        :param input_file: Path on a file
        :type input_file: Path
        :return: Returns True if CSV. Otherwise False.
        :rtype: Boolean
        """
        return input_file.is_file() and input_file.name.endswith(".csv")

    @staticmethod
    def convert(input_file: Path, output_folder: Path) -> None:
        """Reads a files as CSV and saves as Parquet

        :param input_file: Path on a file
        :type input_file: Path
        :param output_folder: Path to an output folder
        :type input_file: Path
        :return: None
        :rtype: NoneType
        """
        try:
            pd.read_csv(input_file).to_parquet(
                output_folder / f"{input_file.name[:-4]}.parquet", engine="pyarrow"
            )
            print(
                f"[INFO] {input_file.name} has been successfully converted to Parquet."
            )
        except:
            print(f"[WARNING] Could not convert {input_file.name} to Parquet!")


if __name__ == "__main__":
    CSVToParquet(*vars(arg_parser(sys.argv[1:])).values()).run()
