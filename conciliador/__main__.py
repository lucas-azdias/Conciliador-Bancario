import argparse
import dotenv
import os
import pathlib
import typing

from . import Conciliador


def main():
    # Load environment variables from .env file
    dotenv.load_dotenv(
        dotenv.find_dotenv(raise_error_if_not_found = True),
        verbose = True,
        encoding = "utf-8"
    )

    # Create parser for arguments
    parser = argparse.ArgumentParser(description = "Process some operation with input/output files.")

    # Required operation
    parser.add_argument(
        "operation",
        choices = ["load", "compile", "check", "all"],
        help = "Operation to perform (required): \"load\", \"compile\", \"check\", \"all\"")

    # Optional input file/folder
    parser.add_argument(
        "--input",
        dest = "input",
        default = "in",
        required = False,
        help = "Input file/folder path (optional)"
    )

    # Optional output folder
    parser.add_argument(
        "--output",
        dest = "output",
        default = "out",
        required = False,
        help = "output folder path (optional)"
    )

    # Optional archive folder
    parser.add_argument(
        "--archive",
        dest = "archive",
        default = "archive",
        required = False,
        help = "Archive folder path (optional)"
    )

    # Optional db_schema_path (default in .env)
    parser.add_argument(
        "--db_schema_path",
        dest = "db_schema_path",
        default = os.getenv("DB_SCHEMA_PATH"),
        required = False,
        help = "Database schema file path (optional)"
    )

    # Optional database_path (default in .env)
    parser.add_argument(
        "--database_path",
        dest = "database_path",
        default = os.getenv("DATABASE_PATH"),
        required = False,
        help = "Database file path (optional)"
    )

    # Optional currency (default in .env)
    parser.add_argument(
        "--currency",
        dest = "currency",
        default = os.getenv("CURRENCY"),
        required = False,
        help = "Currency (optional)"
    )

    # Optional thousands (default in .env)
    parser.add_argument(
        "--thousands",
        dest = "thousands",
        default = os.getenv("THOUSANDS"),
        required = False,
        help = "Currency thousands symbol (optional)"
    )

    # Optional decimals (default in .env)
    parser.add_argument(
        "--decimals",
        dest = "decimals",
        default = os.getenv("DECIMALS"),
        required = False,
        help = "Currency decimals symbol (optional)"
    )

    # Load and parse arguments
    args = {
        key: value
        for key, value in vars(parser.parse_args()).items()
    }

    # Function to clear empty values in args
    clear_empty_args: typing.Callable[[typing.Dict[str, typing.Any]], typing.Dict[str, typing.Any]] = lambda d: {
        key: value
        for key, value in d.items()
        if not value is None
    }

    # Instanciating the main class for the program
    Conciliador.Conciliador(
        **clear_empty_args(
            {
                "db_schema_path": pathlib.Path(args["db_schema_path"]),
                "database_path": pathlib.Path(args["database_path"]),
                "currency": args["currency"],
                "thousands": args["thousands"],
                "decimals": args["decimals"],
            }
        )
    )


if __name__ == "__main__":
    main()