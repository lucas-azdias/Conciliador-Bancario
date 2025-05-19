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
        choices = ["load", "load_reports", "load_statements", "compile", "check", "all"],
        help = "Operation to perform (required): \"load\", \"compile\", \"check\", \"all\"")

    # Optional input reports file/folder
    parser.add_argument(
        "--in-reports",
        dest = "in-reports",
        default = "in/reports",
        required = False,
        help = "Input reports file/folder path (optional)"
    )

    # Optional input statements file/folder
    parser.add_argument(
        "--in-statements",
        dest = "in-statements",
        default = "in/statements",
        required = False,
        help = "Input statements file/folder path (optional)"
    )

    # Optional output folder
    parser.add_argument(
        "--output",
        dest = "output",
        default = "out",
        required = False,
        help = "output folder path (optional)"
    )

    # Optional reports archive folder
    parser.add_argument(
        "--archive-reports",
        dest = "archive-reports",
        default = "archive/reports",
        required = False,
        help = "Reports archive folder path (optional)"
    )

    # Optional statements archive folder
    parser.add_argument(
        "--archive-statements",
        dest = "archive-statements",
        default = "archive/statements",
        required = False,
        help = "Statements archive folder path (optional)"
    )

    # Optional db_schema_path (default in .env)
    parser.add_argument(
        "--db-schema-path",
        dest = "db-schema-path",
        default = os.getenv("DB_SCHEMA_PATH"),
        required = False,
        help = "Database schema file path (optional)"
    )

    # Optional database_path (default in .env)
    parser.add_argument(
        "--database-path",
        dest = "database-path",
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
    conciliador = Conciliador.Conciliador(
        **clear_empty_args(
            {
                "db_schema_path": pathlib.Path(args["db-schema-path"]),
                "database_path": pathlib.Path(args["database-path"]),
                "currency": args["currency"],
                "thousands": args["thousands"],
                "decimals": args["decimals"],
            }
        )
    )

    # Choose the operation and execute
    match args["operation"]:
        case "load":
            conciliador.load_reports(
                input = pathlib.Path(args["in-reports"]),
                archive = pathlib.Path(args["archive-reports"]),
                can_archive = True,
                can_overwrite_archive = True
            )
            conciliador.load_statements(
                input = pathlib.Path(args["in-statements"]),
                archive = pathlib.Path(args["archive-statements"]),
                can_archive = True,
                can_overwrite_archive = True
            )

        case "load_reports":
            conciliador.load_reports(
                input = pathlib.Path(args["in-reports"]),
                archive = pathlib.Path(args["archive-reports"]),
                can_archive = True,
                can_overwrite_archive = True
            )

        case "load_statements":
            conciliador.load_statements(
                input = pathlib.Path(args["in-statements"]),
                archive = pathlib.Path(args["archive-statements"]),
                can_archive = False,
                can_overwrite_archive = True
            )

        case "compile":
            conciliador.compile()

        case "check":
            conciliador.check()

        case "all":
            conciliador.load_reports()
            conciliador.load_statements()
            conciliador.compile()
            conciliador.check()

        case _:
            raise Exception(f"Invalid operation detected: {args["operation"]}")


if __name__ == "__main__":
    main()