import argparse
import dotenv
import os
import pathlib
import typing

from .src import Conciliador


def main():
    # Load environment variables from .env file
    dotenv.load_dotenv(
        dotenv.find_dotenv(raise_error_if_not_found = True),
        verbose = True,
        override = True,
        encoding = "utf-8"
    )

    # Create parser for arguments
    parser = argparse.ArgumentParser(description = "Process some operation with input/output files.")

    # Required operation
    parser.add_argument(
        "operation",
        choices = ["load", "load_reports", "load_statements", "link", "all"],
        help = "Operation to perform (required): \"load\", \"load_reports\", \"load_statements\", \"link\", \"all\"")

    # Optional input reports file/folder
    parser.add_argument(
        "--in-reports",
        dest = "in-reports",
        default = os.getenv("IN_REPORTS") or "in/reports",
        required = False,
        help = "Input reports file/folder path (optional)"
    )

    # Optional input statements file/folder
    parser.add_argument(
        "--in-statements",
        dest = "in-statements",
        default = os.getenv("IN_STATEMENTS") or "in/statements",
        required = False,
        help = "Input statements file/folder path (optional)"
    )

    # Optional output folder
    parser.add_argument(
        "--output",
        dest = "output",
        default = os.getenv("OUTPUT") or "out",
        required = False,
        help = "output folder path (optional)"
    )

    # Optional reports archive folder
    parser.add_argument(
        "--archive-reports",
        dest = "archive-reports",
        default = os.getenv("ARCHIVE_REPORTS") or "archive/reports",
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

    # Optional database URI
    parser.add_argument(
        "--database-uri",
        dest = "database-uri",
        default = os.getenv("DATABASE_URI") or "sqlite:///:memory:",
        required = False,
        help = "Database file path (optional)"
    )

    # Optional database log path
    parser.add_argument(
        "--database-log-path",
        dest = "database-log-path",
        default = os.getenv("DATABASE_LOG_PATH") or "conciliador/db/database.log",
        required = False,
        help = "Database log file path (optional)"
    )

    # Optional database insertions path
    parser.add_argument(
        "--database-insertions-path",
        dest = "database-insertions-path",
        default = os.getenv("DATABASE_INSERTIONS_PATH") or "conciliador/db/db_insertions.json",
        required = False,
        help = "Database insertions file path (optional)"
    )

    # Optional currency
    parser.add_argument(
        "--currency",
        dest = "currency",
        default = os.getenv("CURRENCY"),
        required = False,
        help = "Currency (optional)"
    )

    # Optional thousands
    parser.add_argument(
        "--thousands",
        dest = "thousands",
        default = os.getenv("THOUSANDS"),
        required = False,
        help = "Currency thousands symbol (optional)"
    )

    # Optional decimals
    parser.add_argument(
        "--decimals",
        dest = "decimals",
        default = os.getenv("DECIMALS"),
        required = False,
        help = "Currency decimals symbol (optional)"
    )

    # Flag dev-mode
    parser.add_argument(
        "--dev-mode",
        dest = "dev-mode",
        action = "store_true",
        default = (os.getenv("DEV_MODE", "False").lower() in {"1", "true", "yes", "on"}),
        required = False,
        help = "Set developer mode on"
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
                "database_uri": args["database-uri"],
                "database_log_path": pathlib.Path(args["database-log-path"]),
                "database_insertions_path": pathlib.Path(args["database-insertions-path"]),
                "currency": args["currency"],
                "thousands": args["thousands"],
                "decimals": args["decimals"],
                "has_dev_mode": args["dev-mode"]
            }
        )
    )

    # Choose the operation and execute
    match args["operation"]:
        case "load":
            conciliador.load_reports(
                input = pathlib.Path(args["in-reports"]),
                archive = pathlib.Path(args["archive-reports"]),
                can_archive = not args["dev-mode"],
                can_overwrite_archive = not args["dev-mode"]
            )
            conciliador.load_statements(
                input = pathlib.Path(args["in-statements"]),
                archive = pathlib.Path(args["archive-statements"]),
                can_archive = not args["dev-mode"],
                can_overwrite_archive = not args["dev-mode"]
            )

        case "load_reports":
            conciliador.load_reports(
                input = pathlib.Path(args["in-reports"]),
                archive = pathlib.Path(args["archive-reports"]),
                can_archive = not args["dev-mode"],
                can_overwrite_archive = not args["dev-mode"]
            )

        case "load_statements":
            conciliador.load_statements(
                input = pathlib.Path(args["in-statements"]),
                archive = pathlib.Path(args["archive-statements"]),
                can_archive = not args["dev-mode"],
                can_overwrite_archive = not args["dev-mode"]
            )

        case "link":
            from datetime import date
            conciliador.link(date(2025, 4, 24), date(2025, 4, 24))

        case "all":
            conciliador.load_reports()
            conciliador.load_statements()
            conciliador.link()

        case _:
            raise Exception(f"Invalid operation detected: {args["operation"]}")


if __name__ == "__main__":
    main()