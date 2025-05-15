import argparse
import dotenv
import os


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
    args = parser.parse_args()

    print(f"Operation: {args.operation}")
    print(f"Input file/folder: {args.input}")
    print(f"Output folder: {args.output}")
    print(f"Archive folder: {args.archive}")


if __name__ == "__main__":
    main()