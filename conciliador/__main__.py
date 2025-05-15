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
        choices = ["load", "summarize", "verify"],
        help = "Operation to perform (required): \"load\", \"summarize\", \"verify\"")

    # Optional input file
    parser.add_argument(
        "--in",
        dest = "infolder",
        default = "in",
        required = False,
        help = "Input file path (optional)"
    )

    # Optional output file
    parser.add_argument(
        "--out",
        dest = "outfolder",
        default = "out",
        required = False,
        help = "Output file path (optional)"
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
    print(f"Input file: {args.infolder}")
    print(f"Output file: {args.outfolder}")


if __name__ == "__main__":
    main()