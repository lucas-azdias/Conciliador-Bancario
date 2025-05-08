import argparse


def main():
    parser = argparse.ArgumentParser(description="Process some operation with input/output files.")

    # Required operation
    parser.add_argument(
        "operation",
        choices=["load", "summarize", "verify"],
        help="Operation to perform (required): \"load\", \"summarize\", \"verify\"")

    # Optional input file
    parser.add_argument(
        "--in",
        dest="infolder",
        default="in",
        required=False,
        help="Input file path (optional)"
    )

    # Optional output file
    parser.add_argument(
        "--out",
        dest="outfolder",
        default="out",
        required=False,
        help="Output file path (optional)"
    )

    args = parser.parse_args()

    print(f"Operation: {args.operation}")
    print(f"Input file: {args.infolder}")
    print(f"Output file: {args.outfolder}")


if __name__ == "__main__":
    main()