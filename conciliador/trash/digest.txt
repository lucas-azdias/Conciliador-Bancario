from datetime import date
from json import load
from pathlib import Path
from sys import argv, modules

from . import DIR_SUMMARIES_PATH, summary_filename


def generate_digest(summaries_folder: Path, selected_date: date | None = None):
    if not selected_date:
        selected_date = date.today()

    year = selected_date.year
    month = selected_date.month
    day = selected_date.day

    filepath = summary_filename(year, month, day)
    with open(summaries_folder / f"{year}-{month:02}" / filepath, "r", encoding="utf-8") as file:
        content = load(file)

    # Formatting the digest of the summary
    text_digest = ""

    # Date
    text_digest += f"{content.pop("day")}/{content.pop("month")}/{content.pop("year")}"

    # Entries
    for k, v in content["statements"].items():
        print(k, v)
    
    print(content)


if __name__ == "__main__":
    selected_date = None

    try:
        # Using date as parameters if given
        if len(argv) >= 2:
            if len(argv[1].split("/")) == 3:
                selected_date = date(*map(int, argv[1].split("/")))
            elif len(argv) == 3:
                selected_date = date(*map(int, argv[1:]))

    except:
        print(f"Invalid parameters given. Try \"py -m {modules[__name__].__spec__.name} [<yyyy/mm/dd>]\"")

    else:
        generate_digest(DIR_SUMMARIES_PATH, selected_date)