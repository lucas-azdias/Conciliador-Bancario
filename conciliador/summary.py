from collections import defaultdict
from datetime import date
from json import dump, load
from pathlib import Path
from re import match
from sys import argv, modules
from traceback import print_exception
from typing import Any

from . import DIR_DATABASE_PATH, DIR_SUMMARIES_PATH, SUMMARY_TEMPLATE_PATH, TEMPLATE_DEFAULT_KEY_PATH, TEMPLATE_DEFAULT_KEY_VALUE, TEMPLATE_DEFAULT_KEY_INCOME, TEMPLATE_DEFAULT_KEY_OUTCOME, database_filename, summary_filename
from .utils import n_flatten, get_nested_dict_element, get_nested_dict_elements


def create_summary_default_elem(value: Any | None = None) -> dict:
    """
    Creates a default summary element.

    Parameters:
        name (Any | None): The value of the element.

    Returns:
        dict: The default summary element.
    """
    return {
        TEMPLATE_DEFAULT_KEY_VALUE: value,
        "is_verified": False,
        "is_verification_manual": False,
        "verification_dates": None,
    }


def load_template(template_path: Path) -> dict:
    """Loads a structured template file.
    
    Args:
        template_path (Path): Path to the template file.

    Returns:
        dict: Parsed template dictionary.
    """
    with template_path.open("r", encoding="utf-8") as file:
        return load(file)


def generate_summaries(database_folder: Path, output_folder: Path, template_path: Path, selected_year: int | None = None, selected_month: int | None = None, selected_day: int | None = None) -> None:
    """Reads from the database and generates a summary JSON file for each day based on a structured template.

    Args:
        database_folder (Path): Directory containing JSON database files.
        output_folder (Path): Directory where summary JSON files will be saved.
        template_path (Path): Path to the structured template file.
        selected_year (int | None, optional): Year of databases selected. Defaults to "None".
        selected_month (int | None, optional): Month of databased selected. Defaults to "None".
        selected_day (int | None, optional): Day of databased selected. Defaults to "None".
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    template = load_template(template_path)
    all_matched_names = []
    
    try:
        for db_file in database_folder.glob(database_filename()):
            with db_file.open("r", encoding="utf-8") as file:
                database = load(file)
            
            year = int(database["year"])
            month = int(database["month"])

            if selected_year and selected_year != year:
                continue

            if selected_month and selected_month != month:
                continue

            # Sort data by day (each key inside each day)
            data_by_days = defaultdict(lambda: defaultdict(defaultdict))
            for key, value in database.items():
                if not isinstance(value, dict):
                    continue
                for day, entries in value.items():
                    data_by_days[int(day)][key] = entries
            
            # For each key in each day, execute the template instructions and register the results
            for day, data in data_by_days.items():
                if selected_day and selected_day != day:
                    continue

                summary = dict.fromkeys(template.keys(), {})
                summary["year"] = year
                summary["month"] = month
                summary["day"] = day

                # For each entry in the data of the day, format and save in the summary
                for key, entries in data.items():
                    if not key in template.keys():
                        continue

                    summary[key] = {}
                    key_template = template[key]

                    key_entries = []
                    if TEMPLATE_DEFAULT_KEY_PATH in key_template.keys():
                        path = key_template[TEMPLATE_DEFAULT_KEY_PATH]
                    
                        # Groups all entries in one list of entries
                        key_entries = n_flatten(get_nested_dict_elements(entries, path), 2)
                    else:
                        # Already grouped under the day
                        key_entries = entries

                    # Go through every nested item via a path in summary and uses the rule to format it
                    template_items = [((k, ), v) for k, v in key_template.items() if k != TEMPLATE_DEFAULT_KEY_PATH]
                    while template_items:
                        path, rules = template_items.pop(0)

                        # Handle nested dicts
                        if isinstance(rules, dict):
                            get_nested_dict_element(summary[key], path[:-1])[path[-1]] = {}
                            for subkey, subrules in list(rules.items())[::-1]:
                                template_items.insert(0, ((*path, subkey), subrules))

                        # Handle rules
                        elif isinstance(rules, list):
                            operation, name_pattern = rules[0:2]
                            value_pattern = ".*" if len(rules) < 3 else rules[2]

                            matched = [(name, int(value)) for name, value in key_entries if match(name_pattern, name) and match(value_pattern, str(value))] if name_pattern else []
                            matched_values = [value for _, value in matched]
                            all_matched_names.extend([name for name, _ in matched])

                            result = None
                            match operation:
                                case "sum":
                                    result = create_summary_default_elem(sum(matched_values))
                                case "for":
                                    result = []
                                    for i in range(len(matched_values)):
                                        result.append(create_summary_default_elem(matched_values[i]))

                            get_nested_dict_element(summary[key], path[:-1])[path[-1]] = result
                    
                    summary[key][TEMPLATE_DEFAULT_KEY_INCOME] = [(name, value) for name, value in key_entries if not name in all_matched_names and value >= 0]
                    summary[key][TEMPLATE_DEFAULT_KEY_OUTCOME] = [(name, value) for name, value in key_entries if not name in all_matched_names and value < 0]
                    
                    output_subfolder = output_folder / f"{year}-{month:02}"
                    output_subfolder.mkdir(parents=True, exist_ok=True)
                    output_filepath = output_subfolder / summary_filename(year, month, day)
                    with output_filepath.open("w", encoding="utf-8") as summary_file:
                        dump(summary, summary_file, indent=4, ensure_ascii=False)

    except Exception as e:
        print(f"Error generating summaries:")
        print_exception(e)


def verify_summary(summaries_folder: Path, selected_date: date | None = None):
    if not selected_date:
        selected_date = date.today()

    year = selected_date.year
    month = selected_date.month
    day = selected_date.day

    filepath = summary_filename(year, month, day)
    with open(summaries_folder / f"{year}-{month:02}" / filepath, "r", encoding="utf-8") as file:
        content = load(file)
    
    print(content)


if __name__ == "__main__":
    year = None
    month = None
    day = None
    selected_date = None


    try:
        match argv[1]:
            case "generate":
                # Using year, month and day as parameters if given
                if len(argv) >= 3:
                    year = int(argv[2])
                if len(argv) >= 4:
                    month = int(argv[3])
                if len(argv) >= 5:
                    day = int(argv[4])
            
                generate_summaries(DIR_DATABASE_PATH, DIR_SUMMARIES_PATH, SUMMARY_TEMPLATE_PATH, year, month, day)
                
            case "verify":
                # Using date as parameters
                if len(argv) >= 3:
                    if len(argv[2].split("/")) == 3:
                        selected_date = date(*map(int, argv[2].split("/")))
                    elif len(argv[2:]) == 3:
                        selected_date = date(*map(int, argv[2:]))

                verify_summary(DIR_SUMMARIES_PATH, selected_date)

    except:
        print(f"Invalid parameters given. Try \"py -m {modules[__name__].__spec__.name} <action> [<year>] [<month>] [<day>]\"")