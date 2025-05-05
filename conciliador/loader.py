import pandas as pd

from chardet import detect
from collections import defaultdict
from datetime import datetime
from json import dump, load
from pathlib import Path
from shutil import move
from traceback import print_exception

from . import DATABASE_TEMPLATE_PATH, TEMPLATE_DEFAULT_KEY_PATH, TEMPLATE_DEFAULT_KEY_STATEMENTS, TEMPLATE_DEFAULT_KEY_REPORTS, database_filename
from .utils import log_change


def save_database(new_registries: dict, key: str, database_folder: Path, year: int, month: int, patterns_path: list[str] = []) -> set:
    """
    Saves new registry data into a JSON database file while preserving existing data.

    Args:
        new_registries (dict): A dictionary where keys represent days (int) and values are lists or dictionaries of entries to be stored.
        key (str): The target key to save the registries.
        database_folder (Path): The folder where the database files are stored.
        year (int): The target year for the database entry.
        month (int): The target month for the database entry.
        patterns_path (list[str], optional): The patterns path for the entries if the values of the registries are dictionaries.

    Returns:
        set: A set containing strings representing the dates (in "DD/MM/YYYY" format) where existing data was overwritten.
    """
    log_overwritten = set()
    output_filepath = database_folder / database_filename(year, month)
    temp_filepath = output_filepath.with_suffix(".tmp")  # Temporary file for rollback
    
    try:
        # Load template
        with open(DATABASE_TEMPLATE_PATH, "r", encoding="utf-8") as file:
            database = load(file)

        # Load existing data if file exists
        if output_filepath.exists():
            with output_filepath.open("r", encoding="utf-8") as json_file:
                existing_data = load(json_file)
                for prop in existing_data.keys():
                    database[prop] = existing_data[prop]
        
        database["year"] = year
        database["month"] = month
        existing_reports = database[key]

        # Update only the changed days and log overwrites
        for day, entries in new_registries.items():
            if str(day) in existing_reports:
                log_overwritten.add(f"{day:02}/{month:02}/{year}")
            existing_reports[str(day)] = entries

        save_registries = {}
        if patterns_path:
            save_registries[TEMPLATE_DEFAULT_KEY_PATH] = patterns_path
        save_registries.update(dict(sorted(existing_reports.items())))
        database[key] = save_registries

        # Write to temporary file first
        with temp_filepath.open("w", encoding="utf-8") as json_file:
            dump(database, json_file, indent=4, ensure_ascii=False)

        # Replace original file with the temporary one
        move(temp_filepath, output_filepath)

    except Exception as e:
        # Cleanup in case of failure
        if temp_filepath.exists():
            temp_filepath.unlink()  # Remove temporary file
        raise RuntimeError(f"Failed to save database: {e}")

    return log_overwritten


def report_to_DataFrames(file_content: str) -> list[pd.DataFrame]:
    """
    Converts the content of a report file into a list of Pandas DataFrames.

    Args:
        file_content (str): The content of the report file as a string.

    Returns:
        list[pd.DataFrame]: A list of DataFrames, where each DataFrame represents a structured section of the report.
    """
    # Remove unuseful header
    file_content = file_content.split("\n", maxsplit=4)[-1]

    # Remove unuseful footer
    file_content = file_content.rsplit("\n", maxsplit=3)[0]

    # Get separated header
    sep_header, file_content = file_content.split("\n", maxsplit=1)
    sep_header = sep_header.split(";")

    # Split the content by the first element and filter last columns
    sections = []
    rows = file_content.split("\n")
    for row in rows:
        columns = row.split(";")

        if columns[0].strip(): # If it is a new element
            sections.append([columns[:-5]])
            continue
        
        sections[-1].append(columns[:-5])

    # Condense the first row info into other rows (excluding the header)
    for i, section in enumerate(sections):        
        info, header, section, _ = section[0], section[1], section[2:-1], section[-1]

        # Get the info
        name = info[0]
        date, start = info[4].split(" ")[:2]
        _, end = info[3].split(" ")[:2]

        for j in range(len(section)):
            section[j][0] = name
            section[j][2] = date
            section[j][3] = start
            section[j].insert(3, end)
            section[j].insert(4, section[j].pop(1))
        
        # Adjust the header
        header[0] = sep_header[0]
        header[3] = header[1]
        header[1] = "Data"
        header[2] = "Início"
        header.insert(3, "Término")

        # Convert the sections into DataFrames and store them
        sections[i] = pd.DataFrame(section, columns=header)
        sections[i].loc[sections[i][sections[i].columns[4]] == "RECEBIMENTO DINHEIRO", "Total"] = sections[i]["Informado"]
        sections[i].drop(columns={"Informado"}, inplace=True)
    
    return sections


def statements_loader(extratos_folder: Path, database_folder: Path, logs_folder: Path, processed_folder: Path, encoding: str | None=None) -> None:
    """Loads all CSV files from the specified folder and its subfolders, extracts financial data, 
    and saves it as JSON files grouped by year and month without overwriting
    unchanged registries. Moves processed files to an archive folder.
    The file must contain only one month/year data.

    Args:
        extratos_folder (Path): Directory containing CSV files to be processed.
        database_folder (Path): Directory where JSON output files will be saved.
        logs_folder (Path): Directory where log files will be saved.
        processed_folder (Path): Directory where processed CSV files will be moved.
        encoding (str | None): Encoding for reading the files. Defaults to "None" will automatically be detected.
    """
    database_folder.mkdir(parents=True, exist_ok=True)
    processed_folder.mkdir(parents=True, exist_ok=True)
    
    # Dictionary to store data grouped by year and month
    data_by_month = defaultdict(lambda: defaultdict(list))
    log_entries = set()
    log_overwritten = set()
    filepaths = []
    
    try:
        # Iterate over all CSV files in the folder and subfolders
        for filepath in extratos_folder.rglob("*.csv"):
            # Detecting the right encoding for the file (if not given)
            if not encoding:
                with open(filepath, "rb") as file:
                    file_encoding = detect(file.read())["encoding"]
                    file.close()
            else:
                file_encoding = encoding

            # Reading the file to a DataFrame
            df = pd.read_csv(filepath, sep=";", encoding=file_encoding)
            
            # Normalize column names
            df.columns = ["Data", "Histórico", "Valor"]
            
            years = set()
            months = set()
            for _, row in df.iterrows():
                date, name, value = row["Data"], row["Histórico"], row["Valor"]
                day, month, year = map(int, date.split("/"))
                value = str(value).replace(".", "").split(",")
                value = int(f"{value[0]}{value[1]:02}")  # Ensure value is a int
                
                data_by_month[(year, month)][day].append([name, value])
                log_entries.add(f"{day:02}/{month:02}/{year}")
                years.add(year)
                months.add(month)
            
            # Garantee the file has only one month/year data
            if len(years) > 1 or len(months) > 1:
                raise Exception("File has data from more than one month/year inside of it.")
            
            # Registering file to be moved
            filepaths.append((filepath, processed_folder / f"{year}-{month:02}"))
        
        # Save data to JSON files
        for (year, month), new_registries in data_by_month.items():
            save_database(new_registries, TEMPLATE_DEFAULT_KEY_STATEMENTS, database_folder, year, month)
        
        # Apply logs only if no errors occurred
        for date in log_entries:
            day, month, year = map(int, date.split("/"))
            log_change(year, month, f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} | {TEMPLATE_DEFAULT_KEY_STATEMENTS} - added {day}" if not date in log_overwritten else f"overwritten {day}", logs_folder)
        
    except Exception as e:
        print(f"Error processing files:")
        print_exception(e)

    else:
        # Move processed files
        for filepath, archive_folder in filepaths:
            archive_folder.mkdir(parents=True, exist_ok=True)
            filepath.rename(archive_folder / filepath.name)


def reports_loader(relatorios_folder: Path, database_folder: Path, logs_folder: Path, processed_folder: Path, encoding: str | None=None) -> None:
    """Loads all CSV files from the specified folder and its subfolders, extracts reports data, 
    and saves it as JSON files grouped by year and month without overwriting
    unchanged registries. Moves processed files to an archive folder.
    The file must contain only one month/year data.

    Args:
        relatorios_folder (Path): Directory containing CSV files to be processed.
        database_folder (Path): Directory where JSON output files will be saved.
        logs_folder (Path): Directory where log files will be saved.
        processed_folder (Path): Directory where processed CSV files will be moved.
        encoding (str | None): Encoding for reading the files. Defaults to "None" will automatically be detected.
    """
    database_folder.mkdir(parents=True, exist_ok=True)
    processed_folder.mkdir(parents=True, exist_ok=True)

    # Dictionary to store data grouped by year and month
    data_by_month = defaultdict(lambda: defaultdict(defaultdict))
    log_entries = set()
    log_overwritten = set()
    filepaths = []

    try:
        # Iterate over all CSV files in the folder and subfolders
        for filepath in relatorios_folder.rglob("*.csv"):
            # Detecting the right encoding for the file (if not given)
            if not encoding:
                with open(filepath, "rb") as file:
                    file_encoding = detect(file.read())["encoding"]
                    file.close()
            else:
                file_encoding = encoding
            
            with open(filepath, "r", encoding=file_encoding) as file:
                file_content = file.read()
                file.close()
            
            sections = report_to_DataFrames(file_content)
            
            years = set()
            months = set()
            for df in sections:
                # Normalize column names
                df.columns = ["Funcionário", "Data", "Início", "Término", "Finalizadora", "Total"]

                employee, date, start, end = df["Funcionário"][0], df["Data"][0], df["Início"][0], df["Término"][0]
                day, month, year = map(int, date.split("/"))

                # Create a common header for the shift
                data_by_month[(year, month)][day][start] = {
                    "funcionario": employee,
                    "inicio": start,
                    "termino": end,
                    "finalizadoras": [],
                }
                log_entries.add(f"{day:02}/{month:02}/{year}")
                years.add(year)
                months.add(month)
                
                for _, row in df.iterrows():
                    finisher, total = row["Finalizadora"], row["Total"]
                    total = str(total).replace(".", "").split(",")
                    total = int(f"{total[0]}{total[1]:02}")  # Ensure total is a int
                    data_by_month[(year, month)][day][start]["finalizadoras"].append([finisher, total])
            
            # Garantee the file has only one month/year data
            if len(years) > 1 or len(months) > 1:
                raise Exception("File has data from more than one month/year inside of it.")

            # Registering file to be moved
            filepaths.append((filepath, processed_folder / f"{year}-{month:02}"))

        # Save data to JSON files
        for (year, month), new_registries in data_by_month.items():
            # Sort by shift
            for day, entries in new_registries.items():
                new_registries[day] = {str(i + 1): v for i, (_, v) in enumerate(sorted(entries.items()))}

            save_database(new_registries, TEMPLATE_DEFAULT_KEY_REPORTS, database_folder, year, month)

        # Apply logs only if no errors occurred
        for date in log_entries:
            day, month, year = map(int, date.split("/"))
            log_change(year, month, f"{datetime.now().strftime('%Y/%m/%d %H:%M:%S')} | {TEMPLATE_DEFAULT_KEY_REPORTS} - added {day}" if not date in log_overwritten else f"overwritten {day}", logs_folder)

    except Exception as e:
        print(f"Error processing files:")
        print_exception(e)

    else:
        # Move processed files
        for filepath, archive_folder in filepaths:
            archive_folder.mkdir(parents=True, exist_ok=True)
            filepath.rename(archive_folder / filepath.name)


if __name__ == "__main__":
    from . import DIR_DATABASE_PATH, DIR_LOGS_PATH

    from . import DIR_IN_STATEMENTS_PATH, DIR_OUT_STATEMENTS_PATH
    statements_loader(DIR_IN_STATEMENTS_PATH, DIR_DATABASE_PATH, DIR_LOGS_PATH, DIR_OUT_STATEMENTS_PATH)
    
    from . import DIR_IN_REPORTS_PATH, DIR_OUT_REPORTS_PATH
    reports_loader(DIR_IN_REPORTS_PATH, DIR_DATABASE_PATH, DIR_LOGS_PATH, DIR_OUT_REPORTS_PATH)