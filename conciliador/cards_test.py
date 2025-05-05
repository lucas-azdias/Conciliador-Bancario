import json
import csv
import os
import unicodedata
import collections
from datetime import datetime, timedelta

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d")

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

def int_to_strfloat(num):
    str_num = str(num)
    if str_num.isdigit():
        if len(str_num) < 2:
            str_num = str_num.zfill(2)  # Pad with leading zero if less than 2 digits
        return str_num[:-2] + "," + str_num[-2:]
    else:
        return "0"

def get_relevant_database_files(start_date, end_date, database_path):
    database_files = []
    current_date = start_date
    while current_date <= end_date:
        folder = current_date.strftime("%Y_%m")
        filename = f"database.{folder}.json"
        full_path = os.path.join(database_path, filename)
        if os.path.exists(full_path) and full_path not in database_files:
            database_files.append(full_path)
        current_date += timedelta(days=1)
    return database_files

def expand_dict(data):
    has_found_value = True
    transformed_data = []
    index = 0
    while has_found_value:
        new_dict = collections.defaultdict(dict)
        has_found_value = False
        for key, sub_dict in data.items():
            for subkey, values in sub_dict.items():
                if len(values) > index:
                    new_dict[key][subkey] = values[index]
                    has_found_value = True
                else:
                    new_dict[key][subkey] = None
        if has_found_value:
            transformed_data.append(new_dict)
        index += 1
    return transformed_data
    

def collect_cartoes_data(start_date_str, end_date_str, database_path, output_csv_path, delimiter=";"):
    start_date = parse_date(start_date_str)
    end_date = parse_date(end_date_str)
    delta = timedelta(days=1)

    reports_data = {
        "elo credito": None,
        "elo debito": None,
        "visa credito": None,
        "visa debito": None,
        "master credito": None,
        "master debito": None,
        "pre pago visa credito": None,
        "pre pago visa debito": None,
        "pre pago mastercad credito": None,
        "pre pago mastercard debito": None,
        "pre pago elo credito": None,
        "pre pago elo debito": None,
        "hiper": None,
        "cielo amex": None
    }
    date_labels = []

    # Generate date labels for the entire range
    current_date = start_date
    while current_date <= end_date:
        formatted_date = current_date.strftime("%d/%m/%Y")
        date_labels.append(formatted_date)
        current_date += delta

    # Get list of relevant database files
    database_files = get_relevant_database_files(start_date, end_date, database_path)

    # Process each database file
    for database_file in database_files:
        with open(database_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            reports = data.get("reports", {})

            # Process each day in the reports
            for day_str, report_data in reports.items():
                # Convert day to date for checking if it's in range
                try:
                    file_date = datetime.strptime(f"{data['year']}-{data['month']}-{day_str}", "%Y-%m-%d")
                except ValueError:
                    continue  # Skip invalid dates
                formatted_date = file_date.strftime("%d/%m/%Y")
                
                if file_date < start_date or file_date > end_date or formatted_date not in date_labels:
                    continue

                for _, report in report_data.items():
                    finalizadoras = dict(
                        [(remove_accents(x).lower(), y) for x, y in report.get("finalizadoras", [])]
                    )
                    for cartao_key in reports_data:
                        if reports_data[cartao_key] is None:
                            reports_data[cartao_key] = {date: [] for date in date_labels}
                        value = 0.0
                        if finalizadoras.get(cartao_key):
                            value = finalizadoras[cartao_key]
                        reports_data[cartao_key][formatted_date].append(value)

    # Write the transposed CSV
    with open(output_csv_path, "w", newline="", encoding="latin-1") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        header = ["date"] + date_labels
        writer.writerow(header)
        writer.writerow([])

        reports_data_expanded = expand_dict(reports_data)

        for data_expanded in reports_data_expanded:
            for cartao_key in data_expanded.keys():
                row = [cartao_key] + ([
                    int_to_strfloat(data_expanded[cartao_key].get(date, "")) for date in date_labels
                ] if data_expanded[cartao_key] else ["0" for _ in date_labels])
                writer.writerow(row)
            writer.writerow([])

# === User interaction ===
if __name__ == "__main__":
    start_date_input = input("Enter the start date (YYYY-MM-DD): ")
    end_date_input = input("Enter the end date (YYYY-MM-DD): ")
    database_folder = "database"
    output_file = "test.csv"

    collect_cartoes_data(
        start_date_str=start_date_input,
        end_date_str=end_date_input,
        database_path=database_folder,
        output_csv_path=output_file,
        delimiter=";"
    )

    print(f"\nCSV file '{output_file}' created successfully.")