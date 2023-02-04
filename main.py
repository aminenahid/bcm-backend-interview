import json
import csv
import re
import sys

import requests
from datetime import datetime

sys.tracebacklimit = 0


def normalise_columns(data, column_names):
    # The aim is to have the same column names for all the fetched data
    normalised_data = []
    for record in data:
        start = record[column_names[0]]
        end = record[column_names[1]]
        power = record[column_names[2]]
        normalised_data.append({'start': start, 'end': end, 'power': power})
    return normalised_data


def retrieve_data_from_api(plant, column_names, from_date, to_date):
    endpoint = f"https://interview.beta.bcmenergy.fr/{plant}"
    params = {
        "from": from_date.strftime("%d-%m-%Y"),
        "to": to_date.strftime("%d-%m-%Y"),
    }

    # GET request to the API to retrieve the data
    response = requests.get(endpoint, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Check the content type of the response to determine if it's JSON or CSV
        # and return the data as a list of normalised dictionaries
        if re.search("application/json", response.headers["Content-Type"]):
            return normalise_columns(response.json(), column_names)
        elif re.search("text/csv|text/html", response.headers["Content-Type"]):
            data = []
            reader = csv.DictReader(response.text.strip().split("\n"))
            for row in reader:
                # Convert data from the CSV to integers as it will be needed for further computations
                row = {key: int(value) for key, value in row.items()}
                data.append(row)
            return normalise_columns(data, column_names)
    else:
        raise Exception("An internal server error occurred. Please try again.")


def aggregate_values(start_date, end_date):
    if start_date > end_date:
        raise Exception("Please make sure the start date is before the end date")

    # List of power plants and their column names
    power_plants = {"hawes": ["start", "end", "power"],
                    "barnsley": ["start_time", "end_time", "value"],
                    "hounslow": ["debut", "fin", "valeur"]}

    raw_data = {}
    for plant, columns in power_plants.items():
        raw_data[plant] = retrieve_data_from_api(plant, columns, start_date, end_date)

    # As the time interval is 15 minutes, same as Hawes plant data, let it be the basis for the aggregation
    aggregated_data = raw_data["hawes"]

    for row in aggregated_data:
        # We look in the data of the other plants for the matching time interval
        for plant in raw_data.keys():
            if plant == "hawes":
                continue
            plant_row = next((to_add for to_add in raw_data[plant]
                              if to_add['start'] <= row['start']
                              and to_add['end'] >= row['end']), None)
            if plant_row is not None:
                row['power'] += plant_row['power']
    return aggregated_data


def export_data(data, output_file_format):
    if output_file_format.lower() == "json":
        file = open("output.json", "w")
        file.write(json.dumps(data, indent=4))
        file.close()
        print("Aggregated values saved to output.json")

    elif output_file_format.lower() == "csv":
        with open("output.csv", 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['start', 'end', 'power'])
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print("Aggregated values saved to output.csv")

    else:
        raise Exception("The specified output format is not yet implemented.")


if __name__ == "__main__":
    start_date = input("Enter the start date (DD-MM-YYYY): ")
    end_date = input("Enter the end date (DD-MM-YYYY): ")
    output_format = input("Enter the output format (json/csv): ")

    try:
        start_date = datetime.strptime(start_date, "%d-%m-%Y")
        end_date = datetime.strptime(end_date, "%d-%m-%Y")
        result = aggregate_values(start_date, end_date)
        export_data(result, output_format)
    except Exception as e:
        print(e)
