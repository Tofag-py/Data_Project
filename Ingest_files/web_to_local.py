import os
import requests
from prefect import task, flow
import datetime
import pandas as pd

# Create the data directory if it does not exist
@task(log_prints=True)
def make_path(data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

# Download csv from web
@task(log_prints=True)
def get_csv(date, base_url):
    """Downloads the CSV file for the given date."""
    url = base_url + date.strftime("%m-%d-%Y") + ".csv"
    response = requests.get(url)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred while downloading data for {date}: {e}")
        return None
    return response.content

@task(log_prints=True)
def save_csv(data, date, data_dir):
    """Saves the CSV data to a file with the given date."""
    filename = os.path.join(data_dir, date.strftime("%m-%d-%Y") + ".csv")
    with open(filename, "wb") as f:
        f.write(data)

# Append the csv files to a mega file
@task(log_prints=True)
def append_csv_to_mega_file(csv_data, date, data_dir) -> str:
    """Appends the CSV data to a mega file and stores it locally."""
    mega_filename = "mega_csv_file.csv"
    mega_filepath = os.path.join(data_dir, mega_filename)
    csv_filename = date.strftime("%m-%d-%Y") + ".csv"
    csv_filepath = os.path.join(data_dir, csv_filename)

    # Append the CSV data to the mega file
    with open(mega_filepath, "a") as mega_file:
        mega_file.write(csv_data.decode("utf-8"))

    # Delete the CSV file after it has been appended to the mega file
    os.remove(csv_filepath)
    return mega_filepath

@task(log_prints=True)
def mega_file_path(mega_file):
    return mega_file


#Run the tasks
@flow(log_prints=True)
def run_tasks():
    # Define the dates for which to download the CSV files
    start_date = datetime.date(2020, 1, 22)
    end_date = datetime.date.today()
    dates = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    # Define the base URL for the CSV files
    base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"

    # Define the directory to store the downloaded CSV files
    data_dir = "Ingest_files/covid_data"

    # Define the Prefect flow
    make_path(data_dir)
    csv_data = get_csv.map(dates, base_url)
    csv_files = save_csv.map(csv_data, dates, data_dir)
    mega_file = append_csv_to_mega_file.map(csv_data, dates, data_dir)
    file_path = mega_file_path(mega_file)
    return file_path
   
       

# Run the flow
if __name__ == "__main__":
    run_tasks()

