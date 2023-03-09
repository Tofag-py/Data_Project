from pathlib import Path
import pandas as pd
from prefect import flow, task
import requests
from chardet import detect
import os


os.mkdir("files")

# Define a task that gets the encoding of the data in the URL
@task(log_prints=True)
def get_encoding(url: str):
    # download the file
    response = requests.get(url)
    # detect the encoding
    result = detect(response.content)
    return(result['encoding'])

# Define a task that reads data from a given URL and returns a Pandas DataFrame
@task(log_prints=True)
def fetch_data(url: str, encoding: str) -> pd.DataFrame:
    df = pd.read_csv(url, encoding=encoding)
    return df

@task()
def write_local(df: pd.DataFrame, q: str) -> Path:
    # Define a task that writes a DataFrame to a local Parquet file
    path = Path(f"files\{q}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

# Define a Prefect flow that uses the fetch_data task to read data from a URL and store it in GCS
@flow()
def etl_to_gcs():
    #define variables
    quarters = ["q1", "q2", "q3", "q4"]
    for q in quarters:
        data_url = f"https://fsa-catalogue2.s3.eu-west-2.amazonaws.com/erg-spend-+2122-{q}.csv"
        
        
        #Call the tasks
        file_encoding = get_encoding(data_url)
        df = fetch_data(data_url, file_encoding)
        path = write_local(df, q)


# If the script is being run as the main program, run the etl_to_gcs flow
if __name__ == "__main__":
    etl_to_gcs()
