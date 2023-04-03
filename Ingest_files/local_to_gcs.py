from web_to_local import run_tasks
from prefect import flow, task
import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket


# Convert the mega_file to df
@task(log_prints=True)
def read_csv(dataset_file: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_file)
    return df

@task(log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
     """Fix dtype issues"""
     df = df.loc[df['FIPS'] != 'FIPS']
     df = df.loc[df['Admin2'] != 'Admin2']
     df = df.loc[df['Province_State'] != 'Province_State']
     df = df.loc[df['Country_Region'] != 'Country_Region']
     df = df.loc[df['Last_Update'] != 'Last_Update']
     df = df.loc[df['Lat'] != 'Lat']
     df = df.loc[df['Long_'] != 'Long_']
     df = df.loc[df['Confirmed'] != 'Confirmed']
     df = df.loc[df['Deaths'] != 'Deaths']
     df = df.loc[df['Recovered'] != 'Recovered']
     df = df.loc[df['Active'] != 'Active']
     df = df.loc[df['Combined_Key'] != 'Combined_Key']
     df = df.loc[df['Incident_Rate'] != 'Incident_Rate']
     df = df.loc[df['Case_Fatality_Ratio'] != 'Case_Fatality_Ratio']
     df['Last_Update'] = pd.to_datetime(df['Last_Update'])
     df = df.astype({
         'FIPS': 'object',
         'Admin2': 'object',
         'Province_State': 'category',
         'Country_Region': 'object',
         'Lat': 'float',
         'Long_': 'float',
         'Confirmed': 'float',
         'Deaths': 'float',
         'Recovered': 'float',
         'Active': 'float',
         'Combined_Key': 'object'
          })
     return df

#Convert file to parquet
@task(log_prints=True)
def csv_to_parquet(df: pd.DataFrame) -> pd.DataFrame:
    df.to_parquet('Covid_file.parquet')
    return df
     
#Write file to gcs
@task()
def write_gcs(df: pd.DataFrame, mega_filepath) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("project-bucket")
    gcs_block.upload_from_path(from_path=mega_filepath(), to_path=mega_filepath())
    return


#Run the tasks
@flow(log_prints=True)
def local_to_gcs_tasks():
    
    mega_file = run_tasks()
    df = read_csv(mega_file())
    df_cleaned = clean_data(df)
    csv_to_parquet(df_cleaned)
    write_gcs(csv_to_parquet, run_tasks)

# Run the flow
if __name__ == "__main__":
    local_to_gcs_tasks()