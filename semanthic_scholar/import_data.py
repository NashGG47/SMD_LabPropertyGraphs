import requests
import json
import os
from datetime import datetime, timedelta
def fetch_dataset(base_url, api_key, headers, release_id, dataset_name):
    try:
        response = requests.get(base_url + release_id + '/dataset/' + dataset_name, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} for release_id {release_id}")
    except Exception as err:
        print(f"An error occurred: {err}")
    return None

base_url = "https://api.semanticscholar.org/datasets/v1/release/"
api_key = "GDhZUV3Hhb1M0ge7Zq6lr2DKKeYEEkjr62FklIGS"
headers = {"x-api-key": api_key}
dataset_name = 'papers'
output_dir = "data/semantic_scholar"
output_file = os.path.join(output_dir, "2024_datasets.json")
os.makedirs(output_dir, exist_ok=True)

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)
delta = timedelta(days=1)

all_data = {}

current_date = start_date
while current_date <= end_date:
    release_id = current_date.strftime('%Y-%m-%d')
    print(f"Fetching dataset for release: {release_id}")
    
    dataset = fetch_dataset(base_url, api_key, headers, release_id, dataset_name)
    if dataset:
        all_data[release_id] = dataset
        print(f"Data for {release_id} added.")
    else:
        print(f"No data found for release {release_id}.")

    current_date += delta
with open(output_file, "w") as file:
    json.dump(all_data, file, indent=4)

print(f"Data fetching complete. All datasets saved in '{output_file}'.")
