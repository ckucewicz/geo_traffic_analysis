import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd
import time

# Load environment variables
load_dotenv()
app_token = os.getenv("sodapy_app_token")

# API configuration
base_url = 'https://data.cityofchicago.org/api/v3/views/85ca-t3if/query.json'
year_start = 2015
year_end = 2025
page_size = 1000  # Socrata default max per request
max_retries = 5   # Retry API calls if there is a timeout
sleep_between_retries = 5  # seconds

# Loop over years
for year in range(year_start, year_end + 1):
    print(f"Fetching year {year}...")
    offset = 0
    csv_file = f"crash_data_{year}.csv"

    while True:
        query = (
            f"SELECT * "
            f"WHERE crash_date >= '{year}-01-01T00:00:00' "
            f"AND crash_date < '{year+1}-01-01T00:00:00' "
            f"LIMIT {page_size} OFFSET {offset}"
        )

        # Retry logic
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    base_url,
                    headers={"X-App-Token": app_token, "Content-Type": "application/json"},
                    data=json.dumps({"query": query}),
                    timeout=60  # seconds
                )
                if response.status_code != 200:
                    print(f"Error {response.status_code} at offset {offset}")
                    data = []
                else:
                    data = response.json()
                break  # Exit retry loop if successful
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(f"Timeout/Error at offset {offset}, retrying in {sleep_between_retries}s...")
                time.sleep(sleep_between_retries)
        else:
            print(f"Failed after {max_retries} attempts, moving to next batch")
            break

        if not data:
            break  # No more rows to fetch

        # Convert to DataFrame and append to CSV
        df = pd.DataFrame(data)
        df.to_csv(csv_file, mode='a', index=False, header=(offset == 0))

        print(f"  Fetched {len(data)} rows at offset {offset}")
        offset += page_size

    print(f"Finished fetching year {year}\n")