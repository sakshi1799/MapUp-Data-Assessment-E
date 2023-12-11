import os
import requests
from dotenv import load_dotenv
import json
from concurrent.futures import ThreadPoolExecutor
import argparse

def process_gps_tracks_csv_upload(api_key, api_url, vehicle_type, map_provider, csv_file_path):
    url = f'{api_url}/gps-tracks-csv-upload?mapProvider={map_provider}&vehicleType={vehicle_type}'
    headers = {'x-api-key': api_key, 'Content-Type': 'text/csv'}

    with open(csv_file_path, 'rb') as file:
        response = requests.post(url, data=file, headers=headers)

    return response.json()

def process_gps_tracks_csv_files(input_folder, output_folder, api_key, api_url, vehicle_type='5AxlesTruck', map_provider='osrm'):
    os.makedirs(output_folder, exist_ok=True)
    
    csv_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for csv_file in csv_files:
            csv_file_path = os.path.join(input_folder, csv_file)
            future = executor.submit(process_gps_tracks_csv_upload, api_key, api_url, vehicle_type, map_provider, csv_file_path)
            futures.append((csv_file, future))

        for csv_file, future in futures:
            result = future.result()
            output_file_path = os.path.join(output_folder, f'{os.path.splitext(csv_file)[0]}.json')
            with open(output_file_path, 'w') as json_file:
                json.dump(result, json_file, indent=2)  # Use json.dump to write JSON content with double quotes

def main():
    parser = argparse.ArgumentParser(description='Process GPS tracks CSV files using TollGuru API.')
    parser.add_argument('--to_process', type=str, help='Path to the CSV folder.')
    parser.add_argument('--output_dir', type=str, help='Path to the output folder.')

    args = parser.parse_args()

    input_folder = args.to_process
    output_folder = args.output_dir

    # Load API key and URL from .env file
    #load_dotenv()
    #api_key = os.getenv('TOLLGURU_API_KEY')
    #api_url = os.getenv('TOLLGURU_API_URL')
    api_key='QNmdQ6mj7RDtTpGhF6P2NfHq8NNh737m'
    api_url='https://apis.tollguru.com/toll/v2'


    # Process GPS tracks CSV files and send requests to TollGuru API
    process_gps_tracks_csv_files(input_folder, output_folder, api_key, api_url)

if __name__ == "__main__":
    main()

