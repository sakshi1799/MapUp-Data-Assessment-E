import os
import json
import csv
import argparse

def process_json_file(json_file_path):
    # Read JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract relevant data
    unit = data.get('unit', '')
    trip_id = os.path.splitext(os.path.basename(json_file_path))[0]
    
    # Check if tolls exist
    if 'route' not in data or not data['route'].get('tolls'):
        return None
    
    tolls = data['route']['tolls']

    # Prepare rows for CSV
    rows = []
    for toll in tolls:
        row = {
            'unit': unit,
            'trip_id': trip_id,
            'toll_loc_id_start': toll['start']['id'],
            'toll_loc_id_end': toll['end']['id'],
            'toll_loc_name_start': toll['start']['name'],
            'toll_loc_name_end': toll['end']['name'],
            'toll_system_type': toll['type'],
            'entry_time': toll['start']['timestamp_formatted'],
            'exit_time': toll['end']['timestamp_formatted'],
            'tag_cost': toll['tagCost'] if 'tagCost' in toll else None,
            'cash_cost': toll['cashCost'] if 'cashCost' in toll else None,
            'license_plate_cost': toll['licensePlateCost'] if 'licensePlateCost' in toll else None,
        }
        rows.append(row)

    return rows

def process_json_folder(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(output_folder, 'transformed_data.csv')

    with open(output_file_path, 'w', newline='') as csvfile:
        fieldnames = [
            'unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end',
            'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
            'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write CSV header
        writer.writeheader()

        # Process each JSON file in the input folder
        for filename in os.listdir(input_folder):
            if filename.endswith('.json'):
                json_file_path = os.path.join(input_folder, filename)
                rows = process_json_file(json_file_path)

                # Write rows to CSV
                if rows:
                    writer.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description='Process toll information from JSON files and transform into CSV.')
    parser.add_argument('--to_process', type=str, help='Path to the JSON responses folder.')
    parser.add_argument('--output_dir', type=str, help='Path to the output folder.')

    args = parser.parse_args()

    # Get input from command-line arguments
    to_process_input = args.to_process
    output_dir_input = args.output_dir

    # Process JSON files
    process_json_folder(to_process_input, output_dir_input)

if __name__ == "__main__":
    main()