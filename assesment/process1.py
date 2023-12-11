import os
import argparse
import pandas as pd

def extract_trips(df):
    # Sort DataFrame by 'unit' and 'timestamp'
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(['unit', 'timestamp'], inplace=True)

    trips = []

    for unit, unit_data in df.groupby('unit'):
        trip_number = 0
        current_trip = pd.DataFrame(columns=['latitude', 'longitude', 'timestamp', 'unit'])

        for index, row in unit_data.iterrows():
            if current_trip.empty or (row['timestamp'] - current_trip['timestamp'].iloc[-1]).total_seconds() > 7 * 3600:
                # Start a new trip
                if not current_trip.empty:
                    trips.append(current_trip.copy())  # Save the current trip if not empty
                trip_number += 1
                current_trip = pd.DataFrame(columns=['latitude', 'longitude', 'timestamp', 'unit'])

            # Add data to the current trip
            current_trip = pd.concat([current_trip, pd.DataFrame({'latitude': [row['latitude']],
                                                                  'longitude': [row['longitude']],
                                                                  'timestamp': [row['timestamp']],
                                                                  'unit': [row['unit']]})],
                                     ignore_index=True)

        if not current_trip.empty:
            trips.append(current_trip.copy())  # Save the last trip if not empty

    return trips

def export_to_csv(trips_list, output_directory):
    # Ensure output directory exists
    os.makedirs(output_directory, exist_ok=True)

    csv_paths = []

    for i, trip_df in enumerate(trips_list):
        # Apply data cleaning steps to each trip DataFrame
        trip_df['latitude'] = pd.to_numeric(trip_df['latitude'], errors='coerce')
        trip_df['longitude'] = pd.to_numeric(trip_df['longitude'], errors='coerce')
        trip_df['timestamp'] = pd.to_datetime(trip_df['timestamp'], errors='coerce')
        trip_df = trip_df.dropna()

        # Format the 'timestamp' column in RFC 3339 format
        trip_df['timestamp'] = trip_df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Get unit for naming convention
        unit = trip_df['unit'].iloc[0]

        csv_path = os.path.join(output_directory, f'{unit}_{i + 1}.csv')
        trip_df.to_csv(csv_path, index=False)
        csv_paths.append(csv_path)

    return csv_paths

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process GPS data and extract trips to CSV files.')
    parser.add_argument('--to_process', required=True, help='Path to the Parquet file to be processed.')
    parser.add_argument('--output_dir', required=True, help='The folder to store the resulting CSV files.')
    args = parser.parse_args()

    # Read the Parquet file
    df = pd.read_parquet(args.to_process)

    # Extract trips
    trips_list = extract_trips(df)

    # Export DataFrames to CSV files
    csv_paths = export_to_csv(trips_list, args.output_dir)

    # Print the list of CSV paths
    print('CSV files exported to:')
    for path in csv_paths:
        print(path)

if __name__ == "__main__":
    main()
