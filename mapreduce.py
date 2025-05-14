SELECT * FROM `hive_metastore`.`default`.`acomp_passenger_data_cleaned`;

# Load Hive table into DataFrame
df = spark.table("hive_metastore.default.acomp_passenger_data_cleaned")

# Save it as a CSV file with a header
df.coalesce(1).write.option("header", True).mode(“overwrite").csv("/dbfs/tmp/passenger_data")

import os

# List files in the folder and find the CSV file
folder_path = "/dbfs/tmp/passenger_data"
csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

# Construct file path
file_path = os.path.join(folder_path, csv_files[0])
print("Using file:", file_path)

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Thread-safe mapper for each chunk of lines
def thread_mapper(chunk):
    result = []
    for line in chunk:
        parts = line.strip().split(",")
        if parts[0] != "passenger_id":  # Ignores the header
            passenger_id = parts[0]
            result.append((passenger_id, 1))
    return result

# Shuffle & sort Phase
def shuffle_sort(mapped_data):
    grouped = defaultdict(list)
    for key, value in mapped_data:
        grouped[key].append(value)
    return grouped

# Reducer phase 
def reducer(grouped_data):
    reduced = {k: sum(v) for k, v in grouped_data.items()}
    max_flights = max(reduced.values())
    top_passengers = {k: v for k, v in reduced.items() if v == max_flights}
    return top_passengers

# Multithreaded driver function
def run_mapreduce(file_path, num_threads=4):
    with open(file_path, 'r') as f:
        lines = f.readlines()[1:]  # skip header

    # Split lines into chunks for each thread
    chunk_size = len(lines) // num_threads
    chunks = [lines[i:i+chunk_size] for i in range(0, len(lines), chunk_size)]

    # Multithreaded mapping
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        mapped_chunks = list(executor.map(thread_mapper, chunks))

    # Flatten all mapped chunks into one list
    mapped = [item for chunk in mapped_chunks for item in chunk]

    # Continue with Shuffle & Reduce
    grouped = shuffle_sort(mapped)
    result = reducer(grouped)

    # Print Result
    print("Passenger(s) with the most flights:")
    for pid, count in result.items():
        print(f"{pid}: {count}")

    return result

# Run the full map reduce pipeline
result = run_mapreduce(file_path, num_threads=4)

def save_output(result_dict, output_path="/dbfs/tmp/top_passengers.txt"):
    with open(output_path, "w") as f:
        for passenger_id, count in result_dict.items():
            f.write(f"{passenger_id}: {count}\n")
    print(f"Output saved to: {output_path}”)

save_output(result)

# View the contents of the output file
with open("/dbfs/tmp/top_passengers.txt", "r") as f:
    print(f.read())
