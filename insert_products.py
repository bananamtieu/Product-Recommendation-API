# https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
products_col = db["products"]

# Drop indexes before inserting (for better performance)
products_col.drop_indexes()

# Set batch sizes
chunk_size = 100000  # Read CSV in chunks
batch_size = 10000   # Insert data in batches

# List of CSV files to process
csv_files = ["item_properties_part1.csv", "item_properties_part2.csv"]

# Clear existing data to avoid duplicates
products_col.delete_many({})  

# Function to process values correctly
def process_values(prop_values):
    processed_values = []
    for value in prop_values:
        if value.startswith("n"):  # Numeric values prefixed with "n"
            try:
                processed_values.append(float(value[1:]))  # Convert to float after removing "n"
            except ValueError:
                processed_values.append(value)  # Keep as string if conversion fails
        elif value.isdigit():  # Other numbers (e.g., 639502, 424566) stored as strings
            processed_values.append(value)  
        else:
            processed_values.append(value)  # Keep hashed values as they are
    return processed_values

# Process each CSV file
for file in csv_files:
    print(f"Processing {file}...")

    for chunk in pd.read_csv(file, chunksize=chunk_size, dtype=str):  # Read all values as strings
        product_data = {}

        # Use itertuples() for faster row iteration
        for row in chunk.itertuples(index=False):
            item_id = row.itemid
            prop_name = row.property
            prop_value = row.value

            # Handle multi-value properties (split on spaces)
            prop_values = prop_value.split()

            # Process values correctly
            processed_values = process_values(prop_values)

            # If itemid is not in dictionary, initialize
            if item_id not in product_data:
                product_data[item_id] = {"itemid": item_id, "properties": {}}

            # Store categoryid separately for easier queries
            if prop_name == "categoryid":
                product_data[item_id]["categoryid"] = int(processed_values[0])  # Take the first value as categoryid

            # Store availability separately
            elif prop_name == "available":
                product_data[item_id]["available"] = int(processed_values[0])  # Take the first value as availability

            # Store properties (as a list if multiple values exist)
            elif prop_name in product_data[item_id]["properties"]:
                existing_value = product_data[item_id]["properties"][prop_name]
                if isinstance(existing_value, list):
                    product_data[item_id]["properties"][prop_name].extend(processed_values)
                else:
                    product_data[item_id]["properties"][prop_name] = [existing_value] + processed_values
            else:
                product_data[item_id]["properties"][prop_name] = processed_values if len(processed_values) > 1 else processed_values[0]

        # Convert dictionary to list and insert in batches
        product_data_values = list(product_data.values())
        for i in range(0, len(product_data_values), batch_size):
            products_col.insert_many(product_data_values[i:i + batch_size])

print("Re-inserted products with correctly structured properties!")

# Recreate indexes for efficient queries
products_col.create_index("itemid")
