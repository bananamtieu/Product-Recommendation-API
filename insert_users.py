# https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
users_col = db["users"]

# Read CSV in chunks (if memory is an issue)
chunk_size = 100000  # Process 100,000 rows at a time
batch_size = 10000   # Insert 10,000 documents per batch

# Drop indexes temporarily (if any exist)
users_col.drop_indexes()

for chunk in pd.read_csv("events.csv", chunksize=chunk_size):
    user_data = {}

    # Use itertuples() for faster iteration
    for row in chunk.itertuples(index=False):
        user_id = row.visitorid
        interaction = {
            "itemid": row.itemid,
            "event": row.event,
            "timestamp": pd.to_datetime(row.timestamp, unit="ms")
        }

        if user_id not in user_data:
            user_data[user_id] = {"visitorid": user_id, "interactions": []}
        
        user_data[user_id]["interactions"].append(interaction)

    # Convert dictionary values to a list and insert in batches
    user_data_values = list(user_data.values())
    for i in range(0, len(user_data_values), batch_size):
        users_col.insert_many(user_data_values[i : i + batch_size])

print("Users data inserted successfully!")

# Recreate indexes for faster queries
users_col.create_index("visitorid")
