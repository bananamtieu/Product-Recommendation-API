import pandas as pd
from pymongo import MongoClient
from scipy.sparse import csr_matrix, save_npz
import pickle

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
users_col = db["users"]

# Define event weights
event_weights = {"view": 1, "addtocart": 5, "transaction": 10}

# Load user interactions
cursor = users_col.find({}, {"visitorid": 1, "interactions": 1})  # Fetch user interactions
data = []

visitor_id_map = {}  # Map visitorid to index
item_id_map = {}  # Map itemid to index
visitor_index = 0
item_index = 0

for user in cursor:
    visitor_id = user["visitorid"]

    # Assign visitor_id a unique index
    if visitor_id not in visitor_id_map:
        visitor_id_map[visitor_id] = visitor_index
        visitor_index += 1

    for interaction in user["interactions"]:
        item_id = interaction["itemid"]
        event = interaction["event"]
        weight = event_weights.get(event, 0)

        # Assign item_id a unique index
        if item_id not in item_id_map:
            item_id_map[item_id] = item_index
            item_index += 1

        data.append([visitor_id_map[visitor_id], item_id_map[item_id], weight])

# Convert to DataFrame
df = pd.DataFrame(data, columns=["visitor_idx", "item_idx", "weight"])

# Aggregate interaction scores for each user-item pair
df = df.groupby(["visitor_idx", "item_idx"])["weight"].sum().reset_index()

# Convert to Sparse Matrix
num_users = len(visitor_id_map)
num_items = len(item_id_map)  # 235,061 items

user_item_sparse = csr_matrix(
    (df["weight"], (df["visitor_idx"], df["item_idx"])),
    shape=(num_users, num_items)
)

# Save mapping dictionaries
with open("visitor_id_map.pkl", "wb") as f:
    pickle.dump(visitor_id_map, f)
with open("item_id_map.pkl", "wb") as f:
    pickle.dump(item_id_map, f)

# Save the sparse matrix to a file
save_npz("user_item_sparse.npz", user_item_sparse)

# Display Sparse Matrix Info
print(f"Sparse Matrix Shape: {user_item_sparse.shape}")
print(f"Non-zero interactions: {user_item_sparse.nnz}")
