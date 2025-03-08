from scipy.sparse import load_npz
from sklearn.neighbors import NearestNeighbors
# from sklearn.metrics.pairwise import cosine_similarity
# import heapq
import pickle

# Load Sparse User-Item Matrix
with open("visitor_id_map.pkl", "rb") as f:
    visitor_id_map = pickle.load(f)
with open("item_id_map.pkl", "rb") as f:
    item_id_map = pickle.load(f)

# Load the sparse matrix
user_item_sparse = load_npz("user_item_sparse.npz")

# Reverse item_id_map for correct mapping (index → itemid)
item_id_reverse_map = {v: k for k, v in item_id_map.items()}  

num_items = len(item_id_map)  # 235,061 items

# Transpose user-item matrix to get item-user matrix
item_user_sparse = user_item_sparse.T  

# Use Nearest Neighbors to find top-N similar items
N_SIMILAR = 20  # Number of similar items to retrieve
nn_model = NearestNeighbors(n_neighbors=N_SIMILAR+1, metric="cosine", n_jobs=-1)
nn_model.fit(item_user_sparse)

# Dictionary to store Top-N similar items for each item
top_n_similar_items = {}

# Compute similarities using Nearest Neighbors
distances, indices = nn_model.kneighbors(item_user_sparse)

# Store top-N similar items for each item
for item_idx in range(num_items):
    item_id = item_id_reverse_map[item_idx]  # Convert index → original itemid
    similar_items = [
        (item_id_reverse_map[indices[item_idx][j]], 1 - distances[item_idx][j])  # Convert distance to similarity
        for j in range(1, N_SIMILAR+1)  # Exclude self-similarity
    ]
    top_n_similar_items[item_id] = similar_items

'''
# Compute similarity in batches
batch_size = 5000  

for start in range(0, num_items, batch_size):
    end = min(start + batch_size, num_items)
    
    # Compute cosine similarity for batch
    item_similarity_batch = cosine_similarity(item_user_sparse[start:end], item_user_sparse, dense_output=False)

    # Store only top-N similar items for each item
    for idx, row in enumerate(item_similarity_batch):
        item_idx = start + idx  # Actual index in the full matrix

        # Ensure index exists in reverse map
        if item_idx in item_id_reverse_map:
            similar_items = row.toarray().flatten()  # Convert sparse row to array
            top_n = heapq.nlargest(20, enumerate(similar_items), key=lambda x: x[1])  # Top 20 most similar items
            
            # **Map indices to item IDs & exclude self-similarity**
            top_n_similar_items[item_id_reverse_map[item_idx]] = [
                (item_id_reverse_map[j], sim) for j, sim in top_n if j != item_idx and j in item_id_reverse_map
            ]

    print(f"Processed items {start} to {end}")
'''

# Save Top-N Similar Items Dictionary
with open("top_n_item_similarity.pkl", "wb") as f:
    pickle.dump(top_n_similar_items, f)

print("Optimized Item-Item Similarity Computed and Saved using Nearest Neighbors!")
