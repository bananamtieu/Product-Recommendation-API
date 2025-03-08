from pymongo import MongoClient
import heapq
import pickle

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
users_col = db["users"]

# Load Top-N Similar Items Dictionary
with open("top_n_item_similarity.pkl", "rb") as f:
    top_n_similar_items = pickle.load(f)

print(f"Loaded {len(top_n_similar_items)} items with precomputed similarities!")

def get_recommendations(visitor_id, K=10):
    """
    Generate top-K item recommendations for a given user.
    """
    # Retrieve user's past interactions from MongoDB
    user = users_col.find_one({"visitorid": visitor_id})
    if not user or "interactions" not in user:
        return []

    # Get items the user has interacted with
    interacted_items = {interaction["itemid"] for interaction in user["interactions"]}

    # Dictionary to store recommended item scores
    recommendation_scores = {}

    # Loop through items user interacted with
    for item in interacted_items:
        if item in top_n_similar_items:
            for similar_item, similarity in top_n_similar_items[item]:
                if similar_item not in interacted_items:  # Avoid recommending seen items
                    if similar_item in recommendation_scores:
                        recommendation_scores[similar_item] += similarity
                    else:
                        recommendation_scores[similar_item] = similarity

    # Get top-K recommendations
    top_recommendations = heapq.nlargest(K, recommendation_scores.items(), key=lambda x: x[1])

    return top_recommendations

# Example Usage
visitor_id = 1076270  # Replace with a real visitor ID
recommended_items = get_recommendations(visitor_id)

print(f"Recommended items for user {visitor_id}: {recommended_items}")