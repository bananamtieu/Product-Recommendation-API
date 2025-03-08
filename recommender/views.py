from django.shortcuts import render
from django.http import JsonResponse
from pymongo import MongoClient
import pickle
import heapq

# Load precomputed similarity data
with open("top_n_item_similarity.pkl", "rb") as f:
    top_n_similar_items = pickle.load(f)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
users_col = db["users"]

def get_recommendations_api(request, visitor_id):
    """
    API Endpoint to get recommendations for a given user.
    URL: /api/recommendations/{visitor_id}/
    """
    # Retrieve user's past interactions from MongoDB
    user = users_col.find_one({"visitorid": int(visitor_id)})
    if not user or "interactions" not in user:
        return JsonResponse({"error": "User not found or no interactions."}, status=404)

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

    # Get top-10 recommendations
    top_recommendations = heapq.nlargest(10, recommendation_scores.items(), key=lambda x: x[1])

    # Format response
    response_data = {
        "visitor_id": visitor_id,
        "recommendations": [{"item_id": item, "score": round(score, 4)} for item, score in top_recommendations]
    }

    return JsonResponse(response_data)
