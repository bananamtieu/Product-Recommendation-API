# https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]
categories_col = db["categories"]

category_tree = pd.read_csv("category_tree.csv")

category_docs = category_tree.to_dict(orient="records")
categories_col.insert_many(category_docs)

print("Categories data inserted successfully!")
