from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["recommendation_system"]

# Define collections
users_col = db["users"]
products_col = db["products"]
categories_col = db["categories"]

print("Users count:", users_col.count_documents({}))
print("Products count:", products_col.count_documents({}))
print("Categories count:", categories_col.count_documents({}))

visitor_id = 15795  # Replace with an actual visitor ID from your data
user = users_col.find_one({"visitorid": visitor_id})

if user:
    print(f"User {visitor_id} interactions:")
    for interaction in user["interactions"]:
        print(interaction)
else:
    print("User not found.")

print("--------------------------------------------------")

item_id = 59481  # Replace with an actual item ID
product = products_col.find_one({"itemid": item_id})

if product:
    print(f"Product {item_id} details:")
    print(product)
else:
    print("Product not found.")

print("--------------------------------------------------")

category_id = 790  # Replace with an actual category ID
category = categories_col.find_one({"categoryid": category_id})

if category:
    print(f"Category {category_id} details:")
    print(category)
else:
    print("Category not found.")

print("--------------------------------------------------")
'''
category_id = 1338  # Replace with a real category ID
items_in_category = products_col.find({"categoryid": category_id})

print(f"Items in category {category_id}:")
for item in items_in_category:
    print(item)
'''