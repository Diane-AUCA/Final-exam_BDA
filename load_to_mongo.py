import json
import time
import random
from collections import defaultdict
from pymongo import MongoClient

# Connection Settings
MONGO_URI = "mongodb://admin:password123@localhost:27017/"
DB_NAME   = "ecommerce_analytics"
DATA_DIR  = "."
BATCH_SIZE = 5000

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db     = client[DB_NAME]

# Load a JSON file
def load_json(filename):
    with open(f"{DATA_DIR}/{filename}", "r") as f:
        return json.load(f)

# Insert data in small batches
def bulk_insert(collection, docs, label):
    for i in range(0, len(docs), BATCH_SIZE):
        collection.insert_many(docs[i:i + BATCH_SIZE], ordered=False)
    print(f"  {label}: DONE ({len(docs):,} records)")

start = time.time()

#Load all raw data from JSON files

print("\n Loading JSON files")
users        = load_json("users.json")
products     = load_json("products.json")
categories   = load_json("categories.json")
transactions = load_json("transactions.json")

#clearing all data 
for coll in ["users", "products", "transactions"]:
    db[coll].drop()

#Build category lookups
category_by_id    = {c["category_id"]: c for c in categories}
subcategory_by_id = {
    sub["subcategory_id"]: sub
    for c in categories
    for sub in c["subcategories"]
}
#Calculate how much each user has spent
summary = defaultdict(lambda: {
    "total_orders": 0,
    "total_spent": 0.0,
    "last_order_date": None
})
for t in transactions:
    s = summary[t["user_id"]]
    s["total_orders"] += 1
    s["total_spent"]  += t["total"]
    if s["last_order_date"] is None or t["timestamp"] > s["last_order_date"]:
        s["last_order_date"] = t["timestamp"]

#Insert USERS
user_docs = [{
    "_id":               u["user_id"],
    "geo_data":          u["geo_data"],
    "registration_date": u["registration_date"],
    "last_active":       u["last_active"],
    "purchase_summary":  {
        "total_orders":    summary[u["user_id"]]["total_orders"],
        "total_spent":     round(summary[u["user_id"]]["total_spent"], 2),
        "last_order_date": summary[u["user_id"]]["last_order_date"],
    }
} for u in users]
bulk_insert(db["users"], user_docs, "users")
# Insert PRODUCTS

product_docs = []
for p in products:
    cat = category_by_id.get(p["category_id"])
    
    # Fix: pick a random subcategory from the product's category
    if cat and cat["subcategories"]:
        sub = random.choice(cat["subcategories"])
    else:
        sub = None
    product_docs.append({
        "_id":           p["product_id"],
        "name":          p["name"],
        "category": {
            "category_id": cat["category_id"],
            "name":        cat["name"]
        } if cat else None,
        "subcategory": {
            "subcategory_id": sub["subcategory_id"],
            "name":           sub["name"],
            "profit_margin":  sub["profit_margin"]
        } if sub else None,
        "base_price":    p["base_price"],
        "current_stock": p["current_stock"],
        "is_active":     p["is_active"],
        "price_history": p["price_history"],
        "creation_date": p["creation_date"],
    })
bulk_insert(db["products"], product_docs, "products")

# Insert TRANSACTIONS
transaction_docs = [{
    "_id":            t["transaction_id"],
    "session_id":     t.get("session_id"),
    "user_id":        t["user_id"],
    "timestamp":      t["timestamp"],
    "items":          t["items"],
    "subtotal":       t["subtotal"],
    "discount":       t["discount"],
    "total":          t["total"],
    "payment_method": t["payment_method"],
    "status":         t["status"],
} for t in transactions]
bulk_insert(db["transactions"], transaction_docs, "transactions")

# Create indexes for fast queries
db["transactions"].create_index("user_id")
db["transactions"].create_index("timestamp")
db["transactions"].create_index("items.product_id")
db["products"].create_index("category.category_id")
db["products"].create_index("subcategory.subcategory_id")
db["users"].create_index("purchase_summary.total_spent")
print(" done")

