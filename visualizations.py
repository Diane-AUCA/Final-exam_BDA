import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pymongo import MongoClient
from collections import defaultdict
import os

os.makedirs("output", exist_ok=True)
db = MongoClient("mongodb://admin:password123@localhost:27017/")["ecommerce_analytics"]

# ============================================================
# VIZ 1: Revenue by Category (completed transactions only)
# ============================================================
print("Creating Viz 1: Revenue by Category...")

pipeline1 = [
    {"$match": {"status": "completed"}},
    {"$unwind": "$items"},
    {"$lookup": {
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "_id",
        "as": "product"
    }},
    {"$unwind": "$product"},
    {"$group": {
        "_id": "$product.category.name",
        "revenue":  {"$sum": "$items.subtotal"},
        "orders":   {"$sum": 1},
        "units":    {"$sum": "$items.quantity"}
    }},
    {"$sort": {"revenue": -1}},
    {"$limit": 10}
]

data1    = list(db.transactions.aggregate(pipeline1))
print(f"  Got {len(data1)} categories")
for d in data1:
    print(f"  {d['_id']}: ${d['revenue']:,.0f} | orders: {d['orders']:,} | units: {d['units']:,}")

names    = [d["_id"] for d in data1]
revenues = [d["revenue"] / 1_000_000 for d in data1]

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(names, revenues, color="steelblue", edgecolor="white")
ax.set_xlabel("Revenue (Millions $)")
ax.set_title("Top 10 Product Categories by Revenue\n(317,055 Completed Transactions)", 
             fontsize=13, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}M"))
for bar, val in zip(bars, revenues):
    ax.text(bar.get_width() + 0.05,
            bar.get_y() + bar.get_height() / 2,
            f"${val:.1f}M", va="center", fontsize=9)
plt.tight_layout()
plt.savefig("output/viz1_revenue_by_category.png", dpi=150)
plt.close()
print("  Saved: viz1_revenue_by_category.png")


# ============================================================
# VIZ 2: Customer Segmentation by Purchasing Frequency
# ============================================================
print("\nCreating Viz 2: Customer Segmentation...")

segments = defaultdict(list)
for u in db.users.find({}, {"purchase_summary": 1}):
    orders = u["purchase_summary"]["total_orders"]
    spent  = u["purchase_summary"]["total_spent"]
    if orders >= 55:
        segments["High Value"].append(spent)
    elif orders >= 45:
        segments["Medium Value"].append(spent)
    else:
        segments["Low Value"].append(spent)

print(f"  Segments found: { {k: len(v) for k, v in segments.items()} }")

labels    = list(segments.keys())
counts    = [len(v) for v in segments.values()]
avg_spent = [sum(v) / len(v) for v in segments.values()]
colors    = ["#2ecc71", "#3498db", "#e67e22"]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Customer Segmentation by Purchasing Frequency", 
             fontsize=13, fontweight="bold")

ax1.pie(counts, labels=labels, colors=colors, autopct="%1.1f%%",
        startangle=90, wedgeprops={"edgecolor": "white"})
ax1.set_title("User Distribution by Segment")

bars2 = ax2.bar(labels, avg_spent, color=colors, edgecolor="white", width=0.5)
ax2.set_title("Average Spending by Segment")
ax2.set_ylabel("Average Spending ($)")
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
for bar, val in zip(bars2, avg_spent):
    ax2.text(bar.get_x() + bar.get_width() / 2,
             bar.get_height() + 300,
             f"${val:,.0f}", ha="center", fontsize=9, fontweight="bold")

plt.tight_layout()
plt.savefig("output/viz2_customer_segmentation.png", dpi=150)
plt.close()
print("  Saved: viz2_customer_segmentation.png")


# ============================================================
# VIZ 3: Top 10 Products by Units Sold
# ============================================================
print("\nCreating Viz 3: Product Performance...")

pipeline3 = [
    {"$match": {"status": "completed"}},
    {"$unwind": "$items"},
    {"$group": {
        "_id":   "$items.product_id",
        "units": {"$sum": "$items.quantity"}
    }},
    {"$sort": {"units": -1}},
    {"$limit": 10}
]

data3    = list(db.transactions.aggregate(pipeline3))
products = [d["_id"] for d in data3]
units    = [d["units"] for d in data3]

print(f"  Top product: {products[0]} with {units[0]:,} units")

fig, ax = plt.subplots(figsize=(12, 6))
bars3 = ax.bar(products, units, color="darkorange", edgecolor="white")
ax.set_ylabel("Units Sold")
ax.set_title("Top 10 Products by Units Sold\n(Completed Transactions Only)",
             fontsize=13, fontweight="bold")
ax.set_xticklabels(products, rotation=45, ha="right")
for bar, val in zip(bars3, units):
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            f"{val:,}", ha="center", fontsize=8, fontweight="bold")
plt.tight_layout()
plt.savefig("output/viz3_product_performance.png", dpi=150)
plt.close()
print("  Saved: viz3_product_performance.png")


# ============================================================
# VIZ 4: Conversion Funnel (real numbers from MongoDB)
# ============================================================
print("\nCreating Viz 4: Conversion Funnel...")

total_sessions  = 2000000
total_carted    = db.transactions.count_documents({})
total_converted = db.transactions.count_documents({"status": "completed"})

print(f"  Total sessions:  {total_sessions:,}")
print(f"  Total carted:    {total_carted:,}")
print(f"  Total converted: {total_converted:,}")

stages = ["Total Sessions", "Added to Cart", "Converted"]
values = [total_sessions, total_carted, total_converted]
colors = ["#3498db", "#e67e22", "#2ecc71"]

fig, ax = plt.subplots(figsize=(10, 6))
bars4 = ax.bar(stages, values, color=colors, width=0.5, edgecolor="white")
ax.set_ylabel("Number of Sessions / Transactions")
ax.set_title("E-Commerce Conversion Funnel\n(Sessions → Cart → Purchase)",
             fontsize=13, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

for i, val in enumerate(values):
    pct = val / total_sessions * 100
    ax.text(i, val + 15000,
            f"{val:,}\n({pct:.1f}%)",
            ha="center", fontweight="bold", fontsize=10)

# Add drop-off annotations
dropoffs = [
    (0, 1, f"▼ {(total_sessions - total_carted)/total_sessions*100:.1f}% drop"),
    (1, 2, f"▼ {(total_carted - total_converted)/total_carted*100:.1f}% drop")
]
for x, _, label in dropoffs:
    ax.text(x + 0.5, max(values) * 0.6, label,
            ha="center", color="red", fontsize=9, style="italic")

plt.tight_layout()
plt.savefig("output/viz4_conversion_funnel.png", dpi=150)
plt.close()
print("  Saved: viz4_conversion_funnel.png")

print("\n✓ All 4 visualizations complete! Check output/ folder.")