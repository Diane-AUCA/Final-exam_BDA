import happybase
from pymongo import MongoClient
from collections import defaultdict

# Load from MongoDB
print("\n Loading from MongoDB")
db = MongoClient("mongodb://admin:password123@localhost:27017/")["ecommerce_analytics"]

# User profiles from MongoDB
users = {u["_id"]: u for u in db.users.find()}
print(f"  Users: {len(users):,}")

# Transaction history from MongoDB
txn_stats = defaultdict(lambda: {"total_spent": 0.0, "total_orders": 0, "avg_order": 0.0})
for t in db.transactions.find({"status": "completed"}, {"user_id": 1, "total": 1}):
    s = txn_stats[t["user_id"]]
    s["total_spent"]  += t["total"]
    s["total_orders"] += 1

print(f"  Transaction stats computed for: {len(txn_stats):,} users")

#Load engagement metrics from HBase
print("\n Loading from HBase")
conn = happybase.Connection('localhost', port=9090)
conn.open()

session_stats = defaultdict(lambda: {"sessions": 0, "total_duration": 0, "conversions": 0})
for key, data in conn.table('user_sessions').scan(limit=50000):
    try:
        user_id = key.decode().split("#")[0]
        session_stats[user_id]["sessions"]       += 1
        session_stats[user_id]["total_duration"] += int(data[b'session_info:duration_seconds'].decode())
        if data[b'session_info:conversion_status'].decode() == "converted":
            session_stats[user_id]["conversions"] += 1
    except:
        pass

conn.close()
print(f"  Session stats computed for: {len(session_stats):,} users")

# CLV Analysis
print("\n CLV Analysis")
print("  Data Sources:")
print("  - User profiles    → MongoDB")
print("  - Transaction history → MongoDB")
print("  - Session engagement  → HBase")

results = []
for user_id, s in session_stats.items():
    user = users.get(user_id)
    txn  = txn_stats.get(user_id)
    if not user or not txn:
        continue

    sessions     = s["sessions"]
    avg_duration = s["total_duration"] / sessions / 60 if sessions > 0 else 0
    conversions  = s["conversions"]
    total_spent  = txn["total_spent"]
    total_orders = txn["total_orders"]
    avg_order    = total_spent / total_orders if total_orders > 0 else 0

    clv_tier = (
        "Platinum" if total_spent >= 50000 else
        "Gold"     if total_spent >= 30000 else
        "Silver"   if total_spent >= 10000 else
        "Bronze"
    )

    results.append({
        "user_id":      user_id,
        "country":      user["geo_data"]["country"],
        "total_spent":  round(total_spent, 2),
        "total_orders": total_orders,
        "avg_order":    round(avg_order, 2),
        "sessions":     sessions,
        "avg_mins":     round(avg_duration, 2),
        "conversions":  conversions,
        "clv_tier":     clv_tier
    })

# Sort by total spent
results.sort(key=lambda x: x["total_spent"], reverse=True)

# Print top 20
print(f"\nTop 20 Customers by CLV:")
print(f"{'User':<15} {'Country':<8} {'Spent':>12} {'Orders':>8} {'Avg Order':>10} {'Sessions':>10} {'Avg Mins':>10} {'Tier'}")
print("-" * 90)
for r in results[:20]:
    print(f"{r['user_id']:<15} {r['country']:<8} ${r['total_spent']:>11,.2f} {r['total_orders']:>8} {r['avg_order']:>9,.2f} {r['sessions']:>10} {r['avg_mins']:>10.2f} {r['clv_tier']}")

# CLV Tier Summary
print("\n CLV Tier Summary")
tier_stats = defaultdict(lambda: {"count": 0, "total_spent": 0, "total_sessions": 0, "total_orders": 0})
for r in results:
    t = tier_stats[r["clv_tier"]]
    t["count"]          += 1
    t["total_spent"]    += r["total_spent"]
    t["total_sessions"] += r["sessions"]
    t["total_orders"]   += r["total_orders"]

print(f"\n{'Tier':<10} {'Users':>8} {'Avg Spent':>12} {'Avg Orders':>12} {'Avg Sessions':>14}")
print("-" * 60)
for tier in ["Platinum", "Gold", "Silver", "Bronze"]:
    t = tier_stats[tier]
    if t["count"] > 0:
        print(f"{tier:<10} {t['count']:>8} ${t['total_spent']/t['count']:>11,.2f} {t['total_orders']/t['count']:>12.1f} {t['total_sessions']/t['count']:>14.1f}")
