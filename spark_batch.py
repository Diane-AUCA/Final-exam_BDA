from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, ArrayType, MapType
import os
import glob

# Set environment variables
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# Create hadoop bin directory if it doesn't exist
if not os.path.exists(r"C:\hadoop\bin"):
    os.makedirs(r"C:\hadoop\bin", exist_ok=True)

# Create a dummy winutils.exe if missing
winutils_path = r"C:\hadoop\bin\winutils.exe"
if not os.path.exists(winutils_path):
    with open(winutils_path, 'w') as f:
        f.write("")  # Create empty file to suppress warning

# Spark Session with maximum memory configuration
spark = SparkSession.builder \
    .appName("EcommerceAnalytics") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", "64m") \
    .config("spark.sql.files.openCostInBytes", "64m") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark started successfully!")

# Define a function to load JSON files with explicit schema

def load_json_with_schema(file_pattern, schema=None):
    #Load JSON files with schema to avoid inference
    files = glob.glob(file_pattern)
    if not files:
        print(f"Warning: No files found matching {file_pattern}")
        return spark.createDataFrame([], StructType([]))
    
    print(f"Found {len(files)} files matching {file_pattern}")
    
    if schema is None:
        schema = spark.read.option("multiLine", "false").json(files[0]).schema
    
    df = spark.read.schema(schema).option("multiLine", "false").json(files)
    return df

session_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("conversion_status", StringType(), True),
    StructField("device_profile", StructType([
        StructField("type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True)
    ]), True),
])

users_schema = None
products_schema = None
transactions_schema = None

# STEP 1: Load raw JSON files

print("\n Loading Data")

users = spark.read.option("multiLine", "false").json("users.json")
products = spark.read.option("multiLine", "false").json("products.json")
transactions = spark.read.option("multiLine", "false").json("transactions.json")
sessions = load_json_with_schema("sessions_*.json", session_schema)
if sessions.count() == 0:
    print("Trying alternative loading method for sessions...")
    # Try loading each file individually
    session_files = glob.glob("sessions_*.json")
    dfs = []
    for file in session_files[:10]:  # Limit to first 10 files to test
        try:
            df = spark.read.option("multiLine", "false").json(file)
            dfs.append(df)
        except Exception as e:
            print(f"Failed to load {file}: {e}")
    
    if dfs:
        sessions = dfs[0]
        for df in dfs[1:]:
            sessions = sessions.union(df)
    else:
        print("Could not load any session files!")
        sessions = spark.createDataFrame([], session_schema)

#Clean the Data
users_clean = users \
    .withColumn("registration_date", F.to_timestamp("registration_date")) \
    .withColumn("last_active", F.to_timestamp("last_active")) \
    .withColumn("country", F.col("geo_data.country")) \
    .withColumn("state", F.col("geo_data.state")) \
    .withColumn("city", F.col("geo_data.city")) \
    .drop("geo_data") \
    .dropDuplicates(["user_id"]) \
    .na.fill({"country": "Unknown", "state": "Unknown", "city": "Unknown"})

products_clean = products \
    .withColumn("creation_date", F.to_timestamp("creation_date")) \
    .withColumn("base_price", F.round(F.col("base_price"), 2)) \
    .withColumn("is_active", F.col("is_active").cast("boolean")) \
    .dropDuplicates(["product_id"]) \
    .na.fill({"current_stock": 0})

transactions_clean = transactions \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("session_id", F.coalesce(F.col("session_id"), F.lit("no_session"))) \
    .withColumn("total", F.round(F.col("total"), 2)) \
    .withColumn("discount", F.round(F.col("discount"), 2)) \
    .withColumn("status", F.lower(F.col("status"))) \
    .dropDuplicates(["transaction_id"]) \
    .filter(F.col("total") > 0)

if sessions.count() > 0:
    sessions_clean = sessions \
        .withColumn("start_time", F.to_timestamp("start_time")) \
        .withColumn("end_time", F.to_timestamp("end_time")) \
        .withColumn("conversion_status", F.lower(F.col("conversion_status"))) \
        .withColumn("device_type", F.col("device_profile.type")) \
        .withColumn("os", F.col("device_profile.os")) \
        .withColumn("browser", F.col("device_profile.browser")) \
        .drop("device_profile") \
        .dropDuplicates(["session_id"])
else:
    sessions_clean = sessions

#Register DataFrames as SQL tables
users_clean.createOrReplaceTempView("users")
products_clean.createOrReplaceTempView("products")
transactions_clean.createOrReplaceTempView("transactions")
sessions_clean.createOrReplaceTempView("sessions")

#Spark SQL Analytics
print("\n--- Spark SQL Analytics ---")

# Total revenue by category
print("\nSQL Query 1: Revenue by Category")
try:
    revenue_by_category = spark.sql("""
        SELECT 
            p.category_id,
            COUNT(DISTINCT t.transaction_id) as total_transactions,
            SUM(i.subtotal)                  as total_revenue,
            AVG(i.unit_price)                as avg_price
        FROM transactions t
        LATERAL VIEW explode(t.items) tmp AS i
        JOIN products p ON i.product_id = p.product_id
        WHERE t.status = 'completed'
        GROUP BY p.category_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    revenue_by_category.show(truncate=False)
except Exception as e:
    print(f"Query 1 failed: {e}")

# Device usage patterns
print("\nSQL Query 2: Device Usage Patterns")
try:
    device_patterns = spark.sql("""
        SELECT 
            device_type,
            os,
            COUNT(*)                                    as session_count,
            ROUND(AVG(duration_seconds) / 60, 2)       as avg_duration_mins,
            SUM(CASE WHEN conversion_status = 'converted' 
                THEN 1 ELSE 0 END)                      as conversions,
            ROUND(SUM(CASE WHEN conversion_status = 'converted' 
                THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate_pct
        FROM sessions
        WHERE device_type IS NOT NULL
        GROUP BY device_type, os
        ORDER BY session_count DESC
        LIMIT 20
    """)
    device_patterns.show(truncate=False)
except Exception as e:
    print(f"Query 2 failed: {e}")

Product Recommendations
print("\n--- Product Recommendations: Users who bought X also bought Y ---")

try:
    items_df = transactions_clean \
        .filter(F.col("status") == "completed") \
        .select("transaction_id", "user_id", F.explode("items").alias("item")) \
        .select("transaction_id", "user_id", F.col("item.product_id").alias("product_id"))

    # Self join to find products bought together
    co_purchases = items_df.alias("a") \
        .join(items_df.alias("b"), 
              (F.col("a.transaction_id") == F.col("b.transaction_id")) & 
              (F.col("a.product_id") < F.col("b.product_id"))) \
        .groupBy(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b")
        ) \
        .agg(F.count("*").alias("times_bought_together")) \
        .orderBy(F.col("times_bought_together").desc()) \
        .limit(10)

    print("Top 10 Product Pairs Bought Together:")
    co_purchases.show(truncate=False)
except Exception as e:
    print(f"Product recommendations failed: {e}")

# DONE
print("\n--- Spark Batch Processing Complete ---")
spark.stop()