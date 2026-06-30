import json
import glob
import happybase

# Configurations
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
DATA_DIR   = '.'
BATCH_SIZE = 1000  # how many rows to send at once

# Connect to HBase 
print("Connecting to HBase")
connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
connection.open()
print("Connected!")

# Get our two tables
user_sessions_table   = connection.table('user_sessions')
product_metrics_table = connection.table('product_metrics')

# Load all session files
session_files = sorted(glob.glob(f"{DATA_DIR}/sessions_*.json"))
print(f"Found {len(session_files)} session files")

total_sessions = 0
total_product_views = {}  # track product views per day

for file in session_files:
    print(f"\nProcessing {file}...")
    
    with open(file, 'r') as f:
        sessions = json.load(f)

    # Batch insert into HBase
    with user_sessions_table.batch(batch_size=BATCH_SIZE) as batch:
        for session in sessions:
            row_key = f"{session['user_id']}#{session['start_time']}"

            session_info = {
                b'session_info:session_id':        session['session_id'].encode(),
                b'session_info:start_time':        session['start_time'].encode(),
                b'session_info:end_time':          session['end_time'].encode(),
                b'session_info:duration_seconds':  str(session['duration_seconds']).encode(),
                b'session_info:conversion_status': session['conversion_status'].encode(),
                b'session_info:referrer':          session['referrer'].encode(),
            }

            device = {
                b'device:type':    session['device_profile']['type'].encode(),
                b'device:os':      session['device_profile']['os'].encode(),
                b'device:browser': session['device_profile']['browser'].encode(),
            }

            geo = {
                b'geo:city':       session['geo_data']['city'].encode(),
                b'geo:state':      session['geo_data']['state'].encode(),
                b'geo:country':    session['geo_data']['country'].encode(),
                b'geo:ip_address': session['geo_data']['ip_address'].encode(),
            }

            activity = {
                b'activity:viewed_products': json.dumps(session['viewed_products']).encode(),
                b'activity:cart_contents':   json.dumps(session['cart_contents']).encode(),
                b'activity:page_view_count': str(len(session['page_views'])).encode(),
            }

            data = {**session_info, **device, **geo, **activity}

            batch.put(row_key.encode(), data)

            # Track product views for product_metrics table
            date = session['start_time'][:10]
            for product_id in session['viewed_products']:
                key = f"{product_id}_{date}"
                if key not in total_product_views:
                    total_product_views[key] = 0
                total_product_views[key] += 1

            total_sessions += 1

    print(f"  Loaded {len(sessions):,} sessions from {file}")

# Insert product metrics
print(f"\nInserting product metrics ({len(total_product_views):,} records)...")
with product_metrics_table.batch(batch_size=BATCH_SIZE) as batch:
    for key, view_count in total_product_views.items():
        # Row key: product_id + date
        batch.put(key.encode(), {
            b'stats:views': str(view_count).encode()
        })

print(f"HBase Load Complete")
connection.close()