"""
ETL Pipeline - loads data from three CSV sources into MongoDB, transforms them, and prints progress and statistics.
"""

import pandas as pd
import pymongo
import os
import time
from datetime import datetime, timezone

MONGODB_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"

CSV_FILES = {
    'source1': {
        'path': 'data/online_retail.csv',
        'name': 'Online Retail',
        'description': 'Kaggle Online Retail Dataset'
    },
    'source2': {
        'path': 'data/sales_data.csv',
        'name': 'Sales Data',
        'description': 'Sales transactions data'
    },
    'source3': {
        'path': 'data/customers.csv',
        'name': 'Customers',
        'description': 'Customer information'
    }
}

BATCH_SIZE = 5000
CHUNK_SIZE = 50000

def load_csv_source1(client, csv_path):
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print(f'📂 Source 1: Online Retail\n')
    
    if not os.path.exists(csv_path):
        print(f'❌ File not found: {csv_path}')
        return 0
    
    total_inserted = 0
    start_time = time.time()
    chunks = pd.read_csv(csv_path, chunksize=CHUNK_SIZE)
    
    for chunk_num, df_chunk in enumerate(chunks):
        docs = []
        
        for _, row in df_chunk.iterrows():
            quantity = int(row['Quantity']) if pd.notna(row['Quantity']) else 0
            unit_price = float(row['UnitPrice']) if pd.notna(row['UnitPrice']) else 0.0
            
            doc = {
                'schemaVersion': 1,
                'source': {
                    'name': 'kaggle_csv',
                    'type': 'csv',
                    'sourceId': 'source1',
                    'externalId': str(row.get('InvoiceNo', ''))
                },
                'ingestedAt': datetime.now(timezone.utc),
                'eventTime': pd.to_datetime(row['InvoiceDate']),
                'entity': {
                    'id': str(row.get('CustomerID', 'UNKNOWN')),
                    'type': 'customer'
                },
                'metrics': {
                    'amount': unit_price * quantity,
                    'count': quantity,
                    'unitPrice': unit_price
                },
                'metadata': {
                    'description': row.get('Description', ''),
                    'stockCode': str(row.get('StockCode', '')),
                    'country': row.get('Country', '')
                }
            }
            docs.append(doc)
        
        if docs:
            try:
                result = col.insert_many(docs, ordered=False)
                total_inserted += len(result.inserted_ids)
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed if elapsed > 0 else 0
                print(f'  ✅ Chunk {chunk_num + 1}: {len(docs)} docs | '
                      f'Total: {total_inserted:,} | Rate: {rate:.0f} docs/sec')
            except Exception as e:
                print(f'  ❌ Error in chunk {chunk_num}: {e}')
    
    elapsed = time.time() - start_time
    print(f'✅ Source 1 completed: {total_inserted:,} records in {elapsed:.2f}s\n')
    return total_inserted


def load_csv_source2(client, csv_path):
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print(f'📂 Source 2: Sales Data\n')
    
    if not os.path.exists(csv_path):
        print(f'❌ File not found: {csv_path}')
        return 0
    
    total_inserted = 0
    start_time = time.time()
    
    try:
        df = pd.read_csv(csv_path)
        docs = []
        
        for idx, row in df.iterrows():
            amount = float(row.get('amount', 0)) if pd.notna(row.get('amount')) else 0.0
            
            doc = {
                'schemaVersion': 1,
                'source': {
                    'name': 'sales_csv',
                    'type': 'csv',
                    'sourceId': 'source2',
                    'externalId': str(row.get('sale_id', idx))
                },
                'ingestedAt': datetime.now(timezone.utc),
                'eventTime': pd.to_datetime(row.get('date', datetime.now())) if 'date' in row else datetime.now(timezone.utc),
                'entity': {
                    'id': str(row.get('customer_id', 'UNKNOWN')),
                    'type': 'customer'
                },
                'metrics': {
                    'amount': amount,
                    'count': 1
                },
                'metadata': {
                    'product': row.get('product', ''),
                    'category': row.get('category', ''),
                    'region': row.get('region', '')
                }
            }
            docs.append(doc)
        
        if docs:
            for i in range(0, len(docs), BATCH_SIZE):
                batch = docs[i:i+BATCH_SIZE]
                try:
                    result = col.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    print(f'  ✅ Batch {i//BATCH_SIZE + 1}: {len(batch)} docs | '
                          f'Total: {total_inserted:,} | Rate: {rate:.0f} docs/sec')
                except Exception as e:
                    print(f'  ❌ Error in batch: {e}')
    
    except Exception as e:
        print(f'❌ Error reading CSV: {e}')
    
    elapsed = time.time() - start_time
    print(f'✅ Source 2 completed: {total_inserted:,} records in {elapsed:.2f}s\n')
    return total_inserted


def load_csv_source3(client, csv_path):
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print(f'📂 Source 3: Customers\n')
    
    if not os.path.exists(csv_path):
        print(f'❌ File not found: {csv_path}')
        return 0
    
    total_inserted = 0
    start_time = time.time()
    
    try:
        df = pd.read_csv(csv_path)
        docs = []
        
        for idx, row in df.iterrows():
            doc = {
                'schemaVersion': 1,
                'source': {
                    'name': 'customers_csv',
                    'type': 'csv',
                    'sourceId': 'source3',
                    'externalId': str(row.get('customer_id', idx))
                },
                'ingestedAt': datetime.now(timezone.utc),
                'eventTime': datetime.now(timezone.utc),
                'entity': {
                    'id': str(row.get('customer_id', 'UNKNOWN')),
                    'type': 'customer'
                },
                'metrics': {
                    'totalPurchases': float(row.get('total_purchases', 0)) if pd.notna(row.get('total_purchases')) else 0.0,
                    'averageOrderValue': float(row.get('avg_order_value', 0)) if pd.notna(row.get('avg_order_value')) else 0.0
                },
                'metadata': {
                    'name': row.get('name', ''),
                    'email': row.get('email', ''),
                    'city': row.get('city', ''),
                    'country': row.get('country', ''),
                    'signup_date': row.get('signup_date', '')
                }
            }
            docs.append(doc)
        
        if docs:
            for i in range(0, len(docs), BATCH_SIZE):
                batch = docs[i:i+BATCH_SIZE]
                try:
                    result = col.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    elapsed = time.time() - start_time
                    rate = total_inserted / elapsed if elapsed > 0 else 0
                    print(f'  ✅ Batch {i//BATCH_SIZE + 1}: {len(batch)} docs | '
                          f'Total: {total_inserted:,} | Rate: {rate:.0f} docs/sec')
                except Exception as e:
                    print(f'  ❌ Error in batch: {e}')
    
    except Exception as e:
        print(f'❌ Error reading CSV: {e}')
    
    elapsed = time.time() - start_time
    print(f'✅ Source 3 completed: {total_inserted:,} records in {elapsed:.2f}s\n')
    return total_inserted


def run_etl():
    print('🚀 ETL Pipeline - 3 Data Sources\n')
    print('=' * 60)
    
    client = pymongo.MongoClient(MONGODB_URI)
    
    grand_total = 0
    grand_start = time.time()
    
    total1 = load_csv_source1(client, CSV_FILES['source1']['path'])
    grand_total += total1
    
    total2 = load_csv_source2(client, CSV_FILES['source2']['path'])
    grand_total += total2
    
    total3 = load_csv_source3(client, CSV_FILES['source3']['path'])
    grand_total += total3
    
    grand_elapsed = time.time() - grand_start
    print('=' * 60)
    print(f'\n📊 ETL SUMMARY:')
    print(f'  Source 1 (Online Retail): {total1:,} documents')
    print(f'  Source 2 (Sales Data):    {total2:,} documents')
    print(f'  Source 3 (Customers):     {total3:,} documents')
    print(f'  ─────────────────────────────────')
    print(f'  TOTAL:                     {grand_total:,} documents')
    print(f'  Time: {grand_elapsed:.2f}s')
    print(f'  Speed: {grand_total/grand_elapsed:.0f} docs/sec')
    
    db = client[DB_NAME]
    col = db[COLLECTION]
    stats = db.command('collStats', COLLECTION)
    print(f'\n📈 Collection Stats:')
    print(f'  Documents: {stats["count"]:,}')
    print(f'  Size: {stats["size"] / (1024**2):.2f} MB')
    
    client.close()
    print(f'\n✅ ETL Completed!')


if __name__ == '__main__':
    run_etl()
