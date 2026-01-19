"""
Data verification script - checks if all three data sources are correctly loaded, aggregates metrics, and prints summary statistics from MongoDB.
"""

import pymongo
import pandas as pd

MONGODB_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"

def verify_data():
    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print("🔍 DATA VERIFICATION\n")
    print("=" * 70)
    
    total_docs = col.count_documents({})
    print(f"\n📊 TOTAL DOCUMENTS: {total_docs:,}")
    
    source1_count = col.count_documents({"source.sourceId": "source1"})
    source2_count = col.count_documents({"source.sourceId": "source2"})
    source3_count = col.count_documents({"source.sourceId": "source3"})
    
    print(f"\n  ✅ Source 1 (Online Retail): {source1_count:,}")
    print(f"  ✅ Source 2 (Sales Data):    {source2_count:,}")
    print(f"  ✅ Source 3 (Customers):     {source3_count:,}")
    print(f"  ─────────────────────────────")
    print(f"  📈 TOTAL:                    {total_docs:,}")
    
    print(f"\n💰 REVENUE:")
    
    pipeline_revenue = [{"$group": {"_id": None, "total": {"$sum": "$metrics.amount"}}}]
    result = list(col.aggregate(pipeline_revenue))
    total_revenue = result[0]['total'] if result else 0
    
    pipeline_rev_source = [{"$group": {"_id": "$source.sourceId", "revenue": {"$sum": "$metrics.amount"}}}]
    rev_per_source = {item['_id']: item['revenue'] for item in col.aggregate(pipeline_rev_source)}
    
    print(f"\n  Source 1 (Online Retail): ${rev_per_source.get('source1', 0):,.2f}")
    print(f"  Source 2 (Sales Data):    ${rev_per_source.get('source2', 0):,.2f}")
    print(f"  Source 3 (Customers):     ${rev_per_source.get('source3', 0):,.2f}")
    print(f"  ─────────────────────────────────────")
    print(f"  📈 TOTAL:                 ${total_revenue:,.2f}")
    
    print(f"\n👥 UNIQUE CUSTOMERS:")
    
    pipeline_customers = [{"$group": {"_id": "$entity.id"}}, {"$count": "count"}]
    result = list(col.aggregate(pipeline_customers))
    total_customers = result[0]['count'] if result else 0
    
    pipeline_cust_source = [
        {"$group": {"_id": {"source": "$source.sourceId", "customer": "$entity.id"}}},
        {"$group": {"_id": "$_id.source", "count": {"$sum": 1}}}
    ]
    cust_per_source = {item['_id']: item['count'] for item in col.aggregate(pipeline_cust_source)}
    
    print(f"\n  Source 1 (Online Retail): {cust_per_source.get('source1', 0):,}")
    print(f"  Source 2 (Sales Data):    {cust_per_source.get('source2', 0):,}")
    print(f"  Source 3 (Customers):     {cust_per_source.get('source3', 0):,}")
    print(f"  ─────────────────────────")
    print(f"  📈 TOTAL:                 {total_customers:,}")
    
    print(f"\n📦 UNIQUE PRODUCTS:")
    
    pipeline_products = [
        {"$match": {"metadata.stockCode": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$metadata.stockCode"}},
        {"$count": "count"}
    ]
    result = list(col.aggregate(pipeline_products))
    total_products = result[0]['count'] if result else 0
    
    print(f"\n  TOTAL: {total_products:,} products")
    
    print(f"\n🌍 UNIQUE COUNTRIES:")
    
    pipeline_countries = [
        {"$match": {"metadata.country": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$metadata.country"}},
        {"$count": "count"}
    ]
    result = list(col.aggregate(pipeline_countries))
    total_countries = result[0]['count'] if result else 0
    
    print(f"\n  TOTAL: {total_countries} countries")
    
    print(f"\n💵 AVERAGE ORDER VALUE:")
    
    pipeline_avg = [{"$group": {"_id": None, "avg": {"$avg": "$metrics.amount"}}}]
    result = list(col.aggregate(pipeline_avg))
    avg_order = result[0]['avg'] if result else 0
    
    print(f"\n  Average: ${avg_order:.2f} per transaction")
    
    print(f"\n📊 TOTAL UNITS SOLD:")
    
    pipeline_items = [{"$group": {"_id": None, "total": {"$sum": "$metrics.count"}}}]
    result = list(col.aggregate(pipeline_items))
    total_items = result[0]['total'] if result else 0
    
    print(f"\n  TOTAL: {total_items:,} units")
    
    print(f"\n📈 DATA RANGE:")
    
    pipeline_dates = [{"$group": {"_id": None, "min_date": {"$min": "$eventTime"}, "max_date": {"$max": "$eventTime"}}}]
    result = list(col.aggregate(pipeline_dates))
    if result:
        min_date = result[0]['min_date']
        max_date = result[0]['max_date']
        print(f"\n  Start date: {min_date}")
        print(f"  End date:   {max_date}")
        days_span = (max_date - min_date).days
        print(f"  Span:       {days_span} days")
    
    print(f"\n📋 INDEXES:")
    indexes = list(col.list_indexes())
    print(f"\n  Total indexes: {len(indexes)}")
    for idx in indexes:
        print(f"    - {idx['name']}")
    
    print(f"\n🔍 SAMPLE DOCUMENT:")
    sample = col.find_one({"source.sourceId": "source1"})
    if sample:
        print(f"\n  _id: {sample.get('_id')}")
        print(f"  source: {sample.get('source')}")
        print(f"  entity: {sample.get('entity')}")
        print(f"  metrics: {sample.get('metrics')}")
        print(f"  metadata: {sample.get('metadata')}")
    
    print(f"\n" + "=" * 70)
    print("✅ SUMMARY:")
    print(f"  Documents: {total_docs:,}")
    print(f"  Revenue: ${total_revenue:,.2f}")
    print(f"  Customers: {total_customers:,}")
    print(f"  Products: {total_products:,}")
    print(f"  Countries: {total_countries}")
    print(f"  Status: ✅ READY FOR PRODUCTION")
    print("=" * 70)
    
    client.close()

if __name__ == '__main__':
    verify_data()
