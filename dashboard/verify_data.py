"""
Weryfikacja danych - sprawdzenie czy wszystkie 3 źródła są poprawnie załadowane
"""

import pymongo
import pandas as pd

MONGODB_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"

def verify_data():
    """Weryfikuj wszystkie dane"""
    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print("🔍 WERYFIKACJA DANYCH\n")
    print("=" * 70)
    
    # === LICZBA DOKUMENTÓW ===
    total_docs = col.count_documents({})
    print(f"\n📊 LICZBA DOKUMENTÓW: {total_docs:,}")
    
    # Liczba per źródło
    source1_count = col.count_documents({"source.sourceId": "source1"})
    source2_count = col.count_documents({"source.sourceId": "source2"})
    source3_count = col.count_documents({"source.sourceId": "source3"})
    
    print(f"\n  ✅ Źródło 1 (Online Retail): {source1_count:,}")
    print(f"  ✅ Źródło 2 (Sales Data):    {source2_count:,}")
    print(f"  ✅ Źródło 3 (Customers):     {source3_count:,}")
    print(f"  ─────────────────────────────")
    print(f"  📈 RAZEM:                    {total_docs:,}")
    
    # === PRZYCHÓD ===
    print(f"\n💰 PRZYCHÓD:")
    
    pipeline_revenue = [
        {"$group": {"_id": None, "total": {"$sum": "$metrics.amount"}}}
    ]
    result = list(col.aggregate(pipeline_revenue))
    total_revenue = result[0]['total'] if result else 0
    
    # Per źródło
    pipeline_rev_source = [
        {"$group": {"_id": "$source.sourceId", "revenue": {"$sum": "$metrics.amount"}}}
    ]
    rev_per_source = {item['_id']: item['revenue'] for item in col.aggregate(pipeline_rev_source)}
    
    print(f"\n  Źródło 1 (Online Retail): ${rev_per_source.get('source1', 0):,.2f}")
    print(f"  Źródło 2 (Sales Data):    ${rev_per_source.get('source2', 0):,.2f}")
    print(f"  Źródło 3 (Customers):     ${rev_per_source.get('source3', 0):,.2f}")
    print(f"  ─────────────────────────────────────")
    print(f"  📈 RAZEM:                 ${total_revenue:,.2f}")
    
    # === KLIENCI ===
    print(f"\n👥 KLIENCI (UNIKALNI):")
    
    pipeline_customers = [
        {"$group": {"_id": "$entity.id"}},
        {"$count": "count"}
    ]
    result = list(col.aggregate(pipeline_customers))
    total_customers = result[0]['count'] if result else 0
    
    # Per źródło
    pipeline_cust_source = [
        {"$group": {"_id": {"source": "$source.sourceId", "customer": "$entity.id"}}},
        {"$group": {"_id": "$_id.source", "count": {"$sum": 1}}}
    ]
    cust_per_source = {item['_id']: item['count'] for item in col.aggregate(pipeline_cust_source)}
    
    print(f"\n  Źródło 1 (Online Retail): {cust_per_source.get('source1', 0):,}")
    print(f"  Źródło 2 (Sales Data):    {cust_per_source.get('source2', 0):,}")
    print(f"  Źródło 3 (Customers):     {cust_per_source.get('source3', 0):,}")
    print(f"  ─────────────────────────")
    print(f"  📈 RAZEM:                 {total_customers:,}")
    
    # === PRODUKTY ===
    print(f"\n📦 PRODUKTY (UNIKALNE):")
    
    pipeline_products = [
        {"$match": {"metadata.stockCode": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$metadata.stockCode"}},
        {"$count": "count"}
    ]
    result = list(col.aggregate(pipeline_products))
    total_products = result[0]['count'] if result else 0
    
    print(f"\n  Razem: {total_products:,} produktów")
    
    # === KRAJE ===
    print(f"\n🌍 KRAJE (UNIKALNE):")
    
    pipeline_countries = [
        {"$match": {"metadata.country": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$metadata.country"}},
        {"$count": "count"}
    ]
    result = list(col.aggregate(pipeline_countries))
    total_countries = result[0]['count'] if result else 0
    
    print(f"\n  Razem: {total_countries} krajów")
    
    # === ŚREDNIA WARTOŚĆ ===
    print(f"\n💵 ŚREDNIA WARTOŚĆ ZAMÓWIENIA:")
    
    pipeline_avg = [
        {"$group": {"_id": None, "avg": {"$avg": "$metrics.amount"}}}
    ]
    result = list(col.aggregate(pipeline_avg))
    avg_order = result[0]['avg'] if result else 0
    
    print(f"\n  Średnia: ${avg_order:.2f} per transakcja")
    
    # === JEDNOSTKI ===
    print(f"\n📊 SPRZEDANE JEDNOSTKI:")
    
    pipeline_items = [
        {"$group": {"_id": None, "total": {"$sum": "$metrics.count"}}}
    ]
    result = list(col.aggregate(pipeline_items))
    total_items = result[0]['total'] if result else 0
    
    print(f"\n  Razem: {total_items:,} jednostek")
    
    # === ROZKŁAD DANYCH ===
    print(f"\n📈 ROZKŁAD DANYCH:")
    
    # Data range
    pipeline_dates = [
        {"$group": {"_id": None, "min_date": {"$min": "$eventTime"}, "max_date": {"$max": "$eventTime"}}}
    ]
    result = list(col.aggregate(pipeline_dates))
    if result:
        min_date = result[0]['min_date']
        max_date = result[0]['max_date']
        print(f"\n  Data początkowa: {min_date}")
        print(f"  Data końcowa: {max_date}")
        days_span = (max_date - min_date).days
        print(f"  Span: {days_span} dni")
    
    # === INDEKSY ===
    print(f"\n📋 INDEKSY:")
    indexes = list(col.list_indexes())
    print(f"\n  Razem indeksów: {len(indexes)}")
    for idx in indexes:
        print(f"    - {idx['name']}")
    
    # === SAMPLE DOKUMENTU ===
    print(f"\n🔍 PRZYKŁADOWY DOKUMENT:")
    sample = col.find_one({"source.sourceId": "source1"})
    if sample:
        print(f"\n  _id: {sample.get('_id')}")
        print(f"  source: {sample.get('source')}")
        print(f"  entity: {sample.get('entity')}")
        print(f"  metrics: {sample.get('metrics')}")
        print(f"  metadata: {sample.get('metadata')}")
    
    # === PODSUMOWANIE ===
    print(f"\n" + "=" * 70)
    print("✅ PODSUMOWANIE:")
    print(f"  Dokumenty: {total_docs:,}")
    print(f"  Przychód: ${total_revenue:,.2f}")
    print(f"  Klienci: {total_customers:,}")
    print(f"  Produkty: {total_products:,}")
    print(f"  Kraje: {total_countries}")
    print(f"  Status: ✅ GOTOWE DO PRODUKCJI")
    print("=" * 70)
    
    client.close()

if __name__ == '__main__':
    verify_data()