import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('MONGODB_URI')
DB_NAME = os.getenv('DATABASE_NAME', 'analytics')
COLLECTION = os.getenv('COLLECTION_NAME', 'records')

def create_indexes():
    """Tworzenie indeksów dla 541k rekordów"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print('📊 Tworzenie indeksów...\n')
    
    indexes = [
        ([('source.externalId', 1)], 'idx_invoice_no', 'Numer faktury'),
        ([('entity.id', 1)], 'idx_customer_id', 'ID klienta'),
        ([('entity.id', 1), ('eventTime', -1)], 'idx_customer_date', 'Klient + Data'),
        ([('eventTime', -1)], 'idx_date_desc', 'Data (malejąco)'),
        ([('metadata.country', 1)], 'idx_country', 'Kraj'),
        ([('metadata.country', 1), ('eventTime', -1)], 'idx_country_date', 'Kraj + Data'),
        ([('metadata.stockCode', 1)], 'idx_stock_code', 'Kod produktu'),
        ([('metadata.description', 'text')], 'idx_description_text', 'Full-text search'),
    ]
    
    for keys, name, desc in indexes:
        try:
            col.create_index(keys, name=name, background=True)
            print(f'✅ {name} - {desc}')
        except Exception as e:
            print(f'⚠️  {name}: {e}')
    
    # Wyświetl wszystkie indeksy
    print('\n📋 Wszystkie indeksy:\n')
    for idx in col.list_indexes():
        print(f'  - {idx["name"]}')
    
    # Statystyka
    print('\n📈 Statystyka kolekcji:')
    stats = db.command('collStats', COLLECTION)
    print(f'  Documents: {stats["count"]:,}')
    print(f'  Size: {stats["size"] / (1024**2):.2f} MB')
    
    client.close()
    print('\n✅ Gotowe!')

if __name__ == '__main__':
    create_indexes()