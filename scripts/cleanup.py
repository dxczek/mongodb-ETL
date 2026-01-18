import pymongo

# === WSTAW SWOJE DANE TUTAJ ===
MONGO_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"

def cleanup_duplicates():
    """Usuń duplikaty - zachowaj tylko jedno copie dokumentu"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print('🧹 Czyszczenie duplikatów...\n')
    
    # Sprawdź ile dokumenów jest teraz
    total_before = col.count_documents({})
    print(f'Dokumenty przed: {total_before:,}')
    
    # Usuń całą kolekcję i załaduj jeszcze raz
    print('\n⚠️  Czy chcesz usunąć całą kolekcję?')
    print('   (To zwolni miejsce - najszybciej)\n')
    
    response = input('Wpisz TAK aby usunąć: ')
    
    if response.upper() == 'TAK':
        col.drop()
        print('✅ Kolekcja usunięta - zwolniono miejsce!')
        print('\nTeraz uruchom:')
        print('  python scripts/etl_pipeline.py')
        client.close()
        return
    
    print('Anulowano')
    client.close()

if __name__ == '__main__':
    cleanup_duplicates()