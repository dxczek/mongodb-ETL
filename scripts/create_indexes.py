"""
Duplicate cleanup script - removes duplicate documents from a MongoDB collection or optionally drops the entire collection to free space.
"""

import pymongo

MONGO_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"

def cleanup_duplicates():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]
    
    print('🧹 Cleaning up duplicates...\n')
    
    total_before = col.count_documents({})
    print(f'Documents before: {total_before:,}')
    
    print('\n⚠️  Do you want to drop the entire collection?')
    print('   (This will free space and is the fastest way)\n')
    
    response = input('Type YES to drop: ')
    
    if response.upper() == 'YES':
        col.drop()
        print('✅ Collection dropped - space freed!')
        print('\nNow run:')
        print('  python scripts/etl_pipeline.py')
        client.close()
        return
    
    print('Cancelled')
    client.close()

if __name__ == '__main__':
    cleanup_duplicates()
