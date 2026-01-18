# MongoDB ETL Project

Projekt ETL do importu danych z Kaggle do MongoDB Atlas z automatycznym tworzeniem indeksów i harmonogramem ładowań.

## Dane
- Źródło: Kaggle Online Retail Dataset
- Dokumenty: 541,909
- Indeksy: 8 (zoptymalizowanych)

## Szybki start
\`\`\`bash
# 1. Instalacja
pip install -r requirements.txt

# 2. Konfiguracja
# Edytuj .env ze swoimi danymi MongoDB

# 3. Tworzenie indeksów
python scripts/create_indexes.py

# 4. Opcjonalnie: ETL
python scripts/etl_pipeline.py

# 5. Opcjonalnie: Harmonogram
python scripts/scheduler.py
\`\`\`

## Struktura
- `scripts/` - Skrypty Python (ETL, indeksy, scheduler)
- `data/` - Pliki CSV
- `docs/` - Dokumentacja
- `.env` - Zmienne środowiska