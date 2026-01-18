# 🚀 Instrukcja Uruchomienia

## Wymagania
- Python 3.10+ (sprawdzić: `python --version`)
- MongoDB Atlas (cluster gotowy)
- pip (sprawdzić: `pip --version`)

## Krok 1: Instalacja zależności
\`\`\`bash
pip install -r requirements.txt
\`\`\`

## Krok 2: Konfiguracja
Skopiuj `.env.example` do `.env` i edytuj:
\`\`\`env
MONGODB_URI=mongodb+srv://your_user:your_pass@your_cluster...
CSV_PATH=data/online_retail.csv
\`\`\`

## Krok 3: Tworzenie indeksów
\`\`\`bash
python scripts/create_indexes.py
\`\`\`

## Krok 4: Uruchomienie ETL
\`\`\`bash
python scripts/etl_pipeline.py
\`\`\`

## Krok 5: Harmonogram (opcjonalnie)
\`\`\`bash
python scripts/scheduler.py
\`\`\`

## Sprawdzanie w Atlas
1. Collections → analytics → records
2. Index tab → 8 indeksów
3. Aggregation → przykładowe zapytania