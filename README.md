MongoDB ETL Project

MongoDB ETL Project to kompletne rozwiązanie do importu danych z Kaggle do MongoDB Atlas, wraz z transformacją danych, automatycznym tworzeniem indeksów oraz harmonogramem codziennego ładowania danych. Projekt jest zoptymalizowany pod kątem dużych zbiorów danych i wydajności.

Features

Import danych z wielu źródeł CSV (Online Retail, Sales Data, Customers)

Transformacja danych do jednolitej struktury w MongoDB

Automatyczne tworzenie indeksów dla szybszych zapytań

Obsługa dużych plików CSV w chunks i batchach

Codzienny harmonogram ładowania danych za pomocą skryptu scheduler

Obsługa unikalnych dokumentów i możliwość czyszczenia duplikatów

Raportowanie postępu i statystyk w konsoli

Data

Primary Source: Kaggle Online Retail Dataset

Number of Documents: ~541,909

Indexes: 8 zoptymalizowanych indeksów dla szybkiego wyszukiwania

Obsługiwane typy danych: liczby, daty, tekst, identyfikatory klientów, produkty, regiony, kraje

Quick Start
1. Instalacja

Upewnij się, że masz zainstalowany Python 3.9+ i pip. Następnie uruchom:

pip install -r requirements.txt

2. Konfiguracja

Skopiuj plik .env.example do .env i wprowadź swoje dane MongoDB:

MONGODB_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net
SCHEDULE_TIME=02:00

3. Tworzenie indeksów

Skrypt automatycznie tworzy indeksy dla wszystkich kluczowych pól:

python scripts/create_indexes.py

4. ETL Pipeline

Aby załadować dane do MongoDB:

python scripts/etl_pipeline.py


Pipeline obsługuje:

Ładowanie danych z trzech źródeł CSV

Transformację do wspólnej struktury dokumentu

Podział na batch i chunks dla wydajności

Raportowanie postępu i statystyk w konsoli

5. Scheduler (opcjonalnie)

Aby ustawić automatyczne codzienne ładowanie danych:

python scripts/scheduler.py


Domyślny czas harmonogramu ustawiany jest w .env (np. 02:00).

Project Structure
MongoDB-ETL-Project/
├─ scripts/          # Skrypty Python
│  ├─ etl_pipeline.py    # Główny skrypt ETL
│  ├─ create_indexes.py  # Skrypt tworzenia indeksów
│  ├─ scheduler.py       # Harmonogram codziennego uruchamiania ETL
├─ data/             # Pliki CSV źródłowe
├─ docs/             # Dokumentacja projektu
├─ .env              # Zmienne środowiskowe
├─ requirements.txt  # Biblioteki Python
└─ README.md         # Ten plik

File Descriptions
File	Description
etl_pipeline.py	Importuje i transformuje dane z CSV do MongoDB w formie dokumentów. Obsługuje batchowanie i chunks dla dużych plików.
create_indexes.py	Tworzy indeksy w kolekcji MongoDB dla przyspieszenia zapytań.
scheduler.py	Harmonogram codziennego uruchamiania ETL o określonej godzinie.
.env	Zawiera konfigurację MongoDB oraz czas harmonogramu.
data/	Katalog z plikami CSV dla wszystkich źródeł danych.
docs/	Dokumentacja i dodatkowe materiały projektu.
Usage

Upewnij się, że dane CSV znajdują się w katalogu data/.

Skonfiguruj połączenie do MongoDB w .env.

Uruchom tworzenie indeksów: python scripts/create_indexes.py.

Załaduj dane ETL: python scripts/etl_pipeline.py.

(Opcjonalnie) Włącz scheduler, aby ETL uruchamiał się codziennie: python scripts/scheduler.py.

Stats & Monitoring

Po wykonaniu ETL możesz sprawdzić:

Liczbę dokumentów w kolekcji

Rozmiar kolekcji w MB

Unikalne produkty, klientów i kraje

Średnią wartość zamówienia oraz liczbę sprzedanych jednostek

Dane w konsoli są logowane w czasie rzeczywistym

Notes

Pipeline obsługuje brakujące wartości i różne formaty dat w CSV.

Skrypty są zoptymalizowane pod względem wydajności przy dużych zbiorach danych.

W przypadku błędów w batchach pipeline loguje informacje do konsoli.
