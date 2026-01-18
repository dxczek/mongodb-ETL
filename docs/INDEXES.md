# 📊 Dokumentacja Indeksów

## Lista indeksów (541,909 dokumentów)

| Indeks | Pola | Zastosowanie |
|--------|------|--------------|
| idx_invoice_no | source.externalId | Wyszukiwanie faktury |
| idx_customer_id | entity.id | Zamówienia klienta |
| idx_customer_date | entity.id + eventTime | Chronologiczne zamówienia |
| idx_date_desc | eventTime | Sortowanie po dacie |
| idx_country | metadata.country | Filtrowanie po kraju |
| idx_country_date | metadata.country + eventTime | Sprzedaż czasowo |
| idx_stock_code | metadata.stockCode | Wyszukiwanie produktu |
| idx_description_text | metadata.description | Full-text search |

## Rozmiar indeksów
- Całkowita baza: ~500 MB
- Indeksy: ~150 MB