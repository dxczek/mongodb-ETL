# 🔍 Przykładowe zapytania

## W MongoDB Compass / Atlas

### Zamówienia klienta
\`\`\`javascript
db.records.find({ "entity.id": "17850" })
  .sort({ eventTime: -1 })
  .limit(10)
\`\`\`

### Sprzedaż w kraju
\`\`\`javascript
db.records.find({ "metadata.country": "United Kingdom" })
  .count()
\`\`\`

### Wyszukiwanie produktu
\`\`\`javascript
db.records.find({ $text: { $search: "LANTERN" } })
\`\`\`

### Przychód dziennie
\`\`\`javascript
db.records.aggregate([
  {
    $group: {
      _id: { $dateToString: { format: "%Y-%m-%d", date: "$eventTime" } },
      revenue: { $sum: "$metrics.amount" },
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: -1 } },
  { $limit: 30 }
])
\`\`\`

### Top produkty
\`\`\`javascript
db.records.aggregate([
  {
    $group: {
      _id: "$metadata.stockCode",
      revenue: { $sum: "$metrics.amount" },
      quantity: { $sum: "$metrics.count" }
    }
  },
  { $sort: { revenue: -1 } },
  { $limit: 10 }
])
\`\`\`