"""
MongoDB Aggregation Pipeline'y dla Dashboard'u
Autor: Daniel
Data: 2026-01-18

Wszystkie KPI i agregacje potrzebne do dashboard'u Streamlit
"""

import pymongo
from datetime import datetime, timedelta
from typing import Dict, List, Any

# === CONFIG ===
MONGODB_URI = "mongodb+srv://janduczek_db_user:B2LTZ7stECMF2jg8@dev-cluster.cuerdh8.mongodb.net/?appName=dev-cluster"
DB_NAME = "analytics"
COLLECTION = "records"


class AggregationPipelines:
    """Wszystkie agregacje MongoDB dla KPI i Dashboard'u"""
    
    def __init__(self):
        self.client = pymongo.MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        self.col = self.db[COLLECTION]
    
    # ========== KPI - GŁÓWNE LICZBY ==========
    
    def kpi_total_revenue(self) -> float:
        """KPI: Całkowity przychód"""
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'total': {'$sum': '$metrics.amount'}
                }
            }
        ]
        result = list(self.col.aggregate(pipeline))
        return result[0]['total'] if result else 0
    
    def kpi_total_orders(self) -> int:
        """KPI: Liczba zamówień"""
        return self.col.count_documents({})
    
    def kpi_unique_customers(self) -> int:
        """KPI: Liczba unikalnych klientów"""
        pipeline = [
            {
                '$group': {
                    '_id': '$entity.id'
                }
            },
            {
                '$count': 'count'
            }
        ]
        result = list(self.col.aggregate(pipeline))
        return result[0]['count'] if result else 0
    
    def kpi_average_order_value(self) -> float:
        """KPI: Średnia wartość zamówienia"""
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'avg_value': {'$avg': '$metrics.amount'}
                }
            }
        ]
        result = list(self.col.aggregate(pipeline))
        return result[0]['avg_value'] if result else 0
    
    def kpi_total_items_sold(self) -> int:
        """KPI: Liczba sprzedanych jednostek"""
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'total_items': {'$sum': '$metrics.count'}
                }
            }
        ]
        result = list(self.col.aggregate(pipeline))
        return result[0]['total_items'] if result else 0
    
    def kpi_unique_countries(self) -> int:
        """KPI: Liczba krajów"""
        pipeline = [
            {
                '$match': {
                    'metadata.country': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$metadata.country'
                }
            },
            {
                '$count': 'count'
            }
        ]
        result = list(self.col.aggregate(pipeline))
        return result[0]['count'] if result else 0
    
    # ========== AGREGACJE - TOP LISTY ==========
    
    def top_products(self, limit: int = 10) -> List[Dict]:
        """Agregacja: Top 10 produktów"""
        pipeline = [
            {
                '$match': {
                    'metadata.stockCode': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$metadata.stockCode',
                    'product_name': {'$first': '$metadata.description'},
                    'revenue': {'$sum': '$metrics.amount'},
                    'quantity': {'$sum': '$metrics.count'},
                    'orders': {'$sum': 1}
                }
            },
            {
                '$sort': {'revenue': -1}
            },
            {
                '$limit': limit
            },
            {
                '$project': {
                    '_id': 0,
                    'stock_code': '$_id',
                    'product_name': 1,
                    'revenue': {'$round': ['$revenue', 2]},
                    'quantity': 1,
                    'orders': 1,
                    'avg_price': {
                        '$round': [{'$divide': ['$revenue', '$quantity']}, 2]
                    }
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def top_countries(self, limit: int = 10) -> List[Dict]:
        """Agregacja: Top 10 krajów"""
        pipeline = [
            {
                '$match': {
                    'metadata.country': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$metadata.country',
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1},
                    'customers': {'$addToSet': '$entity.id'}
                }
            },
            {
                '$sort': {'revenue': -1}
            },
            {
                '$limit': limit
            },
            {
                '$project': {
                    '_id': 0,
                    'country': '$_id',
                    'revenue': {'$round': ['$revenue', 2]},
                    'orders': 1,
                    'unique_customers': {'$size': '$customers'}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def top_customers(self, limit: int = 10) -> List[Dict]:
        """Agregacja: Top 10 klientów"""
        pipeline = [
            {
                '$group': {
                    '_id': '$entity.id',
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1},
                    'items': {'$sum': '$metrics.count'},
                    'countries': {'$addToSet': '$metadata.country'}
                }
            },
            {
                '$sort': {'revenue': -1}
            },
            {
                '$limit': limit
            },
            {
                '$project': {
                    '_id': 0,
                    'customer_id': '$_id',
                    'revenue': {'$round': ['$revenue', 2]},
                    'orders': 1,
                    'items': 1,
                    'avg_order_value': {
                        '$round': [{'$divide': ['$revenue', '$orders']}, 2]
                    },
                    'countries': 1
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    # ========== AGREGACJE - TRENDY ==========
    
    def daily_revenue(self) -> List[Dict]:
        """Agregacja: Przychód dziennie"""
        pipeline = [
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m-%d',
                            'date': '$eventTime'
                        }
                    },
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1}
                }
            },
            {
                '$sort': {'_id': 1}
            },
            {
                '$limit': 365  # Ostatni rok
            },
            {
                '$project': {
                    '_id': 0,
                    'date': '$_id',
                    'revenue': {'$round': ['$revenue', 2]},
                    'orders': 1
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def monthly_revenue(self) -> List[Dict]:
        """Agregacja: Przychód miesięcznie"""
        pipeline = [
            {
                '$group': {
                    '_id': {
                        '$dateToString': {
                            'format': '%Y-%m',
                            'date': '$eventTime'
                        }
                    },
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1},
                    'customers': {'$addToSet': '$entity.id'}
                }
            },
            {
                '$sort': {'_id': 1}
            },
            {
                '$project': {
                    '_id': 0,
                    'month': '$_id',
                    'revenue': {'$round': ['$revenue', 2]},
                    'orders': 1,
                    'customers': {'$size': '$customers'}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    # ========== AGREGACJE - ROZKŁADY ==========
    
    def revenue_by_source(self) -> List[Dict]:
        """Agregacja: Przychód per źródło danych"""
        pipeline = [
            {
                '$group': {
                    '_id': '$source.sourceId',
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1},
                    'avg_order': {'$avg': '$metrics.amount'}
                }
            },
            {
                '$sort': {'revenue': -1}
            },
            {
                '$project': {
                    '_id': 0,
                    'source': '$_id',
                    'revenue': {'$round': ['$revenue', 2]},
                    'orders': 1,
                    'avg_order': {'$round': ['$avg_order', 2]}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def customers_by_country(self) -> List[Dict]:
        """Agregacja: Rozkład klientów per kraj"""
        pipeline = [
            {
                '$match': {
                    'metadata.country': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$metadata.country',
                    'customers': {'$addToSet': '$entity.id'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'country': '$_id',
                    'customer_count': {'$size': '$customers'}
                }
            },
            {
                '$sort': {'customer_count': -1}
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def order_value_distribution(self, buckets: int = 5) -> List[Dict]:
        """Agregacja: Rozkład wartości zamówień"""
        pipeline = [
            {
                '$bucket': {
                    'groupBy': '$metrics.amount',
                    'boundaries': [0, 10, 50, 100, 500, 10000],
                    'default': 'other',
                    'output': {
                        'count': {'$sum': 1},
                        'avg': {'$avg': '$metrics.amount'},
                        'total': {'$sum': '$metrics.amount'}
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'range': '$_id',
                    'count': 1,
                    'avg': {'$round': ['$avg', 2]},
                    'total': {'$round': ['$total', 2]}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    # ========== AGREGACJE - ANALITYKA ==========
    
    def customer_segmentation(self) -> List[Dict]:
        """Agregacja: Segmentacja klientów (VIP/Regular/New)"""
        pipeline = [
            {
                '$group': {
                    '_id': '$entity.id',
                    'revenue': {'$sum': '$metrics.amount'},
                    'orders': {'$sum': 1},
                    'first_order': {'$min': '$eventTime'},
                    'last_order': {'$max': '$eventTime'}
                }
            },
            {
                '$addFields': {
                    'segment': {
                        '$cond': [
                            {'$gte': ['$revenue', 1000]},
                            'VIP',
                            {'$cond': [
                                {'$gte': ['$revenue', 100]},
                                'Regular',
                                'New'
                            ]}
                        ]
                    }
                }
            },
            {
                '$group': {
                    '_id': '$segment',
                    'customer_count': {'$sum': 1},
                    'total_revenue': {'$sum': '$revenue'},
                    'avg_revenue': {'$avg': '$revenue'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'segment': '$_id',
                    'customers': '$customer_count',
                    'revenue': {'$round': ['$total_revenue', 2]},
                    'avg_per_customer': {'$round': ['$avg_revenue', 2]}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    def product_performance(self) -> List[Dict]:
        """Agregacja: Performance produktów (marża, obrot)"""
        pipeline = [
            {
                '$match': {
                    'metadata.stockCode': {'$exists': True, '$ne': ''}
                }
            },
            {
                '$group': {
                    '_id': '$metadata.stockCode',
                    'description': {'$first': '$metadata.description'},
                    'total_revenue': {'$sum': '$metrics.amount'},
                    'total_quantity': {'$sum': '$metrics.count'},
                    'avg_price': {'$avg': '$metrics.unitPrice'},
                    'orders': {'$sum': 1}
                }
            },
            {
                '$addFields': {
                    'turnover': {
                        '$multiply': [
                            {'$divide': ['$total_quantity', 365]},
                            '$avg_price'
                        ]
                    }
                }
            },
            {
                '$sort': {'total_revenue': -1}
            },
            {
                '$limit': 20
            },
            {
                '$project': {
                    '_id': 0,
                    'stock_code': '$_id',
                    'description': 1,
                    'revenue': {'$round': ['$total_revenue', 2]},
                    'quantity': '$total_quantity',
                    'orders': 1,
                    'avg_price': {'$round': ['$avg_price', 2]},
                    'turnover': {'$round': ['$turnover', 2]}
                }
            }
        ]
        return list(self.col.aggregate(pipeline))
    
    # ========== POBRANIE WSZYSTKICH KPI ==========
    
    def get_all_kpi(self) -> Dict[str, Any]:
        """Pobierz wszystkie KPI w jednym miejscu"""
        return {
            'total_revenue': self.kpi_total_revenue(),
            'total_orders': self.kpi_total_orders(),
            'unique_customers': self.kpi_unique_customers(),
            'average_order_value': self.kpi_average_order_value(),
            'total_items_sold': self.kpi_total_items_sold(),
            'unique_countries': self.kpi_unique_countries()
        }
    
    def get_all_aggregations(self) -> Dict[str, Any]:
        """Pobierz wszystkie agregacje"""
        return {
            'top_products': self.top_products(),
            'top_countries': self.top_countries(),
            'top_customers': self.top_customers(),
            'daily_revenue': self.daily_revenue(),
            'monthly_revenue': self.monthly_revenue(),
            'revenue_by_source': self.revenue_by_source(),
            'customers_by_country': self.customers_by_country(),
            'order_value_distribution': self.order_value_distribution(),
            'customer_segmentation': self.customer_segmentation(),
            'product_performance': self.product_performance()
        }
    
    def close(self):
        """Zamknij połączenie"""
        self.client.close()


# ========== TESTY ==========

if __name__ == '__main__':
    print('🚀 MongoDB Aggregation Pipelines\n')
    
    agg = AggregationPipelines()
    
    # KPI
    print('📊 KPI:')
    print(f'  Total Revenue: ${agg.kpi_total_revenue():,.2f}')
    print(f'  Total Orders: {agg.kpi_total_orders():,}')
    print(f'  Unique Customers: {agg.kpi_unique_customers():,}')
    print(f'  Avg Order Value: ${agg.kpi_average_order_value():.2f}')
    print(f'  Total Items Sold: {agg.kpi_total_items_sold():,}')
    print(f'  Unique Countries: {agg.kpi_unique_countries()}')
    
    # Top Products
    print('\n🏆 Top 5 Products:')
    for p in agg.top_products(5):
        print(f'  {p["stock_code"]}: ${p["revenue"]} ({p["quantity"]} units)')
    
    # Top Countries
    print('\n🌍 Top 5 Countries:')
    for c in agg.top_countries(5):
        print(f'  {c["country"]}: ${c["revenue"]} ({c["orders"]} orders)')
    
    # Revenue by Source
    print('\n📈 Revenue by Source:')
    for s in agg.revenue_by_source():
        print(f'  {s["source"]}: ${s["revenue"]}')
    
    # Customer Segmentation
    print('\n👥 Customer Segmentation:')
    for seg in agg.customer_segmentation():
        print(f'  {seg["segment"]}: {seg["customers"]} customers (${seg["revenue"]})')
    
    agg.close()
    print('\n✅ Done!')