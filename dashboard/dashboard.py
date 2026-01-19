"""
MongoDB Streamlit Dashboard
Autor: Daniel
Data: 2026-01-18

Interaktywny dashboard z KPI, wykresami i agregacjami
Uruchomienie: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from aggregations import AggregationPipelines
import time

# === PAGE CONFIG ===
st.set_page_config(
    page_title="Online Retail Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === STYLE ===
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 32px;
        font-weight: bold;
        color: #1f77b4;
    }
    .metric-label {
        font-size: 14px;
        color: #666;
        margin-top: 10px;
    }
</style>
""", unsafe_allow_html=True)

# === CACHE ===
@st.cache_resource
def get_aggregations():
    """Cache agregacji"""
    return AggregationPipelines()

@st.cache_data(ttl=300)  # Cache na 5 minut
def load_kpi():
    """Pobierz KPI"""
    agg = get_aggregations()
    return agg.get_all_kpi()

@st.cache_data(ttl=300)
def load_top_products():
    """Pobierz top produkty"""
    agg = get_aggregations()
    return agg.top_products(10)

@st.cache_data(ttl=300)
def load_top_countries():
    """Pobierz top kraje"""
    agg = get_aggregations()
    return agg.top_countries(10)

@st.cache_data(ttl=300)
def load_top_customers():
    """Pobierz top klientów"""
    agg = get_aggregations()
    return agg.top_customers(10)

@st.cache_data(ttl=300)
def load_daily_revenue():
    """Pobierz przychód dziennie"""
    agg = get_aggregations()
    return agg.daily_revenue()

@st.cache_data(ttl=300)
def load_revenue_by_source():
    """Pobierz przychód per źródło"""
    agg = get_aggregations()
    return agg.revenue_by_source()

@st.cache_data(ttl=300)
def load_customer_segmentation():
    """Pobierz segmentację klientów"""
    agg = get_aggregations()
    return agg.customer_segmentation()

@st.cache_data(ttl=300)
def load_product_performance():
    """Pobierz performance produktów"""
    agg = get_aggregations()
    return agg.product_performance() ### bylo 10 TEST na 

# === HEADER ===
st.title("📊 Online Retail Analytics Dashboard")
st.markdown("**Real-time analysis of 541,938 retail transactions**")

# === SIDEBAR ===
with st.sidebar:
    st.header("⚙️ Ustawienia")
    
    refresh_interval = st.slider(
        "Cache refresh (sekundy)",
        60, 3600, 300,
        help="Jak często odświeżać dane"
    )
    
    if st.button("🔄 Odśwież teraz"):
        st.cache_data.clear()
        st.success("Cache wyczyszczony!")
    
    st.markdown("---")
    st.markdown("**Informacje**")
    st.info("""
    📈 **Źródła danych:**
    - Online Retail: 541,909 docs
    - Sales Data: 21 docs
    - Customers: 8 docs
    
    🗄️ **Indeksy:** 9
    ⏱️ **Query time:** <10ms (dzięki indeksom)
    """)

# === MAIN CONTENT ===

# --- KPI Section ---
st.header("📈 Kluczowe Wskaźniki (KPI)")

kpi = load_kpi()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="💰 Całkowity Przychód",
        value=f"${kpi['total_revenue']:,.2f}",
        delta="Wszystkie źródła"
    )

with col2:
    st.metric(
        label="📦 Liczba Zamówień",
        value=f"{kpi['total_orders']:,}",
        delta="Transakcje"
    )

with col3:
    st.metric(
        label="👥 Liczba Klientów",
        value=f"{kpi['unique_customers']:,}",
        delta="Unikalni klienci"
    )

col4, col5, col6 = st.columns(3)

with col4:
    st.metric(
        label="💵 Średnia Wartość Zamówienia",
        value=f"${kpi['average_order_value']:.2f}",
        delta="Per transakcja"
    )

with col5:
    st.metric(
        label="📊 Sprzedane Jednostki",
        value=f"{kpi['total_items_sold']:,}",
        delta="Całkowita ilość"
    )

with col6:
    st.metric(
        label="🌍 Kraje",
        value=f"{kpi['unique_countries']}",
        delta="Regiony dostawy"
    )

# --- Tabs ---
tab1, tab2, tab3, tab4 = st.tabs(["📊 Agregacje", "🎯 Top Listy", "📈 Trendy", "🔍 Szczegóły"])

# === TAB 1: AGREGACJE ===
with tab1:
    st.subheader("1️⃣ Przychód per Źródło Danych")
    
    revenue_source = load_revenue_by_source()
    df_source = pd.DataFrame(revenue_source)
    
    if not df_source.empty:
        fig_source = px.pie(
            df_source,
            values='revenue',
            names='source',
            title="Rozkład przychodu po źródłach",
            hole=0.3
        )
        st.plotly_chart(fig_source, use_container_width=True)
        
        st.dataframe(df_source, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("2️⃣ Segmentacja Klientów")
    
    segmentation = load_customer_segmentation()
    df_seg = pd.DataFrame(segmentation)
    
    if not df_seg.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_seg = px.bar(
                df_seg,
                x='segment',
                y='customers',
                title="Liczba klientów per segment",
                labels={'customers': 'Klienci', 'segment': 'Segment'},
                color='segment',
                color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1']
            )
            st.plotly_chart(fig_seg, use_container_width=True)
        
        with col2:
            fig_rev = px.bar(
                df_seg,
                x='segment',
                y='revenue',
                title="Przychód per segment",
                labels={'revenue': 'Przychód', 'segment': 'Segment'},
                color='segment',
                color_discrete_sequence=['#FF6B6B', '#4ECDC4', '#45B7D1']
            )
            st.plotly_chart(fig_rev, use_container_width=True)
        
        st.dataframe(df_seg, use_container_width=True)

# === TAB 2: TOP LISTY ===
with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top 10 Produktów")
        
        top_prod = load_top_products()
        df_prod = pd.DataFrame(top_prod)
        
        if not df_prod.empty:
            fig_prod = px.bar(
                df_prod,
                x='revenue',
                y='product_name',
                orientation='h',
                title="Przychód per produkt",
                labels={'revenue': 'Przychód ($)', 'product_name': 'Produkt'},
                color='revenue',
                color_continuous_scale='Blues'
            )
            fig_prod.update_layout(height=500)
            st.plotly_chart(fig_prod, use_container_width=True)
            
            st.dataframe(df_prod[['stock_code', 'product_name', 'revenue', 'quantity']], use_container_width=True)
    
    with col2:
        st.subheader("🌍 Top 10 Krajów")
        
        top_country = load_top_countries()
        df_country = pd.DataFrame(top_country)
        
        if not df_country.empty:
            fig_country = px.bar(
                df_country,
                x='revenue',
                y='country',
                orientation='h',
                title="Przychód per kraj",
                labels={'revenue': 'Przychód ($)', 'country': 'Kraj'},
                color='revenue',
                color_continuous_scale='Greens'
            )
            fig_country.update_layout(height=500)
            st.plotly_chart(fig_country, use_container_width=True)
            
            st.dataframe(df_country[['country', 'orders', 'unique_customers']], use_container_width=True)
    
    st.markdown("---")
    st.subheader("👥 Top 10 Klientów")
    
    top_cust = load_top_customers()
    df_cust = pd.DataFrame(top_cust)
    
    if not df_cust.empty:
        fig_cust = px.scatter(
            df_cust,
            x='orders',
            y='revenue',
            size='items',
            hover_data=['customer_id', 'avg_order_value'],
            title="Klienci: zamówienia vs przychód",
            labels={'orders': 'Liczba zamówień', 'revenue': 'Przychód ($)', 'items': 'Jednostki'}
        )
        st.plotly_chart(fig_cust, use_container_width=True)
        
        st.dataframe(df_cust, use_container_width=True)

# === TAB 3: TRENDY ===
with tab3:
    st.subheader("📈 Przychód Dziennie (ostatni rok)")
    
    daily = load_daily_revenue()
    df_daily = pd.DataFrame(daily)
    
    if not df_daily.empty:
        fig_daily = px.line(
            df_daily,
            x='date',
            y='revenue',
            title="Przychód dziennie",
            labels={'date': 'Data', 'revenue': 'Przychód ($)'},
            markers=True
        )
        fig_daily.update_layout(height=400)
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # Statystyka
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Średni przychód dziennie", f"${df_daily['revenue'].mean():.2f}")
        
        with col2:
            st.metric("Maksymalny przychód dziennie", f"${df_daily['revenue'].max():.2f}")
        
        with col3:
            st.metric("Minimalny przychód dziennie", f"${df_daily['revenue'].min():.2f}")

# === TAB 4: SZCZEGÓŁY ===
with tab4:
    st.subheader("🔍 Product Performance")
    
    perf = load_product_performance()
    df_perf = pd.DataFrame(perf)
    
    if not df_perf.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_perf_rev = px.bar(
                df_perf.head(10),
                x='revenue',
                y='description',
                orientation='h',
                title="Top produkty - Przychód",
                color='revenue',
                color_continuous_scale='Reds'
            )
            fig_perf_rev.update_layout(height=500)
            st.plotly_chart(fig_perf_rev, use_container_width=True)
        
        with col2:
            fig_perf_qty = px.bar(
                df_perf.head(10),
                x='quantity',
                y='description',
                orientation='h',
                title="Top produkty - Ilość",
                color='quantity',
                color_continuous_scale='Blues'
            )
            fig_perf_qty.update_layout(height=500)
            st.plotly_chart(fig_perf_qty, use_container_width=True)
        
        st.dataframe(df_perf, use_container_width=True)

# === FOOTER ===
st.markdown("---")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**📊 Data Source**")
    st.markdown("MongoDB Atlas\n541,938 dokumentów\n9 indeksów")

with col2:
    st.markdown("**⚡ Performance**")
    st.markdown("Query time: <10ms\nCache: 5 min\nUpdate: Live")

with col3:
    st.markdown("**📈 Metrics**")
    st.markdown(f"Last refresh: {time.strftime('%H:%M:%S')}\nStatus: ✅ Active")