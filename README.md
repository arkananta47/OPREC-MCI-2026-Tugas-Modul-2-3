# 🚀 MCI2026 Task 2 — Orders Data Pipeline  
### Kelompok 28
# 👥 Anggota Kelompok 28

| Nama | NRP |
|---|---|
| Muh. Aqil Alqadri Syahid | 5025241161 |
| Kadek Andra Wikanjaya Putra | 5025241187 |

**Pipeline Orchestration & Data Visualization**  
`Apache Airflow` → `PySpark` → `ClickHouse` → `Metabase / Power BI`

# 📋 Daftar Isi
1. Overview  
2. Arsitektur Pipeline  
3. Struktur Repository  
4. Cara Menjalankan  
5. Penjelasan Script  
6. Data Warehouse Schema  
7. Metabase Visualization

---

# 🎯 Overview

Project ini membangun **end-to-end ETL data pipeline** menggunakan:

- **Apache Airflow** → orchestrasi pipeline
- **PySpark** → transformasi & analytics
- **Parquet Data Lake** → staging layer
- **ClickHouse** → analytical data warehouse
- **Metabase / Power BI** → dashboard visualization

Dataset berasal dari REST API:

```text
http://96.9.212.102:8000/orders
```

Dataset berbentuk **Instacart-style grocery orders** yang berisi:

- **orders**
- **products**
- **category / department**
- **reordered items**
- **customer orders**

---

# 🏗 Arsitektur Pipeline

```text
                ┌────────────────────┐
                │   REST API Orders  │
                │  /orders endpoint  │
                └─────────┬──────────┘
                          │
                          ▼
              fetch_orders.py (Airflow)
                          │
                          ▼
         ┌────────────────────────────────┐
         │      Data Lake (Parquet)       │
         │ data_lake/orders/*.parquet     │
         └────────────────────────────────┘
                          │
                          ▼
          process_orders_spark.py (PySpark)
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
 orders_trending   orders_category   orders_daily_orders
    _products         _summary
                          │
                          ▼
                  ┌────────────────┐
                  │   ClickHouse   │
                  │   mci2026_db   │
                  └────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
       Metabase                    Power BI
```

---

# 🌐 Akses Service

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | setup awal |
| ClickHouse HTTP | http://localhost:8123 | kelompok28 / kelompok28 |

---

# ▶️ Menjalankan Pipeline

## 1. Buka Airflow

```text
http://localhost:8080
```

---

## 2. Aktifkan DAG

Aktifkan DAG:

```text
mci2026_orders_pipeline
```

---

## 3. Trigger DAG

Klik:

```text
Trigger DAG
```

Pipeline akan menjalankan:

```text
start
   ↓
fetch_orders
   ↓
process_orders_spark
   ↓
end
```

---

# 📦 Penjelasan Script

# 1. fetch_orders.py

Script ini:

- mengambil data dari REST API
- melakukan parsing nested JSON
- flatten products array
- generate synthetic analytics fields
- menyimpan hasil ke Parquet

## Output

```text
data_lake/orders/orders_YYYYMMDD_HHMMSS.parquet
```

---

# 2. process_orders_spark.py

Script ini:

- membaca seluruh file parquet
- melakukan transformasi analytics menggunakan PySpark
- load hasil ke ClickHouse

## Tabel yang dihasilkan

| Tabel | Isi |
|---|---|
| orders | raw transactional data |
| orders_trending_products | top products |
| orders_category_summary | category analytics |
| orders_daily_orders | daily analytics |

---

# 3. pipeline.py

Airflow DAG orchestration:

```text
start
  ↓
fetch_orders
  ↓
process_orders_spark
  ↓
end
```

---

# 🗄 ClickHouse Schema

Database:

```sql
mci2026_db
```

## Tables

### 1. orders

Raw transactional data.

### 2. orders_trending_products

Analytics produk terlaris.

### 3. orders_category_summary

Analytics kategori produk.

### 4. orders_daily_orders

Analytics harian.

---

# 📊 Metabase Visualization

# Setup Metabase

## Pilih ClickHouse

**Database Information**:

| Field | Value |
|---|---|
| Host | clickhouse-server |
| Port | 8123 |
| Database | mci2026_db |
| Username | kelompok28 |
| Password | kelompok28 |

---

# 📈 Dashboard Queries

## Q1 — Daily Order Activity

Visualisasi:

- Line Chart

Menampilkan:

- total_orders
- total_items
- daily_revenue

---

## Q2 — Top Ordered Products

Visualisasi:

- Bar Chart

Menampilkan:

- produk paling sering dibeli

---

## Q3 — Category Summary

Visualisasi:

- Bar Chart

Menampilkan:

- kategori paling populer
- reorder rate
- total revenue

---

## Q4 — Reorder Distribution

Visualisasi:

- Pie Chart

Menampilkan:

- reordered vs first purchase

---

## Q5 — Payment Method Distribution

Visualisasi:

- Pie Chart

---

## Q6 — Top Shipping Cities

Visualisasi:

- Bar Chart

---

## Q7 — Basket Size Distribution

Visualisasi:

- Histogram / Bar Chart

---

## Q8 — Top Reordered Products

Visualisasi:

- Bar Chart

---

## Q9 — KPI Dashboard

Visualisasi:

- Scorecards

Menampilkan:

- total orders
- total customers
- total products
- total revenue

---

## Q10 — Category Product Distribution

Visualisasi:

- Bar Chart



# 📊 Power BI Visualization

Selain Metabase, dataset juga dapat divisualisasikan menggunakan **Power BI**.

---

## Connection Settings

| Field | Value |
|---|---|
| Host | localhost |
| Port | 8123 |
| Database | mci2026_db |
| Username | kelompok28 |
| Password | kelompok28 |

---

# ✅ Hasil Pipeline

Pipeline berhasil:

- Mengambil data dari API
- Menyimpan ke data lake
- Memproses analytics menggunakan Spark
- Memuat data ke ClickHouse
- Divisualisasikan di Metabase & Power BI

---

*MCI2026 — Modul 2 & 3 — Task 2 — Kelompok 28*