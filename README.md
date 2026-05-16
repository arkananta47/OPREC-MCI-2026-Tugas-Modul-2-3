# 🚀 MCI2026 Task 2 — Orders Data Pipeline  
### Kelompok 28

**Pipeline Orchestration & Data Visualization**  
`Apache Airflow` → `PySpark` → `ClickHouse` → `Metabase / Power BI`

---

# 📋 Daftar Isi

1. Overview  
2. Arsitektur Pipeline  
3. Struktur Repository  
4. Cara Menjalankan  
5. Penjelasan Script  
6. Data Warehouse Schema  
7. Metabase Visualization  
8. Power BI Visualization  
9. Troubleshooting  
10. Anggota Kelompok  

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

- orders
- products
- category / department
- reordered items
- customer orders

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

# 📂 Struktur Repository

```text
tugas-modul-2-3/
│
├── docker-compose.yml
│
├── dags/
│   ├── pipeline.py
│   │
│   └── scripts/
│       ├── fetch_orders.py
│       └── process_orders_spark.py
│
├── data_lake/
│   └── orders/
│
├── sql/
│   └── metabase_queries.sql
│
├── requirements.txt
│
└── README.md
```

---

# 🐳 Cara Menjalankan

## 1. Clone Repository

```bash
git clone <repository-url>
cd tugas-modul-2-3
```

---

## 2. Buat Folder Data Lake

```bash
mkdir -p data_lake/orders
```

---

## 3. Jalankan Docker Compose

```bash
docker compose up -d
```

---

## 4. Cek Container

```bash
docker compose ps
```

Pastikan container berikut berjalan:

- postgres
- airflow-webserver
- airflow-scheduler
- clickhouse-server
- metabase

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

Schedule:

```python
schedule_interval="@daily"
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

## Tambahkan Database

Masuk ke:

```text
Admin → Databases → Add Database
```

---

## Pilih ClickHouse

Isi:

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

---

# 📊 Power BI Visualization

Selain Metabase, dataset juga dapat divisualisasikan menggunakan **Power BI**.

---

# 🔌 Connect Power BI ke ClickHouse

## Install ClickHouse ODBC Driver

Download:

```text
https://clickhouse.com/docs/en/interfaces/odbc
```

---

## Tambahkan Data Source

Di Power BI:

```text
Get Data
  → ODBC
```

Pilih ClickHouse DSN.

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

# 📈 Recommended Power BI Charts

| Dashboard | Visualization |
|---|---|
| Daily Orders | Line Chart |
| Revenue Trend | Area Chart |
| Top Products | Horizontal Bar |
| Category Revenue | Treemap |
| Reorder Distribution | Donut Chart |
| Customer Analytics | Table |
| KPI Summary | Cards |

---

# 🔧 Troubleshooting

| Problem | Cause | Solution |
|---|---|---|
| DAG tidak muncul | syntax error DAG | cek scheduler logs |
| Spark gagal membaca parquet | folder kosong | jalankan fetch_orders |
| ClickHouse kosong | insert gagal | cek process_orders_spark logs |
| Metabase tidak connect | hostname salah | gunakan clickhouse-server |
| Power BI gagal connect | ODBC belum install | install ClickHouse ODBC |
| `reordered` column missing | parquet schema lama | hapus parquet lama |

---

# 🧹 Reset Pipeline

Jika schema berubah:

```bash
rm -rf data_lake/orders/*.parquet
```

Lalu trigger ulang DAG.

---

# 👥 Anggota Kelompok 28

| Nama | NRP |
|---|---|
| ... | ... |
| ... | ... |
| ... | ... |

---

# ✅ Hasil Pipeline

Pipeline berhasil:

- mengambil data dari API
- menyimpan ke data lake
- memproses analytics menggunakan Spark
- memuat data ke ClickHouse
- divisualisasikan di Metabase & Power BI

---

*MCI2026 — Modul 2 & 3 — Task 2 — Kelompok 28*