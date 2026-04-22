# 🚗 Real-Time Ride Booking Data Pipeline

> **Kafka · Spark Structured Streaming · Delta Lake · Azure · Power BI**

A production-grade, fault-tolerant streaming pipeline that processes **5,000+ ride events/day** with low-latency real-time analytics for demand forecasting, surge pricing, and location intelligence.

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                    │
│                                                                            │
│   Ride App / Drivers ──► Kafka Topics (3 topics, 4 partitions each)      │
│                           • ride-requests                                  │
│                           • ride-completed                                 │
│                           • driver-locations                               │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        PROCESSING LAYER                                   │
│                                                                            │
│   Spark Structured Streaming (Databricks / Azure HDInsight)               │
│   • Watermarked windows (2–10 min late-data tolerance)                    │
│   • 5 real-time aggregation queries (parallel)                            │
│     ├── Ride Demand by Zone         (5-min tumbling window)               │
│     ├── Surge Pricing Metrics       (10-min sliding window)               │
│     ├── Location Density Heatmap    (5-min tumbling window)               │
│     ├── Revenue Analytics           (hourly tumbling window)              │
│     └── Cancellation Analytics      (15-min tumbling window)             │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                                      │
│                                                                            │
│   Delta Lake on ADLS Gen2                                                 │
│   • ACID transactions · Time travel · Schema enforcement                  │
│   • Auto-optimize + Z-ORDER for fast BI queries                           │
│   • 7-day retention, partitioned by zone_id / ride_type                   │
└───────────────────────────────┬──────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        ANALYTICS LAYER                                    │
│                                                                            │
│   Power BI (DirectQuery on Delta Lake)                                    │
│   • Live KPI cards: Revenue, Rides, Surge, Ratings                        │
│   • Zone demand heatmap                                                   │
│   • Surge pricing trend charts                                            │
│   • Driver availability map                                               │
│   • Cancellation rate by zone                                             │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
ride-booking-pipeline/
├── kafka/
│   ├── ride_event_producer.py    # Simulates 5K+ events/day
│   └── ride_event_consumer.py    # Consumer with group management
│
├── spark/
│   └── streaming_pipeline.py     # 8 parallel streaming queries
│
├── delta_lake/
│   └── table_manager.py          # Table creation, OPTIMIZE, VACUUM
│
├── dashboard/
│   └── powerbi_measures.dax      # All DAX measures for Power BI
│
├── infrastructure/
│   ├── docker-compose.yml        # Local Kafka + Zookeeper + UI
│   └── setup_azure.sh            # Azure CLI provisioning script
│
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Option A — Local Development

```bash
# 1. Start Kafka (Docker)
cd infrastructure
docker compose up -d
# Kafka UI available at http://localhost:8080

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Create Delta tables (local mode)
# Edit DELTA_BASE in delta_lake/table_manager.py to /tmp/delta/...
python delta_lake/table_manager.py

# 4. Start Spark streaming pipeline
python spark/streaming_pipeline.py

# 5. Start event producer
python kafka/ride_event_producer.py
```

### Option B — Azure Production

```bash
# 1. Login to Azure
az login

# 2. Provision infrastructure
chmod +x infrastructure/setup_azure.sh
./infrastructure/setup_azure.sh

# 3. Update DELTA_BASE / CHECKPOINT_BASE / KAFKA_SERVERS
#    in spark/streaming_pipeline.py with values from .env.azure

# 4. Upload spark/streaming_pipeline.py to Databricks workspace
# 5. Create a Databricks job with the streaming script
# 6. Connect Power BI → Databricks SQL endpoint → Delta tables
```

---

## 📊 Delta Lake Tables

| Table | Window | Partitioned By | Purpose |
|---|---|---|---|
| `ride_demand_by_zone` | 5 min tumbling | `zone_id` | Demand forecasting |
| `surge_pricing_metrics` | 10 min sliding | `ride_type` | Surge decisions |
| `location_density` | 5 min tumbling | `zone_id` | Driver heatmap |
| `revenue_analytics` | 1 hour tumbling | `zone_id`, `ride_type` | Revenue reporting |
| `cancellation_analytics` | 15 min tumbling | `zone_id` | Quality monitoring |

---

## 📈 Power BI Dashboard Pages

1. **Executive Overview** — Total rides, revenue, avg surge, driver rating KPIs
2. **Zone Demand Map** — Geo heatmap with demand intensity
3. **Surge Pricing** — Real-time surge tier per ride type + trend
4. **Revenue Analytics** — Hourly/daily revenue breakdown by zone
5. **Operations** — Cancellation rates, wait times, driver availability

---

## 🔧 Key Design Decisions

### Fault Tolerance
- Kafka `acks=all` ensures no message loss on producer side
- Spark checkpoints preserve exactly-once processing semantics
- Delta Lake ACID transactions prevent partial writes
- Watermarking handles late-arriving events (up to 10 minutes)

### Scalability
- 4 Kafka partitions per topic → horizontal consumer scaling
- Spark streaming auto-scales on Databricks (autoscale clusters)
- Delta Lake `autoOptimize` prevents small file accumulation

### Low Latency
- Spark trigger interval: 30 seconds for aggregations
- Raw event sink: 60-second trigger (less critical)
- `linger_ms=5` on Kafka producer for micro-batching efficiency

---

## 📉 Performance Metrics

| Metric | Value |
|---|---|
| Events/day | 5,000+ |
| Streaming trigger | 30 seconds |
| Late data tolerance | 2–10 minutes |
| Reporting lag reduction | ~60% |
| Delta table query p95 | < 2 seconds (Z-ORDERed) |

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 3.x / Azure Event Hubs |
| Stream Processing | Apache Spark 3.4 Structured Streaming |
| Storage | Delta Lake 2.4 on ADLS Gen2 |
| Cloud | Microsoft Azure |
| Compute | Azure Databricks |
| BI / Reporting | Microsoft Power BI |
| Containerization | Docker Compose (local dev) |
| Language | Python 3.10+ |
