# Zerobus Architecture Diagrams and Visuals

This document contains descriptions and text-based representations of the architecture diagrams used in the Zerobus workshop.

## Table of Contents
1. [Traditional vs. Zerobus Architecture Comparison](#traditional-vs-zerobus-architecture-comparison)
2. [Zerobus Component Architecture](#zerobus-component-architecture)
3. [Authentication Flow](#authentication-flow)
4. [Data Flow: Ingestion to Analytics](#data-flow-ingestion-to-analytics)
5. [Schema Evolution Process](#schema-evolution-process)
6. [Monitoring and Alerting Architecture](#monitoring-and-alerting-architecture)
7. [Smart Building IoT Demo Architecture](#smart-building-iot-demo-architecture)

---

## Traditional vs. Zerobus Architecture Comparison

### Traditional Streaming Architecture (3+ Hops)

```
┌─────────────┐         ┌──────────────┐         ┌────────────────┐         ┌────────────┐
│             │         │              │         │                │         │            │
│ Application │ ──────> │    Kafka     │ ──────> │ Spark Streaming│ ──────> │ Delta Lake │
│   /IoT      │  HTTP   │   Cluster    │  Pull   │    (24/7)      │  Write  │   (S3/ADLS)│
│             │         │              │         │                │         │            │
└─────────────┘         └──────────────┘         └────────────────┘         └────────────┘
                              │                           │
                              ▼                           ▼
                        ┌──────────┐              ┌───────────┐
                        │ Zookeeper│              │Checkpoint │
                        │ (3 nodes)│              │  Storage  │
                        └──────────┘              └───────────┘

Challenges:
- 3+ infrastructure components to manage
- 5-15 minute end-to-end latency
- Complex capacity planning
- High operational overhead
- Data duplication (Kafka + Lake)
- Multiple failure points
```

### Zerobus Architecture (1 Hop)

```
┌─────────────┐         ┌──────────────────────────────┐         ┌────────────┐
│             │         │                              │         │            │
│ Application │ ──────> │    Zerobus Endpoint         │ ──────> │ Delta Lake │
│   /IoT      │  HTTP   │    (Serverless)             │  Direct │  (Unity Cat)│
│             │  POST   │                              │  Write  │            │
└─────────────┘         └──────────────────────────────┘         └────────────┘
                                     │                                   │
                                     │                                   ▼
                                     │                           ┌────────────┐
                                     │                           │  Query     │
                                     │                           │  Ready     │
                                     │                           │  (<5 sec)  │
                                     │                           └────────────┘
                                     ▼
                              ┌──────────────┐
                              │ Unity Catalog│
                              │  Governance  │
                              └──────────────┘

Benefits:
- Single infrastructure component (serverless)
- 1-5 second end-to-end latency
- Automatic scaling
- Minimal operational overhead
- No data duplication
- Single governance layer
```

**Comparison Metrics:**

| Aspect | Traditional (Kafka) | Zerobus |
|--------|-------------------|---------|
| Infrastructure Components | 5+ (Kafka, Zookeeper, Spark, Storage, Config) | 1 (Serverless endpoint) |
| End-to-End Latency | 5-15 minutes | 1-5 seconds |
| Monthly Infrastructure Cost | $5,000 - $20,000 | $500 - $2,000 |
| Setup Time | 4-8 weeks | Hours to days |
| Operational FTEs | 2-4 engineers | < 0.5 engineer |

---

## Zerobus Component Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         Databricks Workspace                                  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                      Zerobus Ingestion Layer                            │ │
│  │                                                                         │ │
│  │  ┌───────────────┐        ┌──────────────┐        ┌─────────────┐     │ │
│  │  │   HTTP        │        │ Auth &       │        │  Schema     │     │ │
│  │  │   Endpoint    │ ────>  │ Validation   │ ────>  │  Validation │     │ │
│  │  └───────────────┘        └──────────────┘        └─────────────┘     │ │
│  │         │                         │                        │            │ │
│  │         │                         ▼                        │            │ │
│  │         │                  ┌──────────────┐               │            │ │
│  │         │                  │ Unity        │               │            │ │
│  │         │                  │ Catalog      │               │            │ │
│  │         │                  │ Permissions  │               │            │ │
│  │         │                  └──────────────┘               │            │ │
│  │         │                                                 │            │ │
│  │         └─────────────────────────────────────────────────┘            │ │
│  │                                  │                                      │ │
│  └──────────────────────────────────┼──────────────────────────────────────┘ │
│                                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                        Delta Lake Storage                               │ │
│  │                                                                         │ │
│  │  ┌────────────┐      ┌────────────┐      ┌────────────┐               │ │
│  │  │  Bronze    │ ───> │  Silver    │ ───> │   Gold     │               │ │
│  │  │  (Raw)     │      │ (Validated)│      │(Aggregated)│               │ │
│  │  └────────────┘      └────────────┘      └────────────┘               │ │
│  │       │                    │                    │                       │ │
│  └───────┼────────────────────┼────────────────────┼───────────────────────┘ │
│          │                    │                    │                         │
│          └────────────────────┴────────────────────┘                         │
│                               │                                              │
│  ┌────────────────────────────┼────────────────────────────────────────────┐ │
│  │                      Query & Analytics Layer                            │ │
│  │                            │                                            │ │
│  │   ┌──────────────┐   ┌─────┴──────┐   ┌──────────────┐                │ │
│  │   │ Databricks   │   │   Delta    │   │   Streaming  │                │ │
│  │   │    SQL       │   │   Live     │   │    Queries   │                │ │
│  │   └──────────────┘   │   Tables   │   └──────────────┘                │ │
│  │                      └────────────┘                                    │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Key Components:**

1. **HTTP Endpoint**: RESTful API accepting JSON payloads
2. **Authentication Layer**: PAT tokens or service principals
3. **Unity Catalog Integration**: Permissions, governance, lineage
4. **Schema Validation**: Automatic validation against table schema
5. **Delta Lake Storage**: ACID transactions, time travel, optimization
6. **Query Layer**: Immediate access via SQL, DLT, streaming APIs

---

## Authentication Flow

```
┌──────────────┐                                     ┌──────────────────┐
│              │  1. HTTP POST                       │                  │
│ Application  │  + Bearer Token                     │  Zerobus         │
│              │  + JSON Events                      │  Endpoint        │
│              │ ─────────────────────────────────>  │                  │
└──────────────┘                                     └────────┬─────────┘
                                                              │
                                                              │ 2. Validate Token
                                                              │
                                                              ▼
                                     ┌────────────────────────────────────┐
                                     │    Unity Catalog                   │
                                     │                                    │
                                     │  ┌──────────────────────────────┐  │
                                     │  │ Token Validation:            │  │
                                     │  │  - Is token valid?           │  │
                                     │  │  - Is token expired?         │  │
                                     │  │  - Which principal?          │  │
                                     │  └──────────────────────────────┘  │
                                     │                │                   │
                                     │                ▼                   │
                                     │  ┌──────────────────────────────┐  │
                                     │  │ Permission Check:            │  │
                                     │  │  - INSERT on target table?   │  │
                                     │  │  - USAGE on catalog/schema?  │  │
                                     │  └──────────────────────────────┘  │
                                     └────────────────┬───────────────────┘
                                                      │
                    ┌─────────────────────────────────┴──────────────────┐
                    │                                                    │
                    ▼ 3a. Authorized                                     ▼ 3b. Unauthorized
        ┌───────────────────────┐                           ┌────────────────────────┐
        │ Validate Schema       │                           │  Return 401            │
        │ Write to Delta Lake   │                           │  Unauthorized          │
        │ Return 200 OK         │                           └────────────────────────┘
        └───────────────────────┘
```

**Authentication Methods:**

1. **Personal Access Token (PAT)**
   - For development and testing
   - User-specific permissions
   - Easy to generate: User Settings → Developer → Access Tokens

2. **Service Principal**
   - For production applications
   - Machine-to-machine authentication
   - Better security and audit trail
   - Can be rotated without user impact

**Security Best Practices:**

✅ Store tokens in secret managers (Databricks Secrets, Azure Key Vault, AWS Secrets Manager)  
✅ Use service principals for production  
✅ Set appropriate token expiration (90 days max)  
✅ Apply principle of least privilege (only INSERT permission needed)  
✅ Rotate tokens regularly  
✅ Monitor token usage in audit logs  

❌ Never hardcode tokens in source code  
❌ Never commit tokens to version control  
❌ Never share tokens via email or chat  

---

## Data Flow: Ingestion to Analytics

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                          Real-Time Event Flow                                  │
└────────────────────────────────────────────────────────────────────────────────┘

Step 1: Event Generation
┌─────────────┐
│ IoT Sensor  │ ─┐
└─────────────┘  │
                 │
┌─────────────┐  │         {"sensor_id": "SENSOR-001",
│ Web App     │ ─┤          "timestamp": "2026-01-10T10:00:00Z",
└─────────────┘  │  ──────> "temperature": 22.5,
                 │          "humidity": 45.0,
┌─────────────┐  │          "location": "Building A"}
│ Mobile App  │ ─┘
└─────────────┘

Step 2: HTTP POST to Zerobus (< 100ms)
       POST https://<workspace>/api/2.0/zerobus/v1/ingest/<catalog>/<schema>/<connection>
       Authorization: Bearer dapi...
       Content-Type: application/json
       
       [{"sensor_id": "SENSOR-001", ...}, {...}, ...]

Step 3: Ingestion & Validation (< 500ms)
┌──────────────────────────────────────┐
│ Zerobus Processing:                  │
│  1. Authenticate request             │
│  2. Validate JSON structure          │
│  3. Check schema compatibility       │
│  4. Write to Delta Lake (ACID)       │
│  5. Return 200 OK                    │
└──────────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────┐
│  Bronze Layer (Raw Data)             │
│  Table: sensor_events_bronze         │
│  - All events, immutable             │
│  - Partitioned by date               │
│  - Available for querying            │
└──────────────────────────────────────┘
                │
                │ Delta Live Tables
                ▼
┌──────────────────────────────────────┐
│  Silver Layer (Validated)            │
│  Table: sensor_events_silver         │
│  - Data quality checks applied       │
│  - Enriched with derived fields      │
│  - Invalid data quarantined          │
└──────────────────────────────────────┘
                │
                │ Aggregations
                ▼
┌──────────────────────────────────────┐
│  Gold Layer (Analytics-Ready)        │
│  Table: sensor_metrics_gold          │
│  - Hourly/daily aggregations         │
│  - Business KPIs                     │
│  - Optimized for dashboards          │
└──────────────────────────────────────┘
                │
                ▼
┌──────────────────────────────────────┐
│  Consumption Layer                   │
│  - Databricks SQL Dashboards         │
│  - Real-time alerts                  │
│  - ML feature serving                │
│  - BI tool integration               │
└──────────────────────────────────────┘

Total Latency: 1-5 seconds (event generation → dashboard)
```

**Performance Characteristics:**

- **HTTP Request**: 50-100ms
- **Authentication**: 10-50ms
- **Schema Validation**: 10-20ms
- **Delta Write**: 200-500ms
- **Query Availability**: Immediate (ACID guarantees)
- **Bronze → Silver**: 10-30 seconds (streaming)
- **Silver → Gold**: 30-60 seconds (streaming)

---

## Schema Evolution Process

```
Initial Schema (v1)
┌─────────────────────────────────────────┐
│ Table: sensor_events                    │
├─────────────────────────────────────────┤
│ sensor_id         STRING                │
│ timestamp         TIMESTAMP             │
│ temperature       DOUBLE                │
│ humidity          DOUBLE                │
│ pressure          DOUBLE                │
│ location          STRING                │
│ ingestion_time    TIMESTAMP             │
└─────────────────────────────────────────┘
         │
         │ New Requirements:
         │ - Track battery level
         │ - Monitor signal strength  
         │ - Store firmware version
         │
         ▼
Evolution Strategy 1: ALTER TABLE (Recommended)
┌──────────────────────────────────────────────────────────┐
│ ALTER TABLE sensor_events ADD COLUMNS (                 │
│   battery_level     DOUBLE,                              │
│   signal_strength   INT,                                 │
│   firmware_version  STRING                               │
│ );                                                       │
└──────────────────────────────────────────────────────────┘
         │
         ▼
Updated Schema (v2)
┌─────────────────────────────────────────┐
│ Table: sensor_events                    │
├─────────────────────────────────────────┤
│ sensor_id         STRING                │
│ timestamp         TIMESTAMP             │
│ temperature       DOUBLE                │
│ humidity          DOUBLE                │
│ pressure          DOUBLE                │
│ location          STRING                │
│ ingestion_time    TIMESTAMP             │
│ battery_level     DOUBLE       ← NEW   │
│ signal_strength   INT          ← NEW   │
│ firmware_version  STRING       ← NEW   │
└─────────────────────────────────────────┘

Backwards Compatibility:
┌──────────────────────────────────────────────────────────┐
│ Old Events (v1):                                         │
│ - New columns will be NULL                               │
│ - All old data still queryable                           │
│ - No data migration needed                               │
│                                                          │
│ New Events (v2):                                         │
│ - All columns populated                                  │
│ - Seamless ingestion                                     │
│ - No pipeline changes required                           │
└──────────────────────────────────────────────────────────┘

Handling Queries Across Schema Versions:
SELECT 
  sensor_id,
  temperature,
  COALESCE(battery_level, 100.0) as battery_level,  -- Default for old records
  COALESCE(firmware_version, 'unknown') as firmware_version
FROM sensor_events
WHERE timestamp >= '2026-01-01';
```

**Schema Evolution Patterns:**

| Change Type | Zerobus Support | Migration Required |
|------------|----------------|-------------------|
| Add column | ✅ Yes (NULL for old data) | No |
| Remove column | ⚠️ Column remains, stop populating | No |
| Rename column | ✅ Yes (with column mapping) | No |
| Change type (widen) | ✅ int→long, float→double | No |
| Change type (narrow) | ❌ Not supported | Yes (recreate table) |
| Add NOT NULL constraint | ⚠️ Only for new data | No |
| Change partitioning | ❌ Not supported | Yes (recreate table) |

---

## Monitoring and Alerting Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                     Monitoring & Alerting System                              │
└───────────────────────────────────────────────────────────────────────────────┘

Data Sources:
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│ Delta Table     │    │ System Tables    │    │ Delta History       │
│ Metrics         │    │ (if available)   │    │ & Table Details     │
└────────┬────────┘    └────────┬─────────┘    └──────────┬──────────┘
         │                      │                           │
         └──────────────────────┴───────────────────────────┘
                                │
                                ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         Monitoring Queries                                    │
│                                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────┐             │
│  │ Volume Metrics  │  │ Latency Metrics │  │ Quality Metrics  │             │
│  │ - Events/min    │  │ - p50, p95, p99 │  │ - Missing fields │             │
│  │ - Active sensors│  │ - Ingestion lag │  │ - Invalid values │             │
│  │ - Data growth   │  │ - Query time    │  │ - Outliers       │             │
│  └─────────────────┘  └─────────────────┘  └──────────────────┘             │
└───────────────────────────────────────────┬───────────────────────────────────┘
                                            │
                                            ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                          Alert Conditions                                     │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ CRITICAL Alerts:                                                       │  │
│  │  • No data for > 15 minutes                                           │  │
│  │  • p95 latency > 60 seconds                                           │  │
│  │  • Error rate > 10%                                                   │  │
│  │  • Table write failures                                               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ WARNING Alerts:                                                        │  │
│  │  • p95 latency > 10 seconds                                           │  │
│  │  • Invalid data > 5%                                                  │  │
│  │  • Sensor offline > 30 minutes                                        │  │
│  │  • Low battery devices > 10                                           │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────┬───────────────────────────────────┘
                                            │
                                            ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                       Notification Channels                                   │
│                                                                               │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                │
│   │    Email     │     │    Slack     │     │  PagerDuty   │                │
│   │              │     │              │     │              │                │
│   │ Daily digest │     │ Real-time    │     │ Critical     │                │
│   │ Weekly report│     │ alerts       │     │ only         │                │
│   └──────────────┘     └──────────────┘     └──────────────┘                │
└───────────────────────────────────────────────────────────────────────────────┘
                                            │
                                            ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                        Dashboards & Visualization                             │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │  Databricks SQL Dashboard (Auto-refresh every 60s)                     │ │
│  │                                                                         │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │ │
│  │  │ Ingestion    │  │ Latency      │  │ Data Quality │                 │ │
│  │  │ Rate         │  │ Trends       │  │ Score        │                 │ │
│  │  │ (Line Chart) │  │ (Line Chart) │  │ (Gauge)      │                 │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                 │ │
│  │                                                                         │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                 │ │
│  │  │ Active       │  │ Alert        │  │ Sensor       │                 │ │
│  │  │ Sensors      │  │ Summary      │  │ Health Map   │                 │ │
│  │  │ (Table)      │  │ (Table)      │  │ (Heatmap)    │                 │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Key Monitoring Metrics:**

1. **Volume**: Events/second, events/minute, cumulative events
2. **Latency**: Ingestion lag (event time → ingestion time), p50/p95/p99
3. **Quality**: Missing fields, invalid values, schema violations
4. **Health**: Offline sensors, battery status, signal strength
5. **Performance**: Query response time, table optimization status
6. **Cost**: Storage growth, compute usage, API calls

---

## Smart Building IoT Demo Architecture

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                      Smart Building IoT Solution                              │
└───────────────────────────────────────────────────────────────────────────────┘

Physical Layer:
┌─────────────────────────────────────────────────────────────────────────────┐
│  Building A              Building B              Building C                 │
│  ┌──────────┐           ┌──────────┐           ┌──────────┐                │
│  │ Floor 10 │           │ Floor 8  │           │ Floor 5  │                │
│  │ 15sensors│           │ 12sensors│           │ 10sensors│                │
│  ├──────────┤           ├──────────┤           ├──────────┤                │
│  │ Floor 9  │           │ Floor 7  │           │ Floor 4  │                │
│  │ ...      │           │ ...      │           │ ...      │                │
│  │ Floor 1  │           │ Floor 1  │           │ Floor 1  │                │
│  └──────────┘           └──────────┘           └──────────┘                │
│     │                        │                       │                      │
│     └────────────────────────┴───────────────────────┘                      │
│                              │                                              │
│                    200 IoT Sensors Total                                    │
│                    (Temp, Humidity, CO2, Air Quality)                       │
└──────────────────────────────────────────────────┬──────────────────────────┘
                                                   │
                                    HTTP POST (Batched, every 30s)
                                                   │
                                                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                         Databricks Platform                                   │
│                                                                               │
│  Ingestion Layer:                                                            │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Zerobus Endpoint: /api/2.0/zerobus/v1/ingest/.../smart_building       │  │
│  │ - Accepts JSON batches                                                 │  │
│  │ - Rate: 200-500 events/min                                            │  │
│  │ - Latency: < 2 seconds                                                 │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                   │                                           │
│                                   ▼                                           │
│  Storage Layer:                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Bronze: sensor_telemetry_bronze                                        │  │
│  │ - Raw events, immutable                                                │  │
│  │ - Partitioned by building_id, date                                     │  │
│  │ - Retention: 90 days                                                   │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                   │                                           │
│                                   ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Silver: sensor_telemetry_silver                                        │  │
│  │ - Validated & enriched                                                 │  │
│  │ - Data quality flags                                                   │  │
│  │ - Alert indicators                                                     │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                   │                                           │
│                                   ▼                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Gold: building_metrics_gold                                            │  │
│  │ - Hourly aggregations per building/floor                               │  │
│  │ - KPIs: avg temp, CO2 levels, occupancy                               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  Analytics & Monitoring:                                                     │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Real-time Dashboard                                                    │  │
│  │ - Building overview (temp, humidity, CO2)                              │  │
│  │ - Floor-by-floor heatmaps                                             │  │
│  │ - Sensor health monitoring                                             │  │
│  │ - Energy efficiency metrics                                            │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ Alerting System                                                        │  │
│  │ - High CO2 (>1500 ppm) → Facilities team                             │  │
│  │ - Temperature out of range → HVAC alert                               │  │
│  │ - Sensor offline → Maintenance ticket                                 │  │
│  │ - Low battery → Replacement schedule                                  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                           Business Outcomes                                   │
│                                                                               │
│  • Improved occupant comfort and productivity                                │
│  • 15-20% energy cost savings through optimization                           │
│  • Predictive maintenance reduces downtime                                   │
│  • Regulatory compliance (air quality standards)                             │
│  • ESG reporting (carbon footprint tracking)                                 │
└───────────────────────────────────────────────────────────────────────────────┘
```

**Use Case Metrics:**

- **Sensors**: 200 devices across 3 buildings
- **Event Rate**: 6,000 events/hour (10 readings/hour per sensor)
- **Data Volume**: ~500MB/day (~15GB/month)
- **Latency**: < 5 seconds (sensor → dashboard)
- **Retention**: 90 days hot, 1 year archive
- **Dashboard Refresh**: 30-60 seconds
- **Alert Latency**: < 60 seconds

---

## Conclusion

These architecture diagrams illustrate the simplicity, performance, and operational benefits of Zerobus compared to traditional streaming architectures. The elimination of intermediate message brokers, combined with serverless scaling and Unity Catalog governance, enables organizations to build production-grade streaming applications with minimal infrastructure and operational overhead.

For more detailed information, refer to the workshop notebooks and hands-on labs.
