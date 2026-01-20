# Workshop Architecture Diagrams

This directory contains architecture diagrams and visual aids for the Advanced Lakehouse with PostgreSQL workshop.

## Architecture Diagrams

### Federation Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Unity Catalog                                 │
│                  (Centralized Governance Layer)                      │
│                                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐             │
│  │   Catalog 1  │  │   Catalog 2  │  │   Catalog 3  │             │
│  │  (Delta Lake)│  │ (PostgreSQL) │  │   (MySQL)    │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘             │
└─────────┼──────────────────┼──────────────────┼───────────────────┘
          │                  │                  │
          │                  │                  │
┌─────────▼──────────┐  ┌────▼─────────────┐  ┌▼──────────────────┐
│   Delta Tables     │  │  Foreign Catalog │  │  Foreign Catalog  │
│   (Lakehouse)      │  │  Connection      │  │  Connection       │
│                    │  │                  │  │                   │
│  - Native Storage  │  │  - PostgreSQL    │  │  - MySQL          │
│  - ACID            │  │  - External DB   │  │  - External DB    │
│  - Time Travel     │  │  - Live Queries  │  │  - Live Queries   │
└────────────────────┘  └──────────────────┘  └───────────────────┘
```

### CDC Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Source Database (PostgreSQL)                   │
│                                                                   │
│  ┌─────────────┐        ┌──────────────────┐                    │
│  │  Tables     │        │  Write-Ahead Log │                    │
│  │  (OLTP)     │───────►│  (WAL)           │                    │
│  └─────────────┘        └─────────┬────────┘                    │
└──────────────────────────────────────┼───────────────────────────┘
                                       │
                                       │ Read WAL
                                       ▼
                          ┌────────────────────────┐
                          │  Debezium Connector    │
                          │  - Captures changes    │
                          │  - Converts to events  │
                          │  - Publishes to Kafka  │
                          └───────────┬────────────┘
                                      │
                                      │ CDC Events
                                      ▼
                          ┌────────────────────────┐
                          │    Apache Kafka        │
                          │    - Topic: orders     │
                          │    - Topic: customers  │
                          │    - Topic: inventory  │
                          └───────────┬────────────┘
                                      │
                                      │ Consume Events
                                      ▼
                          ┌────────────────────────┐
                          │  Delta Live Tables     │
                          │  - Streaming ingestion │
                          │  - Transformations     │
                          │  - SCD Type 2          │
                          └───────────┬────────────┘
                                      │
                                      ▼
                          ┌────────────────────────┐
                          │     Delta Lake         │
                          │  Bronze → Silver → Gold│
                          │  (Analytics/ML)        │
                          └────────────────────────┘
```

### Saga Pattern Flow

```
Order Processing Saga:

┌─────────────────────────────────────────────────────────────┐
│                      Saga Orchestrator                       │
└───┬─────────────┬─────────────┬─────────────┬──────────────┘
    │             │             │             │
    ▼             ▼             ▼             ▼
┌────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐
│Reserve │  │ Process │  │  Create  │  │  Update  │
│Inventory  │ Payment │  │ Shipping │  │  Status  │
└───┬────┘  └────┬────┘  └─────┬────┘  └────┬─────┘
    │            │             │             │
    │ Success    │ Success     │ Success     │ Success
    ▼            ▼             ▼             ▼
┌────────────────────────────────────────────────────┐
│                 Transaction Complete                │
└────────────────────────────────────────────────────┘

On Failure (Compensation Flow):
    │            │             │             │
    │ Failure    ◄─────────────◄─────────────┘
    ▼            ▼             ▼
┌────────┐  ┌─────────┐  ┌──────────┐
│Release │  │ Refund  │  │  Cancel  │
│Inventory  │ Payment │  │ Shipping │
└────────┘  └─────────┘  └──────────┘
```

### High Availability Architecture

```
Active-Passive Configuration:
┌─────────────────────────────────────────────┐
│              Primary Region                  │
│  ┌─────────────────────────────────────┐   │
│  │       PostgreSQL (Active)           │   │
│  │  - Handles all read/write traffic   │   │
│  └────────────────┬────────────────────┘   │
└───────────────────┼─────────────────────────┘
                    │
                    │ Replication
                    ▼
┌─────────────────────────────────────────────┐
│           Secondary Region                   │
│  ┌─────────────────────────────────────┐   │
│  │     PostgreSQL (Standby)            │   │
│  │  - Ready for automatic failover     │   │
│  └─────────────────────────────────────┘   │
└─────────────────────────────────────────────┘

Active-Active Configuration:
┌───────────────┐        ┌───────────────┐
│   Region A    │◄──────►│   Region B    │
│   (Active)    │  Sync  │   (Active)    │
│               │        │               │
│ - Read/Write  │        │ - Read/Write  │
│ - Load Bal.   │        │ - Load Bal.   │
└───────────────┘        └───────────────┘
```

### CQRS Pattern

```
┌──────────────────────────────────────────────┐
│                Application                   │
└──────────────┬───────────────────┬───────────┘
               │                   │
               ▼                   ▼
        ┌─────────────┐    ┌──────────────┐
        │  Commands   │    │   Queries    │
        │  (Writes)   │    │   (Reads)    │
        └──────┬──────┘    └──────┬───────┘
               │                   │
               ▼                   ▼
        ┌─────────────┐    ┌──────────────┐
        │   OLTP DB   │    │   OLAP DB    │
        │(Normalized) │────►│(Denormalized)│
        │             │sync │              │
        └─────────────┘    └──────────────┘
```

### Event Sourcing Architecture

```
Event Store (Source of Truth):
┌─────────────────────────────────────────┐
│ Event 1: AccountCreated                 │
│   account_id: A001                      │
│   initial_balance: 1000                 │
├─────────────────────────────────────────┤
│ Event 2: MoneyDeposited                 │
│   account_id: A001                      │
│   amount: 500                           │
├─────────────────────────────────────────┤
│ Event 3: MoneyWithdrawn                 │
│   account_id: A001                      │
│   amount: 200                           │
└────────────┬────────────────────────────┘
             │
             │ Event Replay
             ▼
┌─────────────────────────────────────────┐
│         Read Model Projections          │
│                                         │
│  ┌──────────────┐   ┌───────────────┐  │
│  │   Balance    │   │   Audit Log   │  │
│  │   View       │   │   View        │  │
│  └──────────────┘   └───────────────┘  │
│                                         │
│  ┌──────────────┐   ┌───────────────┐  │
│  │ Transaction  │   │   Analytics   │  │
│  │   History    │   │   View        │  │
│  └──────────────┘   └───────────────┘  │
└─────────────────────────────────────────┘
```

### Medallion Architecture with Federation

```
┌─────────────────────────────────────────────┐
│           Operational Layer (OLTP)          │
│  - Normalized schema                        │
│  - Row-level updates                        │
│  - ACID transactions                        │
│  - PostgreSQL / Lakebase                    │
└────────────────┬────────────────────────────┘
                 │ CDC / Federation
                 ▼
┌─────────────────────────────────────────────┐
│         Bronze Layer (Raw Events)           │
│  - Immutable append-only                    │
│  - Schema-on-read                           │
│  - Full fidelity                            │
└────────────────┬────────────────────────────┘
                 │ Cleansing / Enrichment
                 ▼
┌─────────────────────────────────────────────┐
│       Silver Layer (Cleaned, SCD Type 2)    │
│  - Validated data                           │
│  - Conformed dimensions                     │
│  - Historical tracking                      │
└────────────────┬────────────────────────────┘
                 │ Aggregation / Business Logic
                 ▼
┌─────────────────────────────────────────────┐
│     Gold Layer (Aggregated, Business)       │
│  - Business-level aggregates                │
│  - Denormalized for performance             │
│  - Optimized for BI/ML                      │
└─────────────────────────────────────────────┘
```

## Performance Optimization Diagrams

### Predicate Pushdown

```
Without Pushdown:
PostgreSQL                         Databricks
   │                                   │
   │  Transfer 1M rows                │
   ├──────────────────────────────────►│
   │                                   │
   │                         Filter 1M rows
   │                         Return 100K rows

With Pushdown:
PostgreSQL                         Databricks
   │                                   │
   │  Filter locally (fast)            │
   │  Transfer 100K rows               │
   ├──────────────────────────────────►│
   │                                   │
   │                         Receive 100K rows
```

### Query Caching Layers

```
┌──────────────────────────────────────┐
│         Application Layer            │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│    Query Result Cache (Memory)       │
│         - Instant response           │
│         - Seconds TTL                │
└──────────────┬───────────────────────┘
               │ Cache Miss
               ▼
┌──────────────────────────────────────┐
│  Materialized View (Delta Lake)      │
│         - Pre-aggregated             │
│         - Minutes/Hours refresh      │
└──────────────┬───────────────────────┘
               │ Cache Miss
               ▼
┌──────────────────────────────────────┐
│   Federated Query (PostgreSQL)       │
│         - Live data                  │
│         - Network latency            │
└──────────────────────────────────────┘
```

## Security Architecture

### Row-Level Security

```
User Request
    │
    ▼
┌────────────────────────┐
│   Unity Catalog        │
│   - Check user groups  │
│   - Apply RLS filters  │
└──────────┬─────────────┘
           │
           ▼
┌────────────────────────┐
│   Query Rewrite        │
│   WHERE region IN (    │
│     user_regions       │
│   )                    │
└──────────┬─────────────┘
           │
           ▼
┌────────────────────────┐
│   Execute Query        │
│   - Only visible rows  │
└────────────────────────┘
```

### Data Masking

```
Raw Data:
┌─────────────────────────────────────┐
│ email: alice@example.com            │
│ ssn: 123-45-6789                    │
│ phone: 555-0123                     │
└─────────────────────────────────────┘
           │
           ▼ Apply Masking Functions
           │
┌──────────┴──────────────────────────┐
│                                     │
▼ Regular User                       ▼ Admin User
┌────────────────────┐    ┌────────────────────────┐
│ email: al***@      │    │ email: alice@          │
│        example.com │    │        example.com     │
│ ssn: ***-**-6789   │    │ ssn: 123-45-6789       │
│ phone: ***-0123    │    │ phone: 555-0123        │
└────────────────────┘    └────────────────────────┘
```

## Data Flow Patterns

### Hybrid Query Pattern

```
Query Request
    │
    ▼
┌────────────────────────┐
│   Query Analyzer       │
│   - Check data age     │
│   - Route query        │
└──────────┬─────────────┘
           │
    ┌──────┴────────┐
    │               │
    ▼               ▼
┌─────────┐    ┌──────────┐
│  Hot    │    │  Cold    │
│  Data   │    │  Data    │
│ (Live   │    │ (Cache)  │
│  PG)    │    │ (Delta)  │
└────┬────┘    └─────┬────┘
     │               │
     └───────┬───────┘
             ▼
    ┌────────────────┐
    │  Combined      │
    │  Result        │
    └────────────────┘
```

## Notes

All diagrams in this document use ASCII art for maximum compatibility. For presentations, these can be converted to visual diagrams using tools like:
- draw.io
- Mermaid
- PlantUML
- Lucidchart

Each diagram corresponds to concepts taught in the workshop lectures and labs.
