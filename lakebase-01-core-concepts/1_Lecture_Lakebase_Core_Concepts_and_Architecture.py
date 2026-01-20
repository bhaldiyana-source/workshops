# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Core Concepts and Architecture
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC - Explain what Lakebase is and how it fits within the Databricks ecosystem
# MAGIC - Describe the serverless architecture with separated compute and storage
# MAGIC - Identify key features including Unity Catalog integration, zero-copy branching, and AI capabilities
# MAGIC - Recognize use cases for operational workloads, real-time applications, and AI agents
# MAGIC - Compare Lakebase OLTP with traditional analytical Lakehouse workloads
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Basic understanding of databases and SQL
# MAGIC - Familiarity with Databricks and Unity Catalog concepts
# MAGIC - Understanding of OLTP vs OLAP workloads

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakebase?
# MAGIC
# MAGIC **Lakebase** is a fully managed, **PostgreSQL-compatible operational database** developed by Databricks. It is designed to unify analytics and operations on the Databricks Lakehouse Platform, enabling developers to build AI-driven applications and agents more efficiently.
# MAGIC
# MAGIC ### Key Characteristics
# MAGIC
# MAGIC - **Fully Managed Service**: No infrastructure management required
# MAGIC - **PostgreSQL Compatible**: Works with existing PostgreSQL tools, drivers, and extensions
# MAGIC - **Cloud-Native**: Built for modern multi-cloud environments
# MAGIC - **AI-Ready**: Optimized for AI applications and real-time feature serving
# MAGIC - **Lakehouse Integrated**: Seamlessly connects with Delta Lake and Unity Catalog
# MAGIC
# MAGIC ### Why Lakebase?
# MAGIC
# MAGIC Traditional architectures require separate systems for:
# MAGIC - **Analytics** (data warehouses, data lakes)
# MAGIC - **Operations** (transactional databases)
# MAGIC
# MAGIC This leads to:
# MAGIC - Complex ETL pipelines
# MAGIC - Data silos
# MAGIC - Inconsistent governance
# MAGIC - Delayed insights
# MAGIC
# MAGIC **Lakebase eliminates these challenges** by bringing operational workloads directly into the Lakehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Architecture
# MAGIC
# MAGIC ### Serverless Design with Separated Compute and Storage
# MAGIC
# MAGIC Lakebase employs a **cloud-native architecture** that decouples compute and storage resources:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                  Client Applications                     │
# MAGIC │        (Python, JDBC, SQL Tools, AI Agents)             │
# MAGIC └─────────────────────────┬───────────────────────────────┘
# MAGIC                           │
# MAGIC                           │ PostgreSQL Protocol
# MAGIC                           │
# MAGIC ┌─────────────────────────▼───────────────────────────────┐
# MAGIC │              Lakebase Compute Layer                      │
# MAGIC │   • Serverless PostgreSQL Engine                        │
# MAGIC │   • Auto-scaling (including scale-to-zero)              │
# MAGIC │   • High concurrency (>10,000 QPS)                      │
# MAGIC │   • Low latency (<10ms)                                 │
# MAGIC └─────────────────────────┬───────────────────────────────┘
# MAGIC                           │
# MAGIC ┌─────────────────────────▼───────────────────────────────┐
# MAGIC │              Lakebase Storage Layer                      │
# MAGIC │   • Durable object storage                              │
# MAGIC │   • Zero-copy branching                                 │
# MAGIC │   • Point-in-time recovery                              │
# MAGIC │   • Encrypted at rest                                   │
# MAGIC └─────────────────────────┬───────────────────────────────┘
# MAGIC                           │
# MAGIC ┌─────────────────────────▼───────────────────────────────┐
# MAGIC │            Unity Catalog Integration                     │
# MAGIC │   • Unified governance                                  │
# MAGIC │   • Access control                                      │
# MAGIC │   • Lineage tracking                                    │
# MAGIC │   • Delta Lake synchronization                          │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Architecture Benefits
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | **Separated Compute/Storage** | Scale compute and storage independently based on workload needs |
# MAGIC | **Serverless** | Automatic scaling with no manual tuning; pay only for what you use |
# MAGIC | **PostgreSQL Compatible** | Use familiar tools, drivers (psycopg3, JDBC), and extensions |
# MAGIC | **High Performance** | Sub-10ms latency, >10,000 queries per second |
# MAGIC | **High Availability** | Built-in redundancy and automatic failover |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Features of Lakebase
# MAGIC
# MAGIC ### 1. Unity Catalog Integration
# MAGIC
# MAGIC Lakebase databases are **registered in Unity Catalog**, providing:
# MAGIC
# MAGIC - **Unified Governance**: Single security model across analytical and operational data
# MAGIC - **Access Control**: Fine-grained permissions using GRANT statements
# MAGIC - **Data Lineage**: Track data flow between operational and analytical systems
# MAGIC - **Discovery**: Find and catalog operational databases alongside Delta tables
# MAGIC
# MAGIC ### 2. Zero-Copy Branching
# MAGIC
# MAGIC Create **instant database clones** without duplicating data:
# MAGIC
# MAGIC - **Development Environments**: Test schema changes safely
# MAGIC - **CI/CD Pipelines**: Run automated tests on production-like data
# MAGIC - **Agent Development**: Let AI agents experiment in isolated branches
# MAGIC - **Copy-on-Write**: Only modified data is stored separately
# MAGIC
# MAGIC Example use case:
# MAGIC ```
# MAGIC Production DB ──┬──> Branch: Development (for testing)
# MAGIC                 ├──> Branch: Staging (pre-production)
# MAGIC                 └──> Branch: Agent Sandbox (AI experiments)
# MAGIC ```
# MAGIC
# MAGIC ### 3. AI-Ready Capabilities
# MAGIC
# MAGIC Lakebase is optimized for AI workloads:
# MAGIC
# MAGIC - **pgvector Extension**: Store and query vector embeddings for similarity search
# MAGIC - **PostGIS Extension**: Handle geospatial data for location-based AI
# MAGIC - **Real-Time Feature Serving**: Low-latency access to ML features
# MAGIC - **Agent Integration**: Support for autonomous AI agents with database access
# MAGIC - **Fast Startup**: Instances launch in under 1 second
# MAGIC
# MAGIC ### 4. Bidirectional Sync with Delta Lake
# MAGIC
# MAGIC Seamlessly move data between operational and analytical systems:
# MAGIC
# MAGIC - **Snapshot Sync**: One-time full table copy
# MAGIC - **Triggered Sync**: Manual or scheduled incremental updates
# MAGIC - **Continuous Sync**: Real-time streaming with Change Data Capture (CDC)
# MAGIC - **No ETL Required**: Automatic synchronization managed by Databricks
# MAGIC
# MAGIC ### 5. Enterprise Security and Observability
# MAGIC
# MAGIC - **Encryption**: Data encrypted at rest and in transit
# MAGIC - **Network Security**: PrivateLink, IP access control lists
# MAGIC - **Monitoring**: Built-in metrics (TPS, latency, compute usage)
# MAGIC - **Point-in-Time Recovery**: Restore to any moment in the past
# MAGIC - **Automated Backups**: Continuous backup with configurable retention

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases for Lakebase
# MAGIC
# MAGIC ### 1. Operational Workloads
# MAGIC
# MAGIC Replace traditional OLTP databases for:
# MAGIC
# MAGIC - **Customer-facing applications**: E-commerce, SaaS platforms
# MAGIC - **Real-time inventory management**: Track stock levels with low latency
# MAGIC - **Session storage**: User sessions, shopping carts
# MAGIC - **Metadata catalogs**: Store configuration and metadata
# MAGIC
# MAGIC **Example**: An e-commerce platform uses Lakebase to manage:
# MAGIC - Customer orders (transactional)
# MAGIC - Product catalog (frequently updated)
# MAGIC - Inventory levels (real-time)
# MAGIC
# MAGIC While simultaneously syncing to Delta Lake for:
# MAGIC - Sales analytics
# MAGIC - Customer behavior analysis
# MAGIC - ML model training
# MAGIC
# MAGIC ### 2. Real-Time Applications
# MAGIC
# MAGIC Build applications requiring immediate data access:
# MAGIC
# MAGIC - **Dashboards**: Operational dashboards with <10ms query response
# MAGIC - **APIs**: Backend for REST APIs serving mobile/web applications
# MAGIC - **IoT Applications**: Ingest and query sensor data in real-time
# MAGIC - **Gaming**: Player state, leaderboards, game configuration
# MAGIC
# MAGIC ### 3. AI Agents and ML Feature Stores
# MAGIC
# MAGIC Power intelligent applications:
# MAGIC
# MAGIC - **Feature Store**: Serve ML features with sub-10ms latency
# MAGIC - **AI Agents**: Provide agents with operational data access
# MAGIC - **Vector Search**: Store embeddings for semantic search
# MAGIC - **Real-Time Personalization**: Update user preferences instantly
# MAGIC - **RAG Applications**: Store and query document embeddings with pgvector
# MAGIC
# MAGIC **Example**: A recommendation engine:
# MAGIC 1. Stores user features in Lakebase (profile, recent actions)
# MAGIC 2. ML model queries features in real-time (<10ms)
# MAGIC 3. Returns personalized recommendations
# MAGIC 4. Updates user interaction history immediately
# MAGIC 5. Syncs to Delta Lake for model retraining

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase vs Traditional Architectures
# MAGIC
# MAGIC ### Traditional Architecture (Separate OLTP and OLAP)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐
# MAGIC │  OLTP Database  │ ──ETL Pipeline──┐
# MAGIC │  (PostgreSQL)   │                 │
# MAGIC └─────────────────┘                 │
# MAGIC                                     ▼
# MAGIC                              ┌──────────────┐
# MAGIC                              │  Data Lake   │
# MAGIC                              │ (Analytics)  │
# MAGIC                              └──────────────┘
# MAGIC
# MAGIC Problems:
# MAGIC • Complex ETL pipelines
# MAGIC • Data latency (hours/days)
# MAGIC • Inconsistent governance
# MAGIC • Duplicate data management
# MAGIC • Higher costs
# MAGIC ```
# MAGIC
# MAGIC ### Lakebase Architecture (Unified)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │         Databricks Lakehouse Platform           │
# MAGIC │                                                 │
# MAGIC │  ┌──────────────┐      ┌──────────────┐       │
# MAGIC │  │   Lakebase   │◄────►│  Delta Lake  │       │
# MAGIC │  │    (OLTP)    │ Sync │  (Analytics) │       │
# MAGIC │  └──────────────┘      └──────────────┘       │
# MAGIC │          │                     │               │
# MAGIC │          └─────────┬───────────┘               │
# MAGIC │                    │                           │
# MAGIC │            ┌───────▼────────┐                 │
# MAGIC │            │ Unity Catalog  │                 │
# MAGIC │            │  (Governance)  │                 │
# MAGIC │            └────────────────┘                 │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC
# MAGIC Benefits:
# MAGIC ✓ Real-time data sync
# MAGIC ✓ Unified governance
# MAGIC ✓ Simplified architecture
# MAGIC ✓ Lower total cost
# MAGIC ✓ Single platform
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: OLTP (Lakebase) vs OLAP (Lakehouse)
# MAGIC
# MAGIC | Aspect | Lakebase (OLTP) | Delta Lake (OLAP) |
# MAGIC |--------|-----------------|-------------------|
# MAGIC | **Workload Type** | Transactional | Analytical |
# MAGIC | **Query Pattern** | Point lookups, small updates | Aggregations, scans, joins |
# MAGIC | **Latency** | <10ms | Seconds to minutes |
# MAGIC | **Concurrency** | >10,000 QPS | Hundreds of queries |
# MAGIC | **Data Volume** | Gigabytes to terabytes | Terabytes to petabytes |
# MAGIC | **Write Pattern** | Frequent small writes | Batch writes |
# MAGIC | **Read Pattern** | Single/few rows | Many rows at once |
# MAGIC | **Use Cases** | Applications, APIs, agents | Reporting, ML training, BI |
# MAGIC | **Access Method** | PostgreSQL protocol | Spark SQL, Python, SQL |
# MAGIC | **Indexing** | Primary keys, B-tree indexes | Partition pruning, Z-ordering |
# MAGIC | **ACID Support** | Full ACID transactions | ACID with Delta protocol |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Use Lakebase when you need:**
# MAGIC - Sub-second query latency
# MAGIC - High concurrency from many clients
# MAGIC - Frequent small updates
# MAGIC - Real-time data access for applications
# MAGIC - PostgreSQL compatibility
# MAGIC
# MAGIC **Use Delta Lake when you need:**
# MAGIC - Complex analytical queries
# MAGIC - Processing large data volumes
# MAGIC - Batch transformations
# MAGIC - Historical analysis and reporting
# MAGIC - ML model training at scale
# MAGIC
# MAGIC **Best Practice**: Use **both together** with automatic synchronization!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase and the Lakehouse Paradigm
# MAGIC
# MAGIC Lakebase completes the Lakehouse vision by adding operational capabilities:
# MAGIC
# MAGIC ### Traditional Lakehouse (Analytics-Focused)
# MAGIC - Data lakes with ACID transactions
# MAGIC - Batch and streaming analytics
# MAGIC - ML and AI model training
# MAGIC - Business intelligence
# MAGIC
# MAGIC ### Lakehouse + Lakebase (Analytics + Operations)
# MAGIC - **Everything above, PLUS:**
# MAGIC - Operational applications
# MAGIC - Real-time transactions
# MAGIC - Low-latency feature serving
# MAGIC - Agent-based systems
# MAGIC
# MAGIC ### The Unified Data Platform
# MAGIC
# MAGIC With Lakebase, organizations can:
# MAGIC
# MAGIC 1. **Build operational apps** on Lakebase
# MAGIC 2. **Sync data automatically** to Delta Lake
# MAGIC 3. **Run analytics and train models** on Delta Lake
# MAGIC 4. **Serve predictions in real-time** from Lakebase
# MAGIC 5. **Maintain unified governance** via Unity Catalog
# MAGIC
# MAGIC All on a single platform with consistent:
# MAGIC - Security model
# MAGIC - Data lineage
# MAGIC - Cost management
# MAGIC - Operational tooling

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Lakebase** is a fully managed, PostgreSQL-compatible operational database from Databricks
# MAGIC
# MAGIC ✅ **Serverless architecture** with separated compute and storage enables elastic scaling and cost efficiency
# MAGIC
# MAGIC ✅ **Unity Catalog integration** provides unified governance across operational and analytical data
# MAGIC
# MAGIC ✅ **Zero-copy branching** enables safe development and testing without data duplication
# MAGIC
# MAGIC ✅ **AI-ready features** like pgvector and fast startup support modern AI applications and agents
# MAGIC
# MAGIC ✅ **Bidirectional sync** with Delta Lake eliminates complex ETL and enables hybrid workloads
# MAGIC
# MAGIC ✅ **Use Lakebase for OLTP** (applications, APIs, real-time) and **Delta Lake for OLAP** (analytics, ML training)
# MAGIC
# MAGIC ✅ **Together, they create a unified platform** that bridges the gap between data operations and data analytics
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next lab, you'll get hands-on experience:
# MAGIC - Creating a Lakebase PostgreSQL database instance
# MAGIC - Registering it in Unity Catalog
# MAGIC - Creating tables and running basic SQL operations
# MAGIC - Exploring the database configuration and settings
# MAGIC
# MAGIC Proceed to **Lab 2: Creating and Exploring a Lakebase PostgreSQL Database**
