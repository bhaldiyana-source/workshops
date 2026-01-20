# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lineage Tracking Setup

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime

class LineageTracker:
    """Track data lineage for geospatial pipelines"""
    
    def __init__(self, spark, lineage_table="metadata.data_lineage"):
        self.spark = spark
        self.lineage_table = lineage_table
        self._ensure_lineage_table()
    
    def _ensure_lineage_table(self):
        """Create lineage tracking table if it doesn't exist"""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.lineage_table} (
                lineage_id STRING,
                source_table STRING,
                target_table STRING,
                transformation_type STRING,
                transformation_description STRING,
                processing_timestamp TIMESTAMP,
                processed_by STRING,
                source_record_count BIGINT,
                target_record_count BIGINT,
                records_added BIGINT,
                records_updated BIGINT,
                records_deleted BIGINT,
                processing_duration_seconds DOUBLE,
                job_id STRING,
                notebook_path STRING
            )
            USING DELTA
        """)
    
    def record_transformation(self,
                            source_table: str,
                            target_table: str,
                            transformation_type: str,
                            description: str,
                            source_count: int = None,
                            target_count: int = None,
                            duration: float = None):
        """
        Record a data transformation in lineage table
        """
        lineage_record = {
            'lineage_id': f"{source_table}_{target_table}_{int(datetime.now().timestamp())}",
            'source_table': source_table,
            'target_table': target_table,
            'transformation_type': transformation_type,
            'transformation_description': description,
            'processing_timestamp': datetime.now(),
            'processed_by': self.spark.sql("SELECT current_user()").collect()[0][0],
            'source_record_count': source_count,
            'target_record_count': target_count,
            'records_added': target_count - source_count if source_count and target_count else None,
            'records_updated': None,  # Would need to track separately
            'records_deleted': None,
            'processing_duration_seconds': duration,
            'job_id': None,  # Could get from dbutils
            'notebook_path': None  # Could get from dbutils
        }
        
        df_lineage = self.spark.createDataFrame([lineage_record])
        df_lineage.write.format("delta").mode("append").saveAsTable(self.lineage_table)
        
        return lineage_record['lineage_id']
    
    def get_lineage(self, table_name: str, direction='downstream'):
        """
        Get lineage for a table
        
        Args:
            table_name: Name of table
            direction: 'upstream' or 'downstream'
        """
        if direction == 'downstream':
            query = f"""
                SELECT * FROM {self.lineage_table}
                WHERE source_table = '{table_name}'
                ORDER BY processing_timestamp DESC
            """
        else:  # upstream
            query = f"""
                SELECT * FROM {self.lineage_table}
                WHERE target_table = '{table_name}'
                ORDER BY processing_timestamp DESC
            """
        
        return self.spark.sql(query)
    
    def get_full_lineage_graph(self, root_table: str):
        """Get complete lineage graph from root table"""
        # Recursive query to get all downstream dependencies
        query = f"""
            WITH RECURSIVE lineage_graph AS (
                -- Base case
                SELECT 
                    source_table,
                    target_table,
                    transformation_type,
                    1 as level
                FROM {self.lineage_table}
                WHERE source_table = '{root_table}'
                
                UNION ALL
                
                -- Recursive case
                SELECT 
                    l.source_table,
                    l.target_table,
                    l.transformation_type,
                    lg.level + 1
                FROM {self.lineage_table} l
                JOIN lineage_graph lg ON l.source_table = lg.target_table
                WHERE lg.level < 10  -- Prevent infinite recursion
            )
            SELECT * FROM lineage_graph
            ORDER BY level, source_table
        """
        
        return self.spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Initialize tracker
tracker = LineageTracker(spark)

# Record a transformation
import time
start_time = time.time()

# Simulate transformation
df_source = spark.table("bronze.raw_events")
source_count = df_source.count()

df_transformed = df_source.filter(col("quality_score") >= 80)
df_transformed.write.format("delta").mode("overwrite").saveAsTable("silver.clean_events")

target_count = df_transformed.count()
duration = time.time() - start_time

# Record lineage
lineage_id = tracker.record_transformation(
    source_table="bronze.raw_events",
    target_table="silver.clean_events",
    transformation_type="filtering",
    description="Filter events with quality score >= 80",
    source_count=source_count,
    target_count=target_count,
    duration=duration
)

print(f"Lineage recorded: {lineage_id}")

# COMMAND ----------

# Query lineage
downstream = tracker.get_lineage("bronze.raw_events", direction='downstream')
display(downstream)

# COMMAND ----------

# Get full lineage graph
graph = tracker.get_full_lineage_graph("bronze.raw_events")
display(graph)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lineage Visualization

# COMMAND ----------

def visualize_lineage(spark, root_table: str):
    """
    Generate a simple text-based lineage visualization
    """
    lineage_df = spark.sql(f"""
        SELECT DISTINCT
            source_table,
            target_table,
            transformation_type
        FROM metadata.data_lineage
        WHERE source_table LIKE '%{root_table}%'
            OR target_table LIKE '%{root_table}%'
    """).collect()
    
    print(f"\nLineage for {root_table}:")
    print("=" * 80)
    
    for row in lineage_df:
        print(f"{row.source_table}")
        print(f"  └─ [{row.transformation_type}] ─> {row.target_table}")
    
    print("=" * 80)

# Example
# visualize_lineage(spark, "events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Impact Analysis

# COMMAND ----------

def analyze_impact(spark, table_name: str):
    """
    Analyze impact of changes to a table
    """
    # Find all downstream dependencies
    downstream = spark.sql(f"""
        WITH RECURSIVE deps AS (
            SELECT target_table, 1 as level
            FROM metadata.data_lineage
            WHERE source_table = '{table_name}'
            
            UNION
            
            SELECT l.target_table, d.level + 1
            FROM metadata.data_lineage l
            JOIN deps d ON l.source_table = d.target_table
            WHERE d.level < 10
        )
        SELECT target_table, MAX(level) as dependency_level
        FROM deps
        GROUP BY target_table
        ORDER BY dependency_level, target_table
    """)
    
    count = downstream.count()
    print(f"\n⚠️  Changing {table_name} will impact {count} downstream table(s):")
    downstream.show(truncate=False)
    
    return downstream

# Example
# analyze_impact(spark, "silver.clean_events")
