"""
Silver Layer - CRM WebAnalytics DLT Pipeline
Transforma dados brutos da Bronze em dados limpos, tipados e enriquecidos.
"""

import dlt
from pyspark.sql import functions as F


BRONZE_CATALOG = spark.conf.get("bronze_catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")


# ── Silver: Clicks em Campanhas CRM ──

@dlt.table(
    name="silver_crm_campaign_clicks",
    comment="Silver: clicks em campanhas CRM - tipados, deduplicated, enriquecidos",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_campaign_id", "campaign_id IS NOT NULL")
@dlt.expect("valid_action_type",
            "action_type IN ('impression', 'click', 'dismiss', 'swipe', 'deep_link', 'conversion')")
def silver_crm_campaign_clicks():
    df = spark.readStream.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_crm_campaign_clicks")

    # Handle columns that may not exist in older data
    if "conversion_value" not in df.columns:
        df = df.withColumn("conversion_value", F.lit(None).cast("decimal(15,2)"))
    else:
        df = df.withColumn("conversion_value", F.col("conversion_value").cast("decimal(15,2)"))

    if "conversion_product" not in df.columns:
        df = df.withColumn("conversion_product", F.lit(None).cast("string"))

    return (
        df
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("response_time_ms", F.col("response_time_ms").cast("int"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("hour_of_day", F.hour("event_timestamp"))
        .withColumn("day_of_week", F.dayofweek("event_timestamp"))
        .withColumn("is_business_hours",
                    (F.hour("event_timestamp").between(9, 18)) &
                    (F.dayofweek("event_timestamp").between(2, 6)))
        .dropDuplicates(["event_id"])
    )


# ── Silver: Metadados das Campanhas ──

@dlt.table(
    name="silver_crm_campaigns",
    comment="Silver: metadados das campanhas CRM - tipados, enriquecidos",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_campaign_id", "campaign_id IS NOT NULL")
def silver_crm_campaigns():
    return (
        spark.read.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_crm_campaigns_metadata")
        .withColumn("campaign_start_date", F.to_date("campaign_start_date"))
        .withColumn("campaign_end_date", F.to_date("campaign_end_date"))
        .withColumn("budget_brl", F.col("budget_brl").cast("decimal(15,2)"))
        .withColumn("is_expired", F.col("campaign_end_date") < F.current_date())
        .withColumn("duration_days",
                    F.datediff("campaign_end_date", "campaign_start_date"))
    )


# ── Silver: Sessões do Mobile App ──

@dlt.table(
    name="silver_app_sessions",
    comment="Silver: sessões do mobile app - tipadas, com duração e nível de engajamento",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_session_id", "session_id IS NOT NULL")
def silver_app_sessions():
    return (
        spark.readStream.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_app_sessions")
        .withColumn("session_start", F.to_timestamp("session_start"))
        .withColumn("session_end", F.to_timestamp("session_end"))
        .withColumn("total_events", F.col("total_events").cast("int"))
        .withColumn("total_clicks", F.col("total_clicks").cast("int"))
        .withColumn("session_duration_minutes",
                    (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 60.0)
        .withColumn("engagement_level",
                    F.when((F.col("total_clicks") >= 5) & (F.col("session_duration_minutes") >= 10), "high")
                     .when((F.col("total_clicks") >= 2) & (F.col("session_duration_minutes") >= 3), "medium")
                     .otherwise("low"))
        .dropDuplicates(["session_id"])
    )
