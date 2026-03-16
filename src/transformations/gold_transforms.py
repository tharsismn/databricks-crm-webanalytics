"""
Gold Layer - CRM WebAnalytics DLT Pipeline
Agregações e KPIs de negócio para os times de CRM de finanças.
Tabelas são streaming tables com CDF habilitado.
"""

import dlt
from pyspark.sql import functions as F


# ── Gold: Performance por Campanha ──

@dlt.table(
    name="gold_campaign_performance",
    comment="Gold: performance consolidada por campanha CRM",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_campaign_performance():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    campaigns = dlt.read("silver_crm_campaigns")

    agg = (
        clicks.groupBy("campaign_id")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.count(F.when(F.col("action_type") == "conversion", 1)).alias("total_conversions"),
            F.count(F.when(F.col("action_type") == "dismiss", 1)).alias("total_dismisses"),
            F.count(F.when(F.col("action_type") == "deep_link", 1)).alias("total_deep_links"),
            F.sum(F.when(F.col("action_type") == "conversion", F.col("conversion_value"))).alias("total_conversion_value"),
            F.sum("response_time_ms").alias("sum_response_time_ms"),
            F.count("*").alias("total_events"),
        )
    )

    return (
        agg.join(campaigns, "campaign_id", "left")
        .withColumn("avg_response_time_ms",
                    F.when(F.col("total_events") > 0,
                           F.round(F.col("sum_response_time_ms") / F.col("total_events"), 2))
                     .otherwise(0))
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
        .withColumn("conversion_rate",
                    F.when(F.col("total_clicks") > 0,
                           F.round(F.col("total_conversions") / F.col("total_clicks") * 100, 2))
                     .otherwise(0))
        .withColumn("cost_per_click",
                    F.when(F.col("total_clicks") > 0,
                           F.round(F.col("budget_brl") / F.col("total_clicks"), 2))
                     .otherwise(None))
        .select(
            "campaign_id", "campaign_name", "campaign_category", "campaign_channel",
            "target_segment", "budget_brl", "is_active", "is_expired",
            "total_impressions", "total_clicks", "total_conversions", "total_dismisses", "total_deep_links",
            "ctr", "conversion_rate", "total_conversion_value",
            "avg_response_time_ms", "cost_per_click", "total_events",
        )
    )


# ── Gold: Métricas Horárias por Campanha ──

@dlt.table(
    name="gold_campaign_hourly_metrics",
    comment="Gold: série temporal horária de métricas por campanha",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_campaign_hourly_metrics():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.groupBy("campaign_id", "campaign_name", "event_date", "hour_of_day")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("clicks"),
            F.count(F.when(F.col("action_type") == "conversion", 1)).alias("conversions"),
            F.count("*").alias("total_events"),
        )
        .withColumn("ctr",
                    F.when(F.col("impressions") > 0,
                           F.round(F.col("clicks") / F.col("impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: Métricas por Minuto por Campanha ──

@dlt.table(
    name="gold_campaign_minute_metrics",
    comment="Gold: série temporal por minuto de métricas por campanha",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_campaign_minute_metrics():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.withColumn("minute_of_hour", F.minute("event_timestamp"))
        .groupBy("campaign_id", "campaign_name", "event_date", "hour_of_day", "minute_of_hour")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("clicks"),
            F.count(F.when(F.col("action_type") == "conversion", 1)).alias("conversions"),
            F.count("*").alias("total_events"),
        )
        .withColumn("ctr",
                    F.when(F.col("impressions") > 0,
                           F.round(F.col("clicks") / F.col("impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: Performance por Canal ──

@dlt.table(
    name="gold_channel_performance",
    comment="Gold: performance por canal de campanha (push, banner, modal, story)",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_channel_performance():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.groupBy("campaign_channel")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.sum("response_time_ms").alias("sum_response_time_ms"),
            F.count("*").alias("total_events"),
        )
        .withColumn("avg_response_time_ms",
                    F.when(F.col("total_events") > 0,
                           F.round(F.col("sum_response_time_ms") / F.col("total_events"), 2))
                     .otherwise(0))
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: Análise por Segmento ──

@dlt.table(
    name="gold_segment_analysis",
    comment="Gold: performance por segmento de cliente (varejo, alta_renda, private, pj)",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_segment_analysis():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    campaigns = dlt.read("silver_crm_campaigns")

    return (
        clicks.join(campaigns.select("campaign_id", "target_segment"), "campaign_id", "left")
        .groupBy("target_segment")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.sum("response_time_ms").alias("sum_response_time_ms"),
            F.count("*").alias("total_events"),
        )
        .withColumn("avg_response_time_ms",
                    F.when(F.col("total_events") > 0,
                           F.round(F.col("sum_response_time_ms") / F.col("total_events"), 2))
                     .otherwise(0))
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: Performance Geográfica ──

@dlt.table(
    name="gold_geo_performance",
    comment="Gold: performance por região e cidade",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_geo_performance():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.groupBy("geo_region", "geo_city")
        .agg(
            F.count("*").alias("total_events"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
        )
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: Resultados A/B Test ──

@dlt.table(
    name="gold_ab_test_results",
    comment="Gold: comparação de performance entre grupos A/B test",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_ab_test_results():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.groupBy("ab_test_group")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.sum("response_time_ms").alias("sum_response_time_ms"),
            F.count("*").alias("total_events"),
        )
        .withColumn("avg_response_time_ms",
                    F.when(F.col("total_events") > 0,
                           F.round(F.col("sum_response_time_ms") / F.col("total_events"), 2))
                     .otherwise(0))
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
    )


# ── Gold: KPIs Diários Consolidados ──

@dlt.table(
    name="gold_daily_kpis",
    comment="Gold: KPIs diários consolidados de todas as campanhas CRM",
    table_properties={"quality": "gold", "delta.enableChangeDataFeed": "true"},
)
def gold_daily_kpis():
    clicks = dlt.read_stream("silver_crm_campaign_clicks")
    return (
        clicks.groupBy("event_date")
        .agg(
            F.count(F.when(F.col("action_type") == "impression", 1)).alias("total_impressions"),
            F.count(F.when(F.col("action_type") == "click", 1)).alias("total_clicks"),
            F.count(F.when(F.col("action_type") == "conversion", 1)).alias("total_conversions"),
            F.count(F.when(F.col("action_type") == "dismiss", 1)).alias("total_dismisses"),
            F.sum(F.when(F.col("action_type") == "conversion", F.col("conversion_value"))).alias("total_conversion_value"),
            F.sum("response_time_ms").alias("sum_response_time_ms"),
            F.count("*").alias("total_events"),
        )
        .withColumn("avg_response_time_ms",
                    F.when(F.col("total_events") > 0,
                           F.round(F.col("sum_response_time_ms") / F.col("total_events"), 2))
                     .otherwise(0))
        .withColumn("ctr",
                    F.when(F.col("total_impressions") > 0,
                           F.round(F.col("total_clicks") / F.col("total_impressions") * 100, 2))
                     .otherwise(0))
        .withColumn("conversion_rate",
                    F.when(F.col("total_clicks") > 0,
                           F.round(F.col("total_conversions") / F.col("total_clicks") * 100, 2))
                     .otherwise(0))
        .orderBy("event_date")
    )
