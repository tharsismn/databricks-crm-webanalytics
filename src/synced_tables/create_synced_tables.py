"""
Synced Tables - Replica Gold tables para Lakebase PostgreSQL
Usa a REST API POST /api/2.0/database/synced_tables para criar
synced tables no Lakebase.
"""

import argparse
import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


GOLD_TABLES = {
    "gold_campaign_performance": ["campaign_id"],
    "gold_campaign_hourly_metrics": ["campaign_id", "event_date", "hour_of_day"],
    "gold_campaign_minute_metrics": ["campaign_id", "event_date", "hour_of_day", "minute_of_hour"],
    "gold_channel_performance": ["campaign_channel"],
    "gold_segment_analysis": ["target_segment"],
    "gold_geo_performance": ["geo_region", "geo_city"],
    "gold_ab_test_results": ["ab_test_group"],
    "gold_daily_kpis": ["event_date"],
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-catalog", required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--lakebase-catalog", required=True)
    parser.add_argument("--lakebase-schema", default="crm_app")
    return parser.parse_args()


def create_synced_table(workspace_url, token, table_name, source_full_name,
                        primary_key_columns, storage_catalog, storage_schema):
    """Create a synced table via POST /api/2.0/database/synced_tables."""
    url = f"{workspace_url}/api/2.0/database/synced_tables"

    payload = json.dumps({
        "name": table_name,
        "spec": {
            "source_table_full_name": source_full_name,
            "primary_key_columns": primary_key_columns,
            "scheduling_policy": "CONTINUOUS",
        },
        "new_pipeline_spec": {
            "storage_catalog": storage_catalog,
            "storage_schema": storage_schema,
        },
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        raise RuntimeError(f"HTTP {e.code}: {body}")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("CRM-SyncedTables").getOrCreate()
    dbutils = DBUtils(spark)

    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
    if workspace_url and not workspace_url.startswith("http"):
        workspace_url = f"https://{workspace_url}"

    token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().apiToken().get()
    )

    print(f"Workspace: {workspace_url}")
    print(f"Source: {args.source_catalog}.{args.source_schema}")
    print(f"Target: {args.lakebase_catalog}.{args.lakebase_schema}\n")

    success = 0
    for table, pk_cols in GOLD_TABLES.items():
        source = f"{args.source_catalog}.{args.source_schema}.{table}"
        target = f"{args.lakebase_catalog}.{args.lakebase_schema}.{table}"

        print(f"  {target} <- SYNCED TABLE {source} (PK: {pk_cols}) ... ",
              end="", flush=True)

        try:
            result = create_synced_table(
                workspace_url=workspace_url,
                token=token,
                table_name=target,
                source_full_name=source,
                primary_key_columns=pk_cols,
                storage_catalog=args.source_catalog,
                storage_schema=args.source_schema,
            )
            print("OK")
            success += 1
        except Exception as e:
            print(f"ERRO: {e}")

    print(f"\n{success}/{len(GOLD_TABLES)} synced tables criadas com sucesso!")
    if success < len(GOLD_TABLES):
        raise RuntimeError(f"{len(GOLD_TABLES) - success} synced tables falharam")


if __name__ == "__main__":
    main()
