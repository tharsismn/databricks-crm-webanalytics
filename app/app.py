"""
CRM Campaign Monitor - Databricks App
FastAPI backend + React frontend para monitoramento real-time de campanhas CRM.
Dados servidos do Lakebase PostgreSQL (synced tables do Gold layer).
"""

import os
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from server.db import pool
from server.routes import kpis, campaigns, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await pool.init()
        print("Lakebase pool ready")
    except Exception as e:
        print(f"WARN: Lakebase pool init failed: {e}")
        traceback.print_exc()
    yield
    await pool.close()


app = FastAPI(title="CRM Campaign Monitor", lifespan=lifespan)

# API routes
app.include_router(health.router, prefix="/api", tags=["health"])
app.include_router(kpis.router, prefix="/api", tags=["kpis"])
app.include_router(campaigns.router, prefix="/api", tags=["campaigns"])


@app.get("/api/debug")
async def debug():
    """Endpoint de diagnóstico."""
    info = {
        "lakebase_host": os.environ.get("LAKEBASE_HOST", "NOT SET"),
        "lakebase_db": os.environ.get("LAKEBASE_DB", "NOT SET"),
        "lakebase_user": os.environ.get("LAKEBASE_USER", "NOT SET"),
        "lakebase_password_set": bool(os.environ.get("LAKEBASE_PASSWORD")),
        "pool_initialized": pool._pool is not None,
    }
    if pool._pool is not None:
        try:
            row = await pool.fetchrow("SELECT 1 as ping")
            info["db_connection"] = "OK"
        except Exception as e:
            info["db_connection"] = f"ERROR: {e}"
        try:
            tables = await pool.fetch(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name"
            )
            info["all_tables"] = [f"{t['table_schema']}.{t['table_name']}" for t in tables]
        except Exception as e:
            info["tables_error"] = str(e)
        try:
            schemas = await pool.fetch(
                "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name"
            )
            info["schemas"] = [s["schema_name"] for s in schemas]
        except Exception as e:
            info["schemas_error"] = str(e)
        try:
            sp = await pool.fetchrow("SHOW search_path")
            info["search_path"] = dict(sp) if sp else "unknown"
        except Exception as e:
            info["search_path_error"] = str(e)
        # Direct test query
        try:
            row = await pool.fetchrow(
                "SELECT count(*) as cnt FROM crm_app.gold_daily_kpis"
            )
            info["gold_daily_kpis_count"] = row["cnt"] if row else 0
        except Exception as e:
            info["gold_daily_kpis_query_error"] = str(e)
        try:
            row = await pool.fetchrow(
                "SELECT count(*) as cnt FROM gold_daily_kpis"
            )
            info["gold_daily_kpis_no_schema_count"] = row["cnt"] if row else 0
        except Exception as e:
            info["gold_daily_kpis_no_schema_error"] = str(e)
    return info


# Serve React frontend - static assets first, then SPA fallback
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dir):
    assets_dir = os.path.join(frontend_dir, "assets")
    if os.path.exists(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")


@app.api_route("/{full_path:path}", methods=["GET"])
async def serve_spa(request: Request, full_path: str):
    """SPA fallback - serve index.html for non-API routes."""
    # Never intercept API routes
    if full_path.startswith("api/") or full_path == "api":
        return JSONResponse({"error": "not found"}, status_code=404)

    index = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return JSONResponse({"error": "frontend not built"}, status_code=404)
