from fastapi import APIRouter, HTTPException
from server.db import pool

router = APIRouter()


@router.get("/kpis/summary")
async def kpis_summary():
    try:
        row = await pool.fetchrow("""
            SELECT
                COALESCE(SUM(total_impressions), 0) as total_impressions,
                COALESCE(SUM(total_clicks), 0) as total_clicks,
                CASE WHEN SUM(total_impressions) > 0
                     THEN ROUND(SUM(total_clicks)::numeric / SUM(total_impressions) * 100, 2)
                     ELSE 0 END as ctr,
                COALESCE(SUM(total_conversions), 0) as total_conversions,
                COALESCE(SUM(total_events), 0) as total_events,
                CASE WHEN SUM(total_events) > 0
                     THEN ROUND(SUM(sum_response_time_ms)::numeric / SUM(total_events), 0)
                     ELSE 0 END as avg_response_time_ms
            FROM gold_daily_kpis
        """)
        return row or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kpis/daily")
async def kpis_daily():
    try:
        rows = await pool.fetch("""
            SELECT event_date, total_impressions, total_clicks, ctr,
                   total_conversions, total_events, avg_response_time_ms,
                   conversion_rate
            FROM gold_daily_kpis
            ORDER BY event_date DESC
            LIMIT 30
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/channels")
async def channel_performance():
    try:
        rows = await pool.fetch("""
            SELECT campaign_channel, total_impressions, total_clicks, ctr,
                   avg_response_time_ms, total_events
            FROM gold_channel_performance
            ORDER BY total_clicks DESC
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/segments")
async def segment_analysis():
    try:
        rows = await pool.fetch("""
            SELECT target_segment, total_impressions, total_clicks, ctr,
                   avg_response_time_ms, total_events
            FROM gold_segment_analysis
            ORDER BY total_clicks DESC
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ab-test")
async def ab_test_results():
    try:
        rows = await pool.fetch("""
            SELECT ab_test_group, total_impressions, total_clicks, ctr,
                   avg_response_time_ms, total_events
            FROM gold_ab_test_results
            ORDER BY ab_test_group
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geo")
async def geo_performance():
    try:
        rows = await pool.fetch("""
            SELECT geo_region, geo_city, total_events, total_clicks,
                   total_impressions, ctr
            FROM gold_geo_performance
            ORDER BY total_clicks DESC
            LIMIT 20
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _serialize(row):
    """Converte tipos especiais (date, Decimal) para JSON."""
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__"):
            out[k] = float(v)
        else:
            out[k] = v
    return out
