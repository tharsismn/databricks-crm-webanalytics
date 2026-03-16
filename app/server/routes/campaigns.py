from fastapi import APIRouter, HTTPException, Query
from server.db import pool

router = APIRouter()


@router.get("/campaigns/performance")
async def campaign_performance():
    try:
        rows = await pool.fetch("""
            SELECT campaign_id, campaign_name, campaign_category, campaign_channel,
                   target_segment, budget_brl, is_active,
                   total_impressions, total_clicks, total_conversions, ctr,
                   conversion_rate, total_conversion_value,
                   avg_response_time_ms, cost_per_click, total_events
            FROM gold_campaign_performance
            ORDER BY ctr DESC
        """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns/hourly")
async def campaign_hourly(campaign_id: str = Query(None)):
    try:
        if campaign_id:
            rows = await pool.fetch("""
                SELECT campaign_id, campaign_name, event_date, hour_of_day,
                       impressions, clicks, conversions, ctr, total_events
                FROM gold_campaign_hourly_metrics
                WHERE campaign_id = $1
                ORDER BY event_date DESC, hour_of_day
                LIMIT 168
            """, campaign_id)
        else:
            rows = await pool.fetch("""
                SELECT event_date, hour_of_day,
                       SUM(impressions) as impressions,
                       SUM(clicks) as clicks,
                       SUM(conversions) as conversions,
                       CASE WHEN SUM(impressions) > 0
                            THEN ROUND(SUM(clicks)::numeric / SUM(impressions) * 100, 2)
                            ELSE 0 END as ctr,
                       SUM(total_events) as total_events
                FROM gold_campaign_hourly_metrics
                GROUP BY event_date, hour_of_day
                ORDER BY event_date DESC, hour_of_day
                LIMIT 168
            """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns/trend")
async def campaign_trend(
    granularity: str = Query("hour"),
    campaign_id: str = Query(None),
):
    try:
        if granularity == "minute":
            if campaign_id:
                rows = await pool.fetch("""
                    SELECT campaign_id, campaign_name, event_date, hour_of_day, minute_of_hour,
                           impressions, clicks, conversions, ctr, total_events
                    FROM gold_campaign_minute_metrics
                    WHERE campaign_id = $1
                    ORDER BY event_date DESC, hour_of_day DESC, minute_of_hour DESC
                    LIMIT 60
                """, campaign_id)
            else:
                rows = await pool.fetch("""
                    SELECT event_date, hour_of_day, minute_of_hour,
                           SUM(impressions) as impressions,
                           SUM(clicks) as clicks,
                           SUM(conversions) as conversions,
                           CASE WHEN SUM(impressions) > 0
                                THEN ROUND(SUM(clicks)::numeric / SUM(impressions) * 100, 2)
                                ELSE 0 END as ctr,
                           SUM(total_events) as total_events
                    FROM gold_campaign_minute_metrics
                    GROUP BY event_date, hour_of_day, minute_of_hour
                    ORDER BY event_date DESC, hour_of_day DESC, minute_of_hour DESC
                    LIMIT 60
                """)
        else:
            if campaign_id:
                rows = await pool.fetch("""
                    SELECT campaign_id, campaign_name, event_date, hour_of_day,
                           impressions, clicks, conversions, ctr, total_events
                    FROM gold_campaign_hourly_metrics
                    WHERE campaign_id = $1
                    ORDER BY event_date DESC, hour_of_day
                    LIMIT 168
                """, campaign_id)
            else:
                rows = await pool.fetch("""
                    SELECT event_date, hour_of_day,
                           SUM(impressions) as impressions,
                           SUM(clicks) as clicks,
                           SUM(conversions) as conversions,
                           CASE WHEN SUM(impressions) > 0
                                THEN ROUND(SUM(clicks)::numeric / SUM(impressions) * 100, 2)
                                ELSE 0 END as ctr,
                           SUM(total_events) as total_events
                    FROM gold_campaign_hourly_metrics
                    GROUP BY event_date, hour_of_day
                    ORDER BY event_date DESC, hour_of_day
                    LIMIT 168
                """)
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns/search")
async def campaign_search(q: str = Query("", min_length=0)):
    try:
        if not q.strip():
            return []
        rows = await pool.fetch("""
            SELECT campaign_id, campaign_name, campaign_category, campaign_channel,
                   target_segment, total_impressions, total_clicks, total_conversions, ctr,
                   total_events
            FROM gold_campaign_performance
            WHERE LOWER(campaign_name) LIKE LOWER($1)
               OR LOWER(campaign_category) LIKE LOWER($1)
               OR LOWER(campaign_channel) LIKE LOWER($1)
            ORDER BY total_clicks DESC
            LIMIT 10
        """, f"%{q}%")
        return [_serialize(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns/hour-detail")
async def campaign_hour_detail(date: str = Query(...), hour: int = Query(...)):
    try:
        top_campaigns = await pool.fetch("""
            SELECT campaign_id, campaign_name, impressions, clicks, conversions, ctr
            FROM gold_campaign_hourly_metrics
            WHERE event_date = $1 AND hour_of_day = $2
            ORDER BY clicks DESC
            LIMIT 5
        """, date, hour)
        channel_breakdown = await pool.fetch("""
            SELECT campaign_channel,
                   SUM(total_clicks) as clicks, SUM(total_impressions) as impressions
            FROM gold_campaign_performance
            WHERE campaign_id IN (
                SELECT DISTINCT campaign_id FROM gold_campaign_hourly_metrics
                WHERE event_date = $1 AND hour_of_day = $2
            )
            GROUP BY campaign_channel
            ORDER BY clicks DESC
        """, date, hour)
        return {
            "date": date,
            "hour": hour,
            "top_campaigns": [_serialize(r) for r in top_campaigns],
            "channel_breakdown": [_serialize(r) for r in channel_breakdown],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/campaigns/compare")
async def campaign_compare(
    id1: str = Query(...), id2: str = Query(...)
):
    try:
        perf1 = await pool.fetchrow("""
            SELECT campaign_id, campaign_name, campaign_category, campaign_channel,
                   target_segment, budget_brl, total_impressions, total_clicks,
                   total_conversions, ctr, conversion_rate, total_conversion_value,
                   avg_response_time_ms, cost_per_click, total_events
            FROM gold_campaign_performance WHERE campaign_id = $1
        """, id1)
        perf2 = await pool.fetchrow("""
            SELECT campaign_id, campaign_name, campaign_category, campaign_channel,
                   target_segment, budget_brl, total_impressions, total_clicks,
                   total_conversions, ctr, conversion_rate, total_conversion_value,
                   avg_response_time_ms, cost_per_click, total_events
            FROM gold_campaign_performance WHERE campaign_id = $1
        """, id2)
        hourly1 = await pool.fetch("""
            SELECT event_date, hour_of_day, impressions, clicks, conversions, ctr
            FROM gold_campaign_hourly_metrics
            WHERE campaign_id = $1
            ORDER BY event_date DESC, hour_of_day
            LIMIT 48
        """, id1)
        hourly2 = await pool.fetch("""
            SELECT event_date, hour_of_day, impressions, clicks, conversions, ctr
            FROM gold_campaign_hourly_metrics
            WHERE campaign_id = $1
            ORDER BY event_date DESC, hour_of_day
            LIMIT 48
        """, id2)
        return {
            "campaign1": _serialize(perf1) if perf1 else None,
            "campaign2": _serialize(perf2) if perf2 else None,
            "hourly1": [_serialize(r) for r in hourly1],
            "hourly2": [_serialize(r) for r in hourly2],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _serialize(row):
    out = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "__float__"):
            out[k] = float(v)
        else:
            out[k] = v
    return out
