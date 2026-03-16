"""
Gerador de Dados Fictícios - CRM WebAnalytics (Simulação ZeroBus)

Simula o comportamento do ZeroBus Producer escrevendo dados diretamente
nas tabelas Delta Bronze. Em produção, o ZeroBus SDK faria isso
automaticamente via `stream.ingest_record_offset(event)`.

Arquitetura real:
  App Mobile -> zerobus_producer.py -> ZeroBus SDK -> Tabela Delta Bronze

Simulação (este script):
  Gera eventos fictícios -> Escreve diretamente na Tabela Delta Bronze
"""

import argparse
import json
import uuid
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--num-events", type=int, default=10000)
    return parser.parse_args()


# ── Dados de referência para geração realista ──

CAMPAIGNS = [
    {"id": "CMP-2026-001", "name": "Crédito Pessoal Pré-Aprovado", "category": "credito", "channel": "in_app_banner", "segment": "varejo", "budget": 150000.00},
    {"id": "CMP-2026-002", "name": "Cartão Platinum Upgrade", "category": "cartoes", "channel": "in_app_modal", "segment": "alta_renda", "budget": 200000.00},
    {"id": "CMP-2026-003", "name": "CDB 120% CDI Exclusivo", "category": "investimentos", "channel": "push", "segment": "alta_renda", "budget": 80000.00},
    {"id": "CMP-2026-004", "name": "Seguro Auto com 30% OFF", "category": "seguros", "channel": "in_app_banner", "segment": "varejo", "budget": 120000.00},
    {"id": "CMP-2026-005", "name": "Conta Digital PJ Zero Taxa", "category": "conta_digital", "channel": "story", "segment": "pj", "budget": 95000.00},
    {"id": "CMP-2026-006", "name": "Consignado INSS Taxa Especial", "category": "credito", "channel": "in_app_modal", "segment": "varejo", "budget": 180000.00},
    {"id": "CMP-2026-007", "name": "Fundo Multimercado Premium", "category": "investimentos", "channel": "in_app_banner", "segment": "private", "budget": 300000.00},
    {"id": "CMP-2026-008", "name": "Cashback Débito 5%", "category": "cartoes", "channel": "push", "segment": "varejo", "budget": 60000.00},
    {"id": "CMP-2026-009", "name": "Seguro Vida Família", "category": "seguros", "channel": "story", "segment": "varejo", "budget": 75000.00},
    {"id": "CMP-2026-010", "name": "Portabilidade Salário Bônus", "category": "conta_digital", "channel": "in_app_modal", "segment": "varejo", "budget": 110000.00},
    {"id": "CMP-2026-011", "name": "Refinanciamento Imobiliário", "category": "credito", "channel": "in_app_banner", "segment": "alta_renda", "budget": 250000.00},
    {"id": "CMP-2026-012", "name": "Tesouro Direto Simplificado", "category": "investimentos", "channel": "push", "segment": "varejo", "budget": 45000.00},
]

ACTION_TYPES = ["impression", "click", "dismiss", "swipe", "deep_link"]
ACTION_WEIGHTS = [0.40, 0.25, 0.15, 0.10, 0.10]

ACTION_ELEMENTS = ["banner", "button_cta", "card", "carousel_item", "floating_button"]

SCREENS = [
    "home", "extrato", "pix", "investimentos", "cartoes",
    "emprestimos", "seguros", "marketplace", "perfil", "notificacoes"
]

DEVICE_OS = ["iOS", "Android"]
DEVICE_OS_WEIGHTS = [0.45, 0.55]

APP_VERSIONS = ["8.12.0", "8.11.3", "8.11.2", "8.10.1", "8.9.0"]

UF_CITIES = {
    "SP": ["São Paulo", "Campinas", "Santos", "Ribeirão Preto", "Osasco"],
    "RJ": ["Rio de Janeiro", "Niterói", "Petrópolis", "Volta Redonda"],
    "MG": ["Belo Horizonte", "Uberlândia", "Juiz de Fora", "Contagem"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringá"],
    "BA": ["Salvador", "Feira de Santana"],
    "PE": ["Recife", "Olinda"],
    "DF": ["Brasília"],
    "SC": ["Florianópolis", "Joinville"],
    "GO": ["Goiânia", "Aparecida de Goiânia"],
}

AB_GROUPS = ["control", "variant_a", "variant_b"]


def generate_event(base_date: datetime) -> dict:
    """
    Gera um evento de web analytics no mesmo formato JSON que o
    ZeroBus Producer enviaria via `stream.ingest_record_offset(event)`.
    """
    campaign = random.choice(CAMPAIGNS)
    uf = random.choice(list(UF_CITIES.keys()))
    city = random.choice(UF_CITIES[uf])
    device_os = random.choices(DEVICE_OS, weights=DEVICE_OS_WEIGHTS)[0]
    action_type = random.choices(ACTION_TYPES, weights=ACTION_WEIGHTS)[0]

    # Distribui timestamps ao longo dos últimos 7 dias com pico 18h-22h
    hour = random.choices(
        range(24),
        weights=[1,1,1,1,1,1,2,3,4,5,5,4,4,5,5,5,6,7,8,9,9,7,4,2]
    )[0]
    event_ts = base_date.replace(hour=hour, minute=random.randint(0, 59), second=random.randint(0, 59))
    event_ts -= timedelta(days=random.randint(0, 6))

    screen = random.choice(SCREENS)
    referrer = random.choice([s for s in SCREENS if s != screen])

    return {
        "event_id": f"EVT-{uuid.uuid4().hex}",
        "event_timestamp": event_ts.isoformat(),
        "customer_id": f"CUST-{random.randint(100000, 999999)}",
        "session_id": f"SESS-{uuid.uuid4().hex[:12]}",
        "device_id": f"DEV-{uuid.uuid4().hex[:8]}",
        "device_os": device_os,
        "app_version": random.choice(APP_VERSIONS),
        "campaign_id": campaign["id"],
        "campaign_name": campaign["name"],
        "campaign_channel": campaign["channel"],
        "campaign_category": campaign["category"],
        "offer_id": f"OFR-{campaign['id'].split('-')[-1]}-{random.randint(1,5):03d}",
        "action_type": action_type,
        "action_element": random.choice(ACTION_ELEMENTS) if action_type in ("click", "deep_link") else None,
        "screen_name": screen,
        "referrer_screen": referrer,
        "geo_region": uf,
        "geo_city": city,
        "is_personalized": random.random() < 0.65,
        "ab_test_group": random.choice(AB_GROUPS),
        "response_time_ms": random.randint(50, 800),
    }


def generate_campaign_metadata(base_date: datetime) -> list:
    """Gera metadados das campanhas no formato JSON do ZeroBus."""
    rows = []
    for c in CAMPAIGNS:
        start = base_date - timedelta(days=random.randint(10, 60))
        end = start + timedelta(days=random.randint(30, 90))
        rows.append({
            "campaign_id": c["id"],
            "campaign_name": c["name"],
            "campaign_description": f"Campanha {c['name']} direcionada ao segmento {c['segment']}",
            "campaign_category": c["category"],
            "campaign_channel": c["channel"],
            "campaign_start_date": start.date().isoformat(),
            "campaign_end_date": end.date().isoformat(),
            "target_segment": c["segment"],
            "budget_brl": float(c["budget"]),
            "is_active": end.date() >= base_date.date(),
            "created_by": random.choice(["equipe_crm", "marketing_digital", "growth_team"]),
        })
    return rows


def generate_sessions(events: list) -> list:
    """Agrega eventos por sessão no formato JSON do ZeroBus."""
    sessions = {}
    for e in events:
        sid = e["session_id"]
        if sid not in sessions:
            sessions[sid] = {
                "session_id": sid,
                "customer_id": e["customer_id"],
                "device_id": e["device_id"],
                "device_os": e["device_os"],
                "app_version": e["app_version"],
                "session_start": e["event_timestamp"],
                "session_end": e["event_timestamp"],
                "geo_region": e["geo_region"],
                "geo_city": e["geo_city"],
                "total_events": 0,
                "total_clicks": 0,
            }
        s = sessions[sid]
        s["total_events"] += 1
        if e["action_type"] == "click":
            s["total_clicks"] += 1
        if e["event_timestamp"] < s["session_start"]:
            s["session_start"] = e["event_timestamp"]
        if e["event_timestamp"] > s["session_end"]:
            s["session_end"] = e["event_timestamp"]
    return list(sessions.values())


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("CRM-WebAnalytics-DataGenerator").getOrCreate()

    base_date = datetime.utcnow()
    print(f"Generating {args.num_events} CRM campaign events (simulating ZeroBus ingestion)...")

    # ── Gerar eventos de clicks (como o ZeroBus Producer faria) ──
    events = [generate_event(base_date) for _ in range(args.num_events)]
    df_clicks = spark.createDataFrame(events)

    target_table = f"{args.catalog}.{args.schema}.bronze_crm_campaign_clicks"
    df_clicks.write.format("delta").mode("append").saveAsTable(target_table)
    print(f"Wrote {df_clicks.count()} events to {target_table}")

    # ── Gerar metadados das campanhas ──
    campaigns_meta = generate_campaign_metadata(base_date)
    df_campaigns = spark.createDataFrame(campaigns_meta)

    meta_table = f"{args.catalog}.{args.schema}.bronze_crm_campaigns_metadata"
    df_campaigns.write.format("delta").mode("overwrite").saveAsTable(meta_table)
    print(f"Wrote {df_campaigns.count()} campaign metadata records to {meta_table}")

    # ── Gerar sessões agregadas ──
    sessions_data = generate_sessions(events)
    df_sessions = spark.createDataFrame(sessions_data)

    sessions_table = f"{args.catalog}.{args.schema}.bronze_app_sessions"
    df_sessions.write.format("delta").mode("append").saveAsTable(sessions_table)
    print(f"Wrote {df_sessions.count()} session records to {sessions_table}")

    print("Data generation complete! (simulating ZeroBus -> Delta Bronze)")


if __name__ == "__main__":
    main()
