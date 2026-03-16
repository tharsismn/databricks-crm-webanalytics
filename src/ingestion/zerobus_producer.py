"""
ZeroBus REST Producer - CRM WebAnalytics
Gera eventos fictícios de web analytics das campanhas CRM do mobile banking
e envia via ZeroBus REST API para tabelas Delta Bronze.

Modos de operação:
  --continuous: Envia 100 records/sec até atingir --max-clicks (300K default)
  (sem flag): One-shot, envia --num-events de uma vez

Fluxo:
  1. Obtém OAuth token com Service Principal (auto-refresh a cada 50min)
  2. Gera eventos de clicks, metadados de campanhas e sessões
  3. Envia via POST para ZeroBus REST endpoint (ingest-batch)
  4. ZeroBus pousa automaticamente nas tabelas Delta Bronze
"""

import argparse
import json
import uuid
import random
import time
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth


def parse_args():
    parser = argparse.ArgumentParser(description="ZeroBus REST Producer - CRM WebAnalytics")
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--secrets-scope", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    # One-shot mode
    parser.add_argument("--num-events", type=int, default=1000)
    # Continuous mode
    parser.add_argument("--continuous", action="store_true",
                        help="Modo contínuo: envia a target-rps até max-clicks")
    parser.add_argument("--target-rps", type=int, default=100,
                        help="Records per second alvo (default 100)")
    parser.add_argument("--max-clicks", type=int, default=300000,
                        help="Stop após N clicks (default 300000)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Registros por batch (default 100)")
    parser.add_argument("--session-flush-interval", type=int, default=3000,
                        help="Flush sessões a cada N clicks (default 3000)")
    return parser.parse_args()


# ── Dados de referência ──

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

ACTION_TYPES = ["impression", "click", "dismiss", "swipe", "deep_link", "conversion"]
ACTION_WEIGHTS = [0.35, 0.22, 0.15, 0.10, 0.10, 0.08]
ACTION_ELEMENTS = ["banner", "button_cta", "card", "carousel_item", "floating_button"]
SCREENS = ["home", "extrato", "pix", "investimentos", "cartoes", "emprestimos", "seguros", "marketplace", "perfil", "notificacoes"]
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
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 5, 4, 4, 5, 5, 5, 6, 7, 8, 9, 9, 7, 4, 2]


# ── Token Manager (auto-refresh) ──

class TokenManager:
    """Gerencia OAuth tokens com auto-refresh antes da expiração."""

    def __init__(self, workspace_url, workspace_id, client_id, client_secret,
                 catalog, schema):
        self.workspace_url = workspace_url
        self.workspace_id = workspace_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.catalog = catalog
        self.schema = schema
        self._tokens = {}  # table_name -> (token, expiry_time)
        self.token_lifetime = 3000  # 50 min (token expira em 1h)

    def get_token(self, table_name: str) -> str:
        now = time.time()
        if table_name in self._tokens:
            token, expiry = self._tokens[table_name]
            if now < expiry:
                return token

        token = self._fetch_token(table_name)
        self._tokens[table_name] = (token, now + self.token_lifetime)
        return token

    def _fetch_token(self, table_name: str) -> str:
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        authorization_details = json.dumps([
            {"type": "unity_catalog_privileges", "privileges": ["USE CATALOG"],
             "object_type": "CATALOG", "object_full_path": self.catalog},
            {"type": "unity_catalog_privileges", "privileges": ["USE SCHEMA"],
             "object_type": "SCHEMA", "object_full_path": f"{self.catalog}.{self.schema}"},
            {"type": "unity_catalog_privileges", "privileges": ["SELECT", "MODIFY"],
             "object_type": "TABLE", "object_full_path": full_table}
        ])
        resp = requests.post(
            f"{self.workspace_url}/oidc/v1/token",
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
                "resource": f"api://databricks/workspaces/{self.workspace_id}/zerobusDirectWriteApi",
                "authorization_details": authorization_details
            }
        )
        resp.raise_for_status()
        print(f"  [Token] Refreshed for {table_name}")
        return resp.json()["access_token"]


# ── ZeroBus REST Ingest (com retry) ──

def zerobus_ingest_batch(zerobus_url, workspace_url, token, full_table_name,
                         records, max_retries=3):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "unity-catalog-endpoint": workspace_url,
        "x-databricks-zerobus-table-name": full_table_name,
    }
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                f"{zerobus_url}/api/1.0/ingest-batch?table_name={full_table_name}",
                headers=headers,
                data=json.dumps(records),
                timeout=30,
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            body = ""
            if hasattr(e, 'response') and e.response is not None:
                body = e.response.text[:500]
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  [Retry] Attempt {attempt + 1} failed: {e}. Body: {body}. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [Error] Failed after {max_retries} attempts: {e}. Body: {body}")
                raise


# ── Session Pool (reutiliza session_ids) ──

class SessionPool:
    """Pool de sessions reutilizáveis para gerar ~6 clicks/sessão."""

    def __init__(self, pool_size=50000):
        self.pool_size = pool_size
        self.sessions = {}
        self._generate_pool()

    def _generate_pool(self):
        for _ in range(self.pool_size):
            sid = f"SESS-{uuid.uuid4().hex[:12]}"
            uf = random.choice(list(UF_CITIES.keys()))
            self.sessions[sid] = {
                "session_id": sid,
                "customer_id": f"CUST-{random.randint(100000, 999999)}",
                "device_id": f"DEV-{uuid.uuid4().hex[:8]}",
                "device_os": random.choices(DEVICE_OS, weights=DEVICE_OS_WEIGHTS)[0],
                "app_version": random.choice(APP_VERSIONS),
                "geo_region": uf,
                "geo_city": random.choice(UF_CITIES[uf]),
                "session_start": None,
                "session_end": None,
                "total_events": 0,
                "total_clicks": 0,
            }

    def get_random_session(self):
        return random.choice(list(self.sessions.values()))

    def record_event(self, session_id, event_timestamp, is_click):
        s = self.sessions[session_id]
        s["total_events"] += 1
        if is_click:
            s["total_clicks"] += 1
        if s["session_start"] is None or event_timestamp < s["session_start"]:
            s["session_start"] = event_timestamp
        if s["session_end"] is None or event_timestamp > s["session_end"]:
            s["session_end"] = event_timestamp

    def flush_sessions(self):
        """Retorna sessões com eventos e reseta contadores."""
        active = []
        for s in self.sessions.values():
            if s["total_events"] > 0:
                active.append({
                    "session_id": s["session_id"],
                    "customer_id": s["customer_id"],
                    "device_id": s["device_id"],
                    "device_os": s["device_os"],
                    "app_version": s["app_version"],
                    "session_start": s["session_start"],
                    "session_end": s["session_end"],
                    "geo_region": s["geo_region"],
                    "geo_city": s["geo_city"],
                    "total_events": s["total_events"],
                    "total_clicks": s["total_clicks"],
                })
                # Reset
                s["total_events"] = 0
                s["total_clicks"] = 0
                s["session_start"] = None
                s["session_end"] = None
        return active


# ── Gerador de eventos ──

def generate_click_event(session_pool: SessionPool) -> dict:
    campaign = random.choice(CAMPAIGNS)
    session = session_pool.get_random_session()
    action_type = random.choices(ACTION_TYPES, weights=ACTION_WEIGHTS)[0]

    now = datetime.utcnow()
    hour = random.choices(range(24), weights=HOUR_WEIGHTS)[0]
    event_ts = now.replace(hour=hour, minute=random.randint(0, 59),
                           second=random.randint(0, 59))
    event_ts -= timedelta(days=random.randint(0, 6))
    ts_str = event_ts.isoformat()

    screen = random.choice(SCREENS)
    referrer = random.choice([s for s in SCREENS if s != screen])

    session_pool.record_event(session["session_id"], ts_str,
                              action_type == "click")

    # Dados de conversão (quando action_type == "conversion")
    conversion_value = 0.0
    conversion_product = ""
    if action_type == "conversion":
        conversion_products = {
            "credito": ("Crédito Pessoal", (5000, 50000)),
            "cartoes": ("Cartão Platinum", (0, 0)),
            "investimentos": ("Aplicação CDB", (1000, 100000)),
            "seguros": ("Seguro Auto", (800, 5000)),
            "conta_digital": ("Conta PJ", (0, 0)),
        }
        prod_info = conversion_products.get(campaign["category"], ("Produto", (0, 0)))
        conversion_product = prod_info[0]
        min_v, max_v = prod_info[1]
        conversion_value = round(random.uniform(min_v, max_v), 2) if max_v > 0 else 0.0

    return {
        "event_id": f"EVT-{uuid.uuid4().hex}",
        "event_timestamp": ts_str,
        "customer_id": session["customer_id"],
        "session_id": session["session_id"],
        "device_id": session["device_id"],
        "device_os": session["device_os"],
        "app_version": session["app_version"],
        "campaign_id": campaign["id"],
        "campaign_name": campaign["name"],
        "campaign_channel": campaign["channel"],
        "campaign_category": campaign["category"],
        "offer_id": f"OFR-{campaign['id'].split('-')[-1]}-{random.randint(1, 5):03d}",
        "action_type": action_type,
        "action_element": random.choice(ACTION_ELEMENTS) if action_type in ("click", "deep_link", "conversion") else None,
        "screen_name": screen,
        "referrer_screen": referrer,
        "geo_region": session["geo_region"],
        "geo_city": session["geo_city"],
        "is_personalized": random.random() < 0.65,
        "ab_test_group": random.choice(AB_GROUPS),
        "response_time_ms": random.randint(50, 800),
        "conversion_value": conversion_value,
        "conversion_product": conversion_product,
    }


def generate_campaign_metadata() -> list:
    base_date = datetime.utcnow()
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
            "budget_brl": c["budget"],
            "is_active": end.date() >= base_date.date(),
            "created_by": random.choice(["equipe_crm", "marketing_digital", "growth_team"]),
        })
    return rows


# ── Main ──

def main():
    args = parse_args()

    # Lê secrets via dbutils
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    client_id = dbutils.secrets.get(scope=args.secrets_scope, key="client-id")
    client_secret = dbutils.secrets.get(scope=args.secrets_scope, key="client-secret")
    print(f"Secrets carregados do scope '{args.secrets_scope}'")

    zerobus_url = f"https://{args.workspace_id}.zerobus.{args.region}.cloud.databricks.com"
    token_mgr = TokenManager(args.workspace_url, args.workspace_id,
                             client_id, client_secret, args.catalog, args.schema)

    tables = {
        "clicks": f"{args.catalog}.{args.schema}.bronze_crm_campaign_clicks",
        "metadata": f"{args.catalog}.{args.schema}.bronze_crm_campaigns_metadata",
        "sessions": f"{args.catalog}.{args.schema}.bronze_app_sessions",
    }

    # ── 1. Campanhas metadata (one-shot) ──
    print("\n=== Ingerindo metadados das campanhas ===")
    campaign_metadata = generate_campaign_metadata()
    token = token_mgr.get_token("bronze_crm_campaigns_metadata")
    zerobus_ingest_batch(zerobus_url, args.workspace_url, token,
                         tables["metadata"], campaign_metadata)
    print(f"  {len(campaign_metadata)} campanhas enviadas via ZeroBus")

    if args.continuous:
        run_continuous(args, zerobus_url, token_mgr, tables)
    else:
        run_oneshot(args, zerobus_url, token_mgr, tables)

    print("\nIngestão ZeroBus concluída!")


def run_continuous(args, zerobus_url, token_mgr, tables):
    """Modo contínuo: ~100 rps até max_clicks, com session flush periódico."""
    print(f"\n=== Modo contínuo: {args.target_rps} rps, stop em {args.max_clicks} clicks ===")

    session_pool = SessionPool(pool_size=50000)
    total_clicks = 0
    total_sessions = 0
    batch_count = 0
    sleep_interval = args.batch_size / args.target_rps  # 100/100 = 1.0s
    start_time = time.time()

    while total_clicks < args.max_clicks:
        batch_start = time.time()

        # Gera e envia batch de clicks
        remaining = args.max_clicks - total_clicks
        current_batch = min(args.batch_size, remaining)
        click_batch = [generate_click_event(session_pool) for _ in range(current_batch)]

        token = token_mgr.get_token("bronze_crm_campaign_clicks")
        zerobus_ingest_batch(zerobus_url, args.workspace_url, token,
                             tables["clicks"], click_batch)
        total_clicks += current_batch
        batch_count += 1

        # Flush sessões periodicamente
        if total_clicks % args.session_flush_interval < args.batch_size:
            sessions = session_pool.flush_sessions()
            if sessions:
                token_sess = token_mgr.get_token("bronze_app_sessions")
                # Envia sessões em sub-batches de 500
                for i in range(0, len(sessions), 500):
                    sub = sessions[i:i + 500]
                    zerobus_ingest_batch(zerobus_url, args.workspace_url,
                                         token_sess, tables["sessions"], sub)
                total_sessions += len(sessions)

        # Progress log a cada 10 batches
        if batch_count % 10 == 0:
            elapsed = time.time() - start_time
            rps = total_clicks / elapsed if elapsed > 0 else 0
            pct = (total_clicks / args.max_clicks) * 100
            print(f"  [{total_clicks:,}/{args.max_clicks:,}] {pct:.1f}% | "
                  f"{rps:.0f} rps | {total_sessions:,} sessions | "
                  f"{elapsed:.0f}s elapsed")

        # Throttle para atingir target rps
        batch_elapsed = time.time() - batch_start
        if batch_elapsed < sleep_interval:
            time.sleep(sleep_interval - batch_elapsed)

    # Flush final de sessões
    final_sessions = session_pool.flush_sessions()
    if final_sessions:
        token_sess = token_mgr.get_token("bronze_app_sessions")
        for i in range(0, len(final_sessions), 500):
            sub = final_sessions[i:i + 500]
            zerobus_ingest_batch(zerobus_url, args.workspace_url,
                                 token_sess, tables["sessions"], sub)
        total_sessions += len(final_sessions)

    elapsed = time.time() - start_time
    print(f"\n=== Ingestão contínua finalizada ===")
    print(f"  Clicks: {total_clicks:,}")
    print(f"  Sessions: {total_sessions:,}")
    print(f"  Tempo: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    print(f"  RPS médio: {total_clicks / elapsed:.0f}")


def run_oneshot(args, zerobus_url, token_mgr, tables):
    """Modo one-shot (legado): gera tudo e envia."""
    print(f"\n=== Modo one-shot: {args.num_events} eventos ===")

    session_pool = SessionPool(pool_size=10000)
    click_events = [generate_click_event(session_pool) for _ in range(args.num_events)]

    # Clicks
    token = token_mgr.get_token("bronze_crm_campaign_clicks")
    for i in range(0, len(click_events), args.batch_size):
        batch = click_events[i:i + args.batch_size]
        zerobus_ingest_batch(zerobus_url, args.workspace_url, token,
                             tables["clicks"], batch)
        print(f"  Clicks batch {i // args.batch_size + 1}: {len(batch)} enviados")
    print(f"  Total: {len(click_events)} clicks")

    # Sessions
    sessions = session_pool.flush_sessions()
    token_sess = token_mgr.get_token("bronze_app_sessions")
    for i in range(0, len(sessions), args.batch_size):
        batch = sessions[i:i + args.batch_size]
        zerobus_ingest_batch(zerobus_url, args.workspace_url, token_sess,
                             tables["sessions"], batch)
    print(f"  Total: {len(sessions)} sessions")


if __name__ == "__main__":
    main()
