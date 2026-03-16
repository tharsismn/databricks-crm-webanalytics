#!/bin/bash
# =============================================================================
# CRM WebAnalytics - Setup & Execution Script
# Orquestra o deploy e execução de todo o pipeline.
#
# Uso:
#   ./setup.sh \
#     --workspace-url "https://my-workspace.cloud.databricks.com" \
#     --workspace-id "1234567890" \
#     --cloud-region "us-west-2" \
#     --catalog "main" \
#     --schema "crm_webanalytics" \
#     --secrets-scope "crm-zerobus-sp" \
#     --lakebase-catalog "crm_app_lakebase" \
#     --profile "DEFAULT"
#
# Pré-requisitos:
#   - Databricks CLI autenticado
#   - Secrets configurados (client-id, client-secret, lakebase-password)
#   - Lakebase instância criada com database e role
#   - Node.js >= 18 instalado
#   - Frontend buildado (npm install && npm run build em app/frontend)
# =============================================================================

set -euo pipefail

# ── Defaults ──
CATALOG="main"
SCHEMA="crm_webanalytics"
WORKSPACE_URL=""
WORKSPACE_ID=""
CLOUD_REGION="us-west-2"
SECRETS_SCOPE="crm-zerobus-sp"
LAKEBASE_CATALOG="crm_app_lakebase"
PROFILE="DEFAULT"
TARGET="dev"
SKIP_BUILD=false
SKIP_DEPLOY=false

# ── Parse args ──
while [[ $# -gt 0 ]]; do
  case $1 in
    --workspace-url) WORKSPACE_URL="$2"; shift 2;;
    --workspace-id) WORKSPACE_ID="$2"; shift 2;;
    --cloud-region) CLOUD_REGION="$2"; shift 2;;
    --catalog) CATALOG="$2"; shift 2;;
    --schema) SCHEMA="$2"; shift 2;;
    --secrets-scope) SECRETS_SCOPE="$2"; shift 2;;
    --lakebase-catalog) LAKEBASE_CATALOG="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --target) TARGET="$2"; shift 2;;
    --skip-build) SKIP_BUILD=true; shift;;
    --skip-deploy) SKIP_DEPLOY=true; shift;;
    *) echo "Parâmetro desconhecido: $1"; exit 1;;
  esac
done

# ── Validação ──
if [[ -z "$WORKSPACE_URL" || -z "$WORKSPACE_ID" ]]; then
  echo "ERRO: --workspace-url e --workspace-id são obrigatórios."
  echo ""
  echo "Uso: ./setup.sh --workspace-url <URL> --workspace-id <ID> [opções]"
  echo ""
  echo "Opções:"
  echo "  --cloud-region     Região cloud (default: us-west-2)"
  echo "  --catalog          Catálogo Unity Catalog (default: main)"
  echo "  --schema           Schema para todas as tabelas (default: crm_webanalytics)"
  echo "  --secrets-scope    Scope de secrets (default: crm-zerobus-sp)"
  echo "  --lakebase-catalog Catálogo UC do Lakebase (default: crm_app_lakebase)"
  echo "  --profile          Perfil do Databricks CLI (default: DEFAULT)"
  echo "  --target           Bundle target: dev/staging/prod (default: dev)"
  echo "  --skip-build       Pular build do frontend"
  echo "  --skip-deploy      Pular bundle deploy (usar deploy existente)"
  exit 1
fi

CLI_PROFILE="--profile $PROFILE"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=============================================="
echo "  CRM WebAnalytics - Setup & Execution"
echo "=============================================="
echo "  Workspace:  $WORKSPACE_URL"
echo "  Workspace ID: $WORKSPACE_ID"
echo "  Region:     $CLOUD_REGION"
echo "  Catalog:    $CATALOG"
echo "  Schema:     $SCHEMA"
echo "  Secrets:    $SECRETS_SCOPE"
echo "  Lakebase:   $LAKEBASE_CATALOG"
echo "  Target:     $TARGET"
echo "=============================================="
echo ""

# ── Step 1: Build frontend ──
if [[ "$SKIP_BUILD" == false ]]; then
  echo "[1/6] Building frontend..."
  cd "$SCRIPT_DIR/app/frontend"
  npm install --silent
  npm run build
  cd "$SCRIPT_DIR"
  echo "       Frontend build OK."
else
  echo "[1/6] Skipping frontend build (--skip-build)."
fi
echo ""

# ── Step 2: Bundle deploy ──
if [[ "$SKIP_DEPLOY" == false ]]; then
  echo "[2/6] Deploying Databricks bundle (target: $TARGET)..."
  databricks bundle deploy -t "$TARGET" $CLI_PROFILE \
    --var="catalog=$CATALOG" \
    --var="schema=$SCHEMA" \
    --var="lakebase_catalog=$LAKEBASE_CATALOG" \
    --var="workspace_url=$WORKSPACE_URL" \
    --var="workspace_id=$WORKSPACE_ID" \
    --var="cloud_region=$CLOUD_REGION" \
    --var="secrets_scope=$SECRETS_SCOPE"
  echo "       Bundle deploy OK."
else
  echo "[2/6] Skipping bundle deploy (--skip-deploy)."
fi
echo ""

# ── Step 3: Run Bronze ingestion job ──
echo "[3/6] Starting Bronze ingestion job..."
BRONZE_JOB_ID=$(databricks jobs list $CLI_PROFILE --output json | \
  python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(next((str(j['job_id']) for j in jobs if 'Bronze Setup' in j.get('settings',{}).get('name','')), ''))")

if [[ -z "$BRONZE_JOB_ID" ]]; then
  echo "ERRO: Job Bronze não encontrado. Verifique se o bundle deploy foi executado."
  exit 1
fi

echo "       Job ID: $BRONZE_JOB_ID"
BRONZE_RUN=$(databricks jobs run-now "$BRONZE_JOB_ID" $CLI_PROFILE --output json | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "       Run ID: $BRONZE_RUN"

# Aguardar setup_tables terminar e zerobus_ingest iniciar
echo "       Aguardando setup_tables terminar e ingestão iniciar..."
while true; do
  TASKS_STATE=$(databricks api get "/api/2.1/jobs/runs/get?run_id=$BRONZE_RUN" $CLI_PROFILE 2>/dev/null | \
    python3 -c "
import sys,json
d=json.load(sys.stdin)
tasks={t['task_key']:t['state'] for t in d.get('tasks',[])}
setup=tasks.get('setup_tables',{})
ingest=tasks.get('zerobus_ingest',{})
print(f\"{setup.get('life_cycle_state','PENDING')}|{setup.get('result_state','PENDING')}|{ingest.get('life_cycle_state','PENDING')}\")
" 2>/dev/null || echo "PENDING|PENDING|PENDING")

  SETUP_STATE=$(echo "$TASKS_STATE" | cut -d'|' -f1)
  SETUP_RESULT=$(echo "$TASKS_STATE" | cut -d'|' -f2)
  INGEST_STATE=$(echo "$TASKS_STATE" | cut -d'|' -f3)

  if [[ "$SETUP_RESULT" == "FAILED" ]]; then
    echo "ERRO: Task setup_tables falhou."
    exit 1
  fi

  if [[ "$INGEST_STATE" == "RUNNING" ]]; then
    echo "       setup_tables: SUCCESS | zerobus_ingest: RUNNING"
    break
  fi

  echo "       setup_tables: $SETUP_STATE ($SETUP_RESULT) | zerobus_ingest: $INGEST_STATE"
  sleep 10
done
echo ""

# ── Step 4: Trigger DLT full refresh + set continuous mode ──
echo "[4/6] Configuring and starting DLT pipeline..."

PIPELINE_ID=$(databricks pipelines list-pipelines $CLI_PROFILE --output json | \
  python3 -c "import sys,json; pipes=json.load(sys.stdin).get('statuses',[]); print(next((p['pipeline_id'] for p in pipes if 'Medallion' in p.get('name','')), ''))")

if [[ -z "$PIPELINE_ID" ]]; then
  echo "ERRO: Pipeline DLT não encontrado."
  exit 1
fi

echo "       Pipeline ID: $PIPELINE_ID"

# Configurar continuous + production
echo "       Configurando continuous=true, development=false..."
databricks api get "/api/2.0/pipelines/$PIPELINE_ID" $CLI_PROFILE 2>/dev/null | \
  python3 -c "
import sys,json
d=json.load(sys.stdin)
spec=d['spec']
spec['continuous']=True
spec['development']=False
with open('/tmp/dlt_spec.json','w') as f:
    json.dump(spec,f)
"
databricks api put "/api/2.0/pipelines/$PIPELINE_ID" --json @/tmp/dlt_spec.json $CLI_PROFILE 2>/dev/null
rm -f /tmp/dlt_spec.json

# Disparar full refresh
echo "       Disparando full refresh..."
databricks api post "/api/2.0/pipelines/$PIPELINE_ID/updates" --json '{"full_refresh": true}' $CLI_PROFILE 2>/dev/null || \
  echo "       (Pipeline já possui update ativo, continuando...)"

# Aguardar DLT processar dados (espera até pelo menos uma tabela gold ter dados)
echo "       Aguardando DLT processar dados (isso pode levar alguns minutos)..."
WAIT_COUNT=0
MAX_WAIT=60  # 10 min max
while [[ $WAIT_COUNT -lt $MAX_WAIT ]]; do
  DLT_STATE=$(databricks api get "/api/2.0/pipelines/$PIPELINE_ID" $CLI_PROFILE 2>/dev/null | \
    python3 -c "
import sys,json
d=json.load(sys.stdin)
state=d.get('state','UNKNOWN')
updates=d.get('latest_updates',[])
u_state=updates[0].get('state','') if updates else ''
print(f'{state}|{u_state}')
" 2>/dev/null || echo "UNKNOWN|UNKNOWN")

  PIPE_STATE=$(echo "$DLT_STATE" | cut -d'|' -f1)
  UPDATE_STATE=$(echo "$DLT_STATE" | cut -d'|' -f2)

  if [[ "$PIPE_STATE" == "RUNNING" && ("$UPDATE_STATE" == "RUNNING" || "$UPDATE_STATE" == "COMPLETED") ]]; then
    echo "       Pipeline: $PIPE_STATE | Update: $UPDATE_STATE"
    # Dar mais tempo para os dados serem processados
    echo "       Aguardando 60s para garantir que Gold tables tenham dados..."
    sleep 60
    break
  fi

  echo "       Pipeline: $PIPE_STATE | Update: $UPDATE_STATE (aguardando...)"
  sleep 10
  WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [[ $WAIT_COUNT -ge $MAX_WAIT ]]; then
  echo "AVISO: Timeout aguardando DLT. Continuando mesmo assim..."
fi
echo ""

# ── Step 5: Run Synced Tables job ──
echo "[5/6] Running Synced Tables job (Gold -> Lakebase)..."
SYNC_JOB_ID=$(databricks jobs list $CLI_PROFILE --output json | \
  python3 -c "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); print(next((str(j['job_id']) for j in jobs if 'Lakebase Sync' in j.get('settings',{}).get('name','')), ''))")

if [[ -z "$SYNC_JOB_ID" ]]; then
  echo "ERRO: Job Synced Tables não encontrado."
  exit 1
fi

echo "       Job ID: $SYNC_JOB_ID"
SYNC_RUN=$(databricks jobs run-now "$SYNC_JOB_ID" $CLI_PROFILE --output json | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "       Run ID: $SYNC_RUN"

# Aguardar conclusão
echo "       Aguardando synced tables serem criadas..."
while true; do
  SYNC_STATE=$(databricks api get "/api/2.1/jobs/runs/get?run_id=$SYNC_RUN" $CLI_PROFILE 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('state',{}); print(f\"{s.get('life_cycle_state','?')}|{s.get('result_state','?')}\")" 2>/dev/null || echo "PENDING|PENDING")

  LC_STATE=$(echo "$SYNC_STATE" | cut -d'|' -f1)
  RESULT=$(echo "$SYNC_STATE" | cut -d'|' -f2)

  if [[ "$LC_STATE" == "TERMINATED" ]]; then
    if [[ "$RESULT" == "SUCCESS" ]]; then
      echo "       Synced tables criadas com sucesso."
    else
      echo "ERRO: Job synced tables falhou com resultado: $RESULT"
      exit 1
    fi
    break
  fi

  echo "       Estado: $LC_STATE..."
  sleep 10
done
echo ""

# ── Step 6: Deploy Databricks App ──
echo "[6/6] Deploying Databricks App..."

# Detectar email do usuário para path do workspace
USER_EMAIL=$(databricks current-user me $CLI_PROFILE --output json 2>/dev/null | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))" 2>/dev/null || echo "")

if [[ -z "$USER_EMAIL" ]]; then
  echo "AVISO: Não foi possível detectar o email do usuário."
  echo "       Deploy do app precisa ser feito manualmente:"
  echo "       databricks apps create crm-campaign-monitor"
  echo "       databricks apps deploy crm-campaign-monitor --source-code-path /Workspace/Users/<email>/.bundle/crm-webanalytics-ingest/$TARGET/files/app"
else
  APP_SOURCE="/Workspace/Users/$USER_EMAIL/.bundle/crm-webanalytics-ingest/$TARGET/files/app"

  # Criar app se não existir
  databricks apps get crm-campaign-monitor $CLI_PROFILE 2>/dev/null || \
    databricks apps create crm-campaign-monitor $CLI_PROFILE 2>/dev/null || true

  databricks apps deploy crm-campaign-monitor --source-code-path "$APP_SOURCE" $CLI_PROFILE
  echo "       App deployed."
fi
echo ""

# ── Done ──
echo "=============================================="
echo "  Setup completo!"
echo "=============================================="
echo ""
echo "  ZeroBus:      Gerando dados (~100 rps)"
echo "  DLT:          Contínuo, processando Silver + Gold"
echo "  Synced Tables: CONTINUOUS, replicando para Lakebase"
echo "  App:          Dashboard atualiza a cada 1s"
echo ""
echo "  Para acessar o app:"
echo "  https://<workspace>/apps/crm-campaign-monitor"
echo "=============================================="
