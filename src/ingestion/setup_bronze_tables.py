"""
Setup Bronze Tables - CRM WebAnalytics
Cria o schema e as tabelas Delta na camada Bronze.
Essas tabelas são o destino do ZeroBus -- o producer envia dados
via REST API e o ZeroBus pousa automaticamente nas tabelas.
Também concede permissões ao Service Principal para o ZeroBus funcionar.
"""

import argparse
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--client-id", required=True,
                        help="Service Principal Application ID para GRANT")
    return parser.parse_args()


def setup_bronze_tables(spark: SparkSession, catalog: str, schema: str, client_id: str):
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Drop tabelas existentes para recriar com schema limpo
    for t in ["bronze_crm_campaign_clicks", "bronze_crm_campaigns_metadata", "bronze_app_sessions"]:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t}")

    # ── Tabela Bronze: eventos brutos de clicks em campanhas CRM ──
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_crm_campaign_clicks (
            event_id              STRING        COMMENT 'ID único do evento (UUID)',
            event_timestamp       STRING        COMMENT 'Timestamp ISO do evento capturado pelo app',
            customer_id           STRING        COMMENT 'ID anonimizado do cliente do banco',
            session_id            STRING        COMMENT 'ID da sessão do mobile app',
            device_id             STRING        COMMENT 'ID do dispositivo móvel',
            device_os             STRING        COMMENT 'Sistema operacional (iOS/Android)',
            app_version           STRING        COMMENT 'Versão do app mobile banking',
            campaign_id           STRING        COMMENT 'ID da campanha CRM',
            campaign_name         STRING        COMMENT 'Nome descritivo da campanha',
            campaign_channel      STRING        COMMENT 'Canal (push, in_app_banner, in_app_modal, story)',
            campaign_category     STRING        COMMENT 'Categoria (credito, investimentos, seguros, cartoes, conta_digital)',
            offer_id              STRING        COMMENT 'ID da oferta vinculada à campanha',
            action_type           STRING        COMMENT 'Tipo de ação (impression, click, dismiss, swipe, deep_link)',
            action_element        STRING        COMMENT 'Elemento UI clicado (banner, button_cta, card, carousel_item)',
            screen_name           STRING        COMMENT 'Tela do app onde o evento ocorreu',
            referrer_screen       STRING        COMMENT 'Tela anterior à interação',
            geo_region            STRING        COMMENT 'Região geográfica (UF)',
            geo_city              STRING        COMMENT 'Cidade do cliente',
            is_personalized       BOOLEAN       COMMENT 'Se a campanha foi personalizada via ML',
            ab_test_group         STRING        COMMENT 'Grupo do teste A/B (control, variant_a, variant_b)',
            response_time_ms      INT           COMMENT 'Tempo de resposta do servidor em ms',
            conversion_value      DOUBLE        COMMENT 'Valor da conversão em BRL (quando action_type=conversion)',
            conversion_product    STRING        COMMENT 'Produto convertido (quando action_type=conversion)'
        )
        USING DELTA
        COMMENT 'Bronze: eventos brutos de clicks em campanhas CRM ingeridos via ZeroBus REST API'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'quality' = 'bronze'
        )
    """)

    # ── Tabela Bronze: metadados das campanhas CRM ──
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_crm_campaigns_metadata (
            campaign_id           STRING        COMMENT 'ID único da campanha CRM',
            campaign_name         STRING        COMMENT 'Nome da campanha',
            campaign_description  STRING        COMMENT 'Descrição da campanha',
            campaign_category     STRING        COMMENT 'Categoria do produto oferecido',
            campaign_channel      STRING        COMMENT 'Canal de veiculação',
            campaign_start_date   STRING        COMMENT 'Data de início (ISO)',
            campaign_end_date     STRING        COMMENT 'Data de encerramento (ISO)',
            target_segment        STRING        COMMENT 'Segmento alvo (varejo, alta_renda, private, pj)',
            budget_brl            DOUBLE        COMMENT 'Orçamento da campanha em BRL',
            is_active             BOOLEAN       COMMENT 'Se a campanha está ativa',
            created_by            STRING        COMMENT 'Equipe/usuário criador'
        )
        USING DELTA
        COMMENT 'Bronze: metadados das campanhas CRM ingeridos via ZeroBus REST API'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'quality' = 'bronze'
        )
    """)

    # ── Tabela Bronze: sessões do mobile app ──
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_app_sessions (
            session_id            STRING        COMMENT 'ID da sessão',
            customer_id           STRING        COMMENT 'ID anonimizado do cliente',
            device_id             STRING        COMMENT 'ID do dispositivo',
            device_os             STRING        COMMENT 'Sistema operacional',
            app_version           STRING        COMMENT 'Versão do app',
            session_start         STRING        COMMENT 'Início da sessão (ISO)',
            session_end           STRING        COMMENT 'Fim da sessão (ISO)',
            geo_region            STRING        COMMENT 'UF',
            geo_city              STRING        COMMENT 'Cidade',
            total_events          INT           COMMENT 'Total de eventos na sessão',
            total_clicks          INT           COMMENT 'Total de clicks em campanhas'
        )
        USING DELTA
        COMMENT 'Bronze: sessões do mobile banking app ingeridas via ZeroBus REST API'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'quality' = 'bronze'
        )
    """)

    # ── GRANT permissões ao Service Principal para ZeroBus ──
    tables = ["bronze_crm_campaign_clicks", "bronze_crm_campaigns_metadata", "bronze_app_sessions"]
    sp = f"`{client_id}`"

    spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO {sp}")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO {sp}")
    for t in tables:
        spark.sql(f"GRANT MODIFY, SELECT ON TABLE {catalog}.{schema}.{t} TO {sp}")

    print(f"Bronze tables created and permissions granted in {catalog}.{schema}")


if __name__ == "__main__":
    args = parse_args()
    spark = SparkSession.builder.appName("CRM-WebAnalytics-BronzeSetup").getOrCreate()
    setup_bronze_tables(spark, args.catalog, args.schema, args.client_id)
