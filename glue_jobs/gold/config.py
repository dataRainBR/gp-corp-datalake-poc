"""
Configuração da camada Gold — Agregações e Feature Tables.
Consome Silver (Iceberg) e gera tabelas otimizadas para QuickSight e ML.
Organização por domínio de negócio — nomenclatura em português.
"""

# Glue Catalog
CATALOG_DATABASE_SILVER = "gpcorp_silver"
ICEBERG_CATALOG = "glue_catalog"

# S3 base
S3_BUCKET = "gpcorp-datalake"

# ═══════════════════════════════════════════════════════════════
# Databases Gold por domínio
# ═══════════════════════════════════════════════════════════════

GOLD_DATABASES = {
    "vendas": {
        "database": "gpcorp_gold_vendas",
        "warehouse_path": f"s3://{S3_BUCKET}/Gold/gpcorp_gold_vendas.db/",
        "description": "Agregações comerciais — faturamento, ranking, mix",
    },
    "cotacoes": {
        "database": "gpcorp_gold_cotacoes",
        "warehouse_path": f"s3://{S3_BUCKET}/Gold/gpcorp_gold_cotacoes.db/",
        "description": "Análise de pipeline e conversão (win-rate)",
    },
    "cadastros": {
        "database": "gpcorp_gold_cadastros",
        "warehouse_path": f"s3://{S3_BUCKET}/Gold/gpcorp_gold_cadastros.db/",
        "description": "Features de clientes e crédito",
    },
    "estoque": {
        "database": "gpcorp_gold_estoque",
        "warehouse_path": f"s3://{S3_BUCKET}/Gold/gpcorp_gold_estoque.db/",
        "description": "Análise de inventário e movimentação",
    },
}

# ═══════════════════════════════════════════════════════════════
# Tabelas Gold — nomes em português
# ═══════════════════════════════════════════════════════════════

GOLD_TABLES = {
    # ─── Domínio: Vendas ───
    "vendas_detalhada": {
        "domain": "vendas",
        "description": "Grão linha de NF — base para drill-down nos dashboards",
        "partition_by": ["ano", "mes", "dia"],
    },
    "faturamento_mensal": {
        "domain": "vendas",
        "description": "Faturamento agregado por mês, vendedor e cliente",
        "partition_by": ["ano", "mes"],
    },
    "ranking_vendedores": {
        "domain": "vendas",
        "description": "Ranking de vendedores por período com métricas comerciais",
        "partition_by": ["ano", "mes"],
    },
    "vendas_por_produto": {
        "domain": "vendas",
        "description": "Vendas agregadas por item/grupo com tendência",
        "partition_by": ["ano", "mes"],
    },
    # ─── Domínio: Cotações ───
    "taxa_conversao": {
        "domain": "cotacoes",
        "description": "Dashboard: volume e taxa de conversão por vendedor e linha de produto",
        "partition_by": [],
    },
    "features_predicao_conversao": {
        "domain": "cotacoes",
        "description": "Features ML: modelo preditivo de conversão cotação→pedido",
        "partition_by": [],
    },
    # ─── Domínio: Cadastros ───
    "analise_credito": {
        "domain": "cadastros",
        "description": "Features de crédito extraídas do FreeText (NLP)",
        "partition_by": [],
    },
    "perfil_cliente": {
        "domain": "cadastros",
        "description": "Perfil RFV do cliente: recência, frequência, valor, segmentação ABC",
        "partition_by": [],
    },
    "catalogo_produtos_ativo": {
        "domain": "cadastros",
        "description": "Catálogo de produtos enriquecido com métricas de vendas (ativo/inativo, preço praticado)",
        "partition_by": [],
    },
    "cobertura_vendedor": {
        "domain": "cadastros",
        "description": "Carteira e cobertura por vendedor: clientes ativos, inativos, mix, receita",
        "partition_by": [],
    },
    # ─── Domínio: Estoque (fase 2) ───
    "movimentacao_estoque": {
        "domain": "estoque",
        "description": "Entradas e saídas de estoque por item/depósito",
        "partition_by": ["ano", "mes"],
    },
}


def get_gold_table_path(table_name: str) -> str:
    """Retorna o full table name com catalog e database corretos."""
    cfg = GOLD_TABLES[table_name]
    domain = cfg["domain"]
    database = GOLD_DATABASES[domain]["database"]
    return f"{ICEBERG_CATALOG}.{database}.{table_name}"


def get_gold_warehouse(domain: str) -> str:
    """Retorna o warehouse path para um domínio."""
    return GOLD_DATABASES[domain]["warehouse_path"]
