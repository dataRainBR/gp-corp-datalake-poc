"""
Configuração centralizada para jobs Bronze → Silver.
Otimizado para custo: Glue 3.0+ com auto-scaling, formato Iceberg, particionamento inteligente.
"""

# S3 paths
S3_BUCKET = "gpcorp-datalake"
BRONZE_PREFIX = f"s3://{S3_BUCKET}/Bronze"
SILVER_PREFIX = f"s3://{S3_BUCKET}/Silver"

# Glue Catalog
CATALOG_DATABASE_SILVER = "gpcorp_silver"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/silver/warehouse"

# Iceberg config
ICEBERG_CATALOG = "glue_catalog"

# Entidades e suas configurações
ENTITIES = {
    "BusinessPartners": {
        "bronze_path": f"{BRONZE_PREFIX}/BusinessPartners/",
        "silver_table": "business_partners",
        "primary_key": ["CardCode"],
        "partition_by": [],  # dimensão pequena, sem partição
        "scd_type": 2,
        "pii_columns": ["Phone1", "Phone2", "Cellular", "FederalTaxID"],
    },
    "Items": {
        "bronze_path": f"{BRONZE_PREFIX}/Items/",
        "silver_table": "items",
        "primary_key": ["ItemCode"],
        "partition_by": [],
        "scd_type": 2,
        "pii_columns": [],
    },
    "ItemGroups": {
        "bronze_path": f"{BRONZE_PREFIX}/ItemGroups/",
        "silver_table": "item_groups",
        "primary_key": ["Number"],
        "partition_by": [],
        "scd_type": 1,  # referência estática
        "pii_columns": [],
    },
    "SalesPersons": {
        "bronze_path": f"{BRONZE_PREFIX}/SalesPersons/",
        "silver_table": "sales_persons",
        "primary_key": ["SalesEmployeeCode"],
        "partition_by": [],
        "scd_type": 2,
        "pii_columns": ["Phone", "Mobile", "Email"],
    },
    "Invoices": {
        "bronze_path": f"{BRONZE_PREFIX}/Invoices/",
        "silver_table": "invoices",
        "primary_key": ["DocEntry", "LineNum"],
        "partition_by": ["year", "month", "day"],
        "scd_type": 1,  # fato — append/overwrite
        "pii_columns": [],
    },
    "Orders": {
        "bronze_path": f"{BRONZE_PREFIX}/Orders/",
        "silver_table": "orders",
        "primary_key": ["DocEntry", "LineNum"],
        "partition_by": ["year", "month", "day"],
        "scd_type": 1,
        "pii_columns": [],
    },
    "Quotations": {
        "bronze_path": f"{BRONZE_PREFIX}/Quotations/",
        "silver_table": "quotations",
        "primary_key": ["DocEntry", "LineNum"],
        "partition_by": ["year", "month", "day"],
        "scd_type": 1,
        "pii_columns": [],
    },
    "InventoryGenEntries": {
        "bronze_path": f"{BRONZE_PREFIX}/InventoryGenEntries/",
        "silver_table": "inventory_gen_entries",
        "primary_key": ["DocEntry", "LineNum"],
        "partition_by": ["year", "month", "day"],
        "scd_type": 1,
        "pii_columns": [],
    },
}

# Glue Job defaults (custo-benefício)
GLUE_JOB_DEFAULTS = {
    "GlueVersion": "4.0",
    "WorkerType": "G.1X",  # menor worker possível
    "NumberOfWorkers": 2,   # mínimo com auto-scaling
    "Timeout": 60,          # timeout 1h
    "DefaultArguments": {
        "--enable-auto-scaling": "true",
        "--enable-continuous-cloudwatch-log": "false",  # reduz custo
        "--enable-metrics": "true",
        "--conf": "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--datalake-formats": "iceberg",
    },
}
