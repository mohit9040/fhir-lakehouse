#FHIr source 
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"


# Order matters: Encounter/Observation/Condition all reference Patient,
# so Patient must land first. This list drives both ingestion loops
# and the Databricks Workflow task dependency chain.
FHIR_RESOURCES = ["Patient", "Encounter", "Observation", "Condition"]


# Page size per FHIR search request (HAPI public server caps around 100-200)
PAGE_SIZE = 100

# Business keys used for SCD2 matching per resource
BUSINESS_KEY = {
    "Patient": "id",
    "Encounter": "id",
    "Observation": "id",
    "Condition": "id",
}

#For unity Catalog
CATALOG = "fhir_lakehouse"          # Unity Catalog catalog name

RAW_SCHEMA = "raw"                  # raw JSON landing (Volumes)
BRONZE_SCHEMA = "bronze"            # bronze Delta tables
SILVER_SCHEMA = "silver"            # silver Delta tables (SCD2)
GOLD_SCHEMA = "gold"                # gold Delta tables / views
CONTROL_SCHEMA = "control"          # watermarks, run logs

# Unity Catalog Volume for raw, immutable API responses (JSON as-is)
RAW_VOLUME = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/raw_data"

def raw_path(resource: str, run_date: str) -> str:
    """Folder layout: /Volumes/.../raw_data/<Resource>/<yyyy-mm-dd>/"""
    return f"{RAW_VOLUME}/{resource}/{run_date}"

def bronze_table(resource: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{resource.lower()}_bronze"

def silver_table(resource: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{resource.lower()}"

def gold_table(name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"

WATERMARK_TABLE = f"{CATALOG}.{CONTROL_SCHEMA}.ingestion_watermark"
RUN_LOG_TABLE = f"{CATALOG}.{CONTROL_SCHEMA}.pipeline_run_log"

BACKFILL_DAYS = 3   # "fetch data incrementally for 2-3 days" per assignment
