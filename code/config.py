# =============================================================================
# Reglas de negocio implícitas para el pipeline de Credit & Debit Memos
# =============================================================================
import os
import pandas as pd

# -----------------------------------------------------------------------------
# Rutas
# -----------------------------------------------------------------------------
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "..", "Data bases")
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "..", "output")
STRONGHOLD_FILE = os.path.join(DATA_FOLDER, "Stronghold info.xlsx")

# -----------------------------------------------------------------------------
# Pipeline settings
# -----------------------------------------------------------------------------
SOURCE_SHEET = "Reference"
BATCH_SIZE = 20_000
USA_STRONGHOLD = "US-ACM"

# -----------------------------------------------------------------------------
# Mapeo de columnas del archivo fuente → nombre interno
# -----------------------------------------------------------------------------
COLUMN_MAP = {
    "SaTy": "sa_ty",           # Tipo de documento de ventas
    "Dv": "division",          # División de producto
    "SOrg.": "sorg",           # Sales Organization
    "SOff.": "sales_office",   # Sales Office (llave temporal para Stronghold)
    "SGrp": "sales_group",     # Sales Group (llave temporal para Stronghold)
    "Sold-to pt": "customer_id",  # ID del cliente
    "SD value": "net_value",   # Monto neto del memo
    "Created on": "created_on",  # Fecha de creación
}

# Alias de columna para archivos legacy (2020-2022)
LEGACY_COLUMN_ALIASES = {
    "SD Net value": "SD value",
}

# -----------------------------------------------------------------------------
# Clasificación de tipo de memo (sa_ty → target)
# Columna fuente: 'SaTy'
# Se usa en el paso F (Target Engineering) para distribuir net_value
# -----------------------------------------------------------------------------
CREDIT_MEMO_TYPES = ["ZCR", "ZICR"]
DEBIT_MEMO_TYPES = ["ZDR"]

# -----------------------------------------------------------------------------
# Mapeo de División (division)
# Columna fuente: 'Dv'
# -----------------------------------------------------------------------------
DIVISION_MAP = {
    2: "Agregados",
    3: "Concreto",
    4: "Asfalto",
    5: "Concratec products",
    6: "Liquid Asphalt",
}

# -----------------------------------------------------------------------------
# Stronghold mapping
# Llave: Sales Org. + Sales Office + Sales Group
# Trae: Region y Stronghold
# -----------------------------------------------------------------------------
def load_stronghold_map() -> pd.DataFrame:
    """Lee Stronghold info.xlsx y retorna mapeo único por SOrg+SOff+SGrp."""
    df = pd.read_excel(STRONGHOLD_FILE)
    keys = ["Sales Org.", "Sales Office", "Sales Group"]
    df = df[keys + ["Region", "Stronghold"]].drop_duplicates(subset=keys)
    df = df.rename(columns={
        "Sales Org.": "sorg",
        "Sales Office": "sales_office",
        "Sales Group": "sales_group",
        "Region": "region",
        "Stronghold": "stronghold",
    })
    return df


def get_division(dv: int) -> str:
    """Determina la división de producto a partir del código Dv."""
    return DIVISION_MAP.get(dv, "UNKNOWN")
