import os
import math
import numpy as np
import pandas as pd
from config import (
    COLUMN_MAP,
    CREDIT_MEMO_TYPES,
    DEBIT_MEMO_TYPES,
    DIVISION_MAP,
    get_division,
    load_stronghold_map,
)

# =============================================================================
# Rutas y configuración
# =============================================================================
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "..", "Data bases")
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "..", "output")
SOURCE_SHEET = "Reference"
BATCH_SIZE = 20_000


# =============================================================================
# Paso A — Carga y unificación de datos
# =============================================================================
def load_and_merge_sources(folder: str, sheet: str) -> pd.DataFrame:
    """Lee todos los .xlsx de la carpeta y concatena la pestaña indicada."""
    frames = []
    for file in sorted(os.listdir(folder)):
        if not file.endswith(".xlsx") or file.startswith("~$"):
            continue
        if "Stronghold" in file:
            continue
        path = os.path.join(folder, file)
        df = pd.read_excel(path, sheet_name=sheet)
        df["source_file"] = file
        frames.append(df)
        print(f"  Cargado: {file} → {len(df)} filas")

    if not frames:
        raise FileNotFoundError(
            f"No se encontraron archivos .xlsx en: {folder}"
        )

    combined = pd.concat(frames, ignore_index=True)
    n_before = len(combined)
    combined = combined.drop_duplicates(subset=["Sales doc."])
    n_duplicates = n_before - len(combined)
    print(f"  Unificado: {n_before} filas → {len(combined)} filas únicas "
          f"({n_duplicates} duplicados eliminados)")
    combined = combined.drop(columns=["source_file"])
    return combined


# =============================================================================
# Paso B — Validación de datos
# =============================================================================
def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Valida schema, rangos y valores esperados."""
    required_cols = list(COLUMN_MAP.keys())
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes en el archivo fuente: {missing}")

    non_numeric = pd.to_numeric(df["SD value"], errors="coerce").isna()
    n_non_numeric = non_numeric.sum()
    if n_non_numeric > 0:
        print(f"  ALERTA: {n_non_numeric} registros con SD value no numérico (eliminados)")
        df = df[~non_numeric].copy()

    if not df["SD value"].astype(float).ge(0).all():
        raise ValueError("Existen registros con SD value negativo")
    if df["Created on"].isna().any():
        raise ValueError("Existen fechas nulas en Created on")
    if not df["SaTy"].str.strip().isin(CREDIT_MEMO_TYPES + DEBIT_MEMO_TYPES).all():
        raise ValueError("Existen valores inesperados en SaTy")
    if not df["Dv"].isin(DIVISION_MAP.keys()).all():
        raise ValueError("Existen valores inesperados en Dv")

    print(f"  Validación OK — {len(df)} registros")
    return df


# =============================================================================
# Paso C — Limpieza y normalización
# =============================================================================
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Selecciona columnas útiles, renombra y castea tipos."""
    df = df[list(COLUMN_MAP.keys())].copy()
    df = df.rename(columns=COLUMN_MAP)

    df["sa_ty"] = df["sa_ty"].str.strip()
    df["created_on"] = pd.to_datetime(df["created_on"])
    df["net_value"] = pd.to_numeric(df["net_value"], errors="coerce").fillna(0)

    print(f"  Limpieza OK — {len(df)} registros "
          f"({(df['net_value'] == 0).sum()} con net_value=0 conservados)")
    return df


# =============================================================================
# Paso D — Enriquecimiento dimensional
# =============================================================================
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Merge con Stronghold info, mapea division código→nombre."""
    stronghold_map = load_stronghold_map()
    join_keys = ["sorg", "sales_office", "sales_group"]

    n_before = len(df)
    df = df.merge(stronghold_map, on=join_keys, how="left")
    df["division"] = df["division"].apply(get_division)

    n_unknown = df["stronghold"].isna().sum()
    if n_unknown > 0:
        print(f"  ALERTA: {n_unknown} registros sin match en Stronghold (eliminados)")
        df = df.dropna(subset=["stronghold"]).copy()

    df = df.drop(columns=join_keys)
    print(f"  Enriquecimiento OK — {n_before}→{len(df)} registros, "
          f"strongholds: {df['stronghold'].unique().tolist()}, "
          f"regiones: {df['region'].unique().tolist()}")
    return df


# =============================================================================
# Paso E — Filtrar solo USA
# =============================================================================
def filter_usa(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra el dataset para conservar solo registros USA (US-ACM)."""
    usa = df[df["stronghold"] == "US-ACM"].copy()
    n_excluded = len(df) - len(usa)
    print(f"  USA: {len(usa)} registros ({n_excluded} no-USA excluidos)")
    return usa


# =============================================================================
# Paso F — Target engineering
# =============================================================================
def engineer_targets(df: pd.DataFrame) -> pd.DataFrame:
    """Separa net_value en credit_net_value y debit_net_value según sa_ty."""
    df["credit_net_value"] = np.where(
        df["sa_ty"].isin(CREDIT_MEMO_TYPES), df["net_value"], 0
    )
    df["debit_net_value"] = np.where(
        df["sa_ty"].isin(DEBIT_MEMO_TYPES), df["net_value"], 0
    )
    df["month"] = df["created_on"].dt.to_period("M").astype(str)
    df = df.drop(columns=["sa_ty", "net_value", "created_on"])
    return df


# =============================================================================
# Procesamiento por batch
# =============================================================================
def process_in_batches(df: pd.DataFrame, batch_size: int) -> pd.DataFrame:
    """Aplica los pasos B, C, D en batches para controlar uso de memoria."""
    n_batches = math.ceil(len(df) / batch_size)
    print(f"  Procesando {len(df)} filas en {n_batches} batch(es) "
          f"de {batch_size} filas")

    processed = []
    for i in range(n_batches):
        start = i * batch_size
        end = min(start + batch_size, len(df))
        batch = df.iloc[start:end].copy()
        print(f"\n  --- Batch {i + 1}/{n_batches} (filas {start}–{end - 1}) ---")

        batch = validate(batch)
        batch = clean(batch)
        batch = enrich(batch)
        processed.append(batch)

    result = pd.concat(processed, ignore_index=True)
    print(f"\n  Total procesado: {len(result)} registros")
    return result


# =============================================================================
# Ejecución: Pasos A → F
# =============================================================================
def run():
    print("\n[A] Carga y unificación de datos")
    df = load_and_merge_sources(DATA_FOLDER, SOURCE_SHEET)

    print("\n[B-C-D] Validación, limpieza y enriquecimiento (por batch)")
    df = process_in_batches(df, BATCH_SIZE)

    print("\n[E] Filtrar solo USA")
    df = filter_usa(df)

    print("\n[F] Target engineering")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    df = engineer_targets(df)
    output_path = os.path.join(OUTPUT_FOLDER, "dataset_USA.csv")
    df.to_csv(output_path, index=False)
    print(f"  USA: {df.shape} → {output_path}")
    print(f"    Columnas: {list(df.columns)}")

    print("\nPipeline A→F completado.")


if __name__ == "__main__":
    run()
