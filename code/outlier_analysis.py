import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import OUTPUT_FOLDER

# =============================================================================
# Configuración
# =============================================================================
PLOTS_FOLDER = os.path.join(OUTPUT_FOLDER, "plots")

sns.set_theme(style="whitegrid", palette="muted")
TARGETS = ["credit_net_value", "debit_net_value"]
SIGMA_THRESHOLDS = [2, 3, 4]


def load_dataset(country: str) -> pd.DataFrame:
    path = os.path.join(OUTPUT_FOLDER, f"dataset_{country}.csv")
    return pd.read_csv(path)


# =============================================================================
# Estadísticas IQR
# =============================================================================
def print_outlier_stats(df: pd.DataFrame, col: str, label: str):
    """Calcula estadísticas de outliers usando el método IQR."""
    data = df[df[col] > 0][col]
    if data.empty:
        return
    q1 = data.quantile(0.25)
    q3 = data.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = data[(data < lower) | (data > upper)]

    print(f"\n  {label} — {col}")
    print(f"    Q1={q1:,.2f}  Q3={q3:,.2f}  IQR={iqr:,.2f}")
    print(f"    Límite inferior={max(0, lower):,.2f}  Límite superior={upper:,.2f}")
    print(f"    Outliers: {len(outliers)} de {len(data)} "
          f"({len(outliers)/len(data)*100:.1f}%)")
    if not outliers.empty:
        print(f"    Min outlier={outliers.min():,.2f}  "
              f"Max outlier={outliers.max():,.2f}")
    print(f"    Percentiles: p90={data.quantile(0.90):,.2f}  "
          f"p95={data.quantile(0.95):,.2f}  p99={data.quantile(0.99):,.2f}")


# =============================================================================
# Conteo por umbral Z-score (2σ, 3σ, 4σ)
# =============================================================================
def print_zscore_thresholds(df: pd.DataFrame, col: str, label: str):
    """Conteo de registros que exceden cada umbral de desviación estándar."""
    data = df[df[col] > 0][col]
    if data.empty:
        return

    mean = data.mean()
    std = data.std()

    print(f"\n  {label} — {col}")
    print(f"    Media={mean:,.2f}  Desv. Estándar={std:,.2f}")

    for sigma in SIGMA_THRESHOLDS:
        upper = mean + sigma * std
        count = (data > upper).sum()
        pct = count / len(data) * 100
        print(f"    > {sigma}σ (>{upper:,.2f}): {count} registros ({pct:.1f}%)")


def print_zscore_by_year(df: pd.DataFrame, col: str, country: str):
    """Conteo por umbral desglosado por año."""
    data = df[df[col] > 0].copy()
    if data.empty:
        return

    data["year"] = data["month"].str[:4]
    years = sorted(data["year"].unique())

    print(f"\n  {country} — {col} — Desglose por año")
    print(f"    {'Año':<6} {'N':>6} {'Media':>12} {'Std':>12}", end="")
    for sigma in SIGMA_THRESHOLDS:
        print(f"  {f'>{sigma}σ':>8}", end="")
    print()

    for year in years:
        subset = data[data["year"] == year][col]
        mean = subset.mean()
        std = subset.std()
        row = f"    {year:<6} {len(subset):>6} {mean:>12,.2f} {std:>12,.2f}"
        for sigma in SIGMA_THRESHOLDS:
            upper = mean + sigma * std
            count = (subset > upper).sum()
            row += f"  {count:>6} ({count/len(subset)*100:.0f}%)"
        print(row)


# =============================================================================
# Boxplots
# =============================================================================
def plot_boxplots(df: pd.DataFrame, country: str):
    """Genera boxplots para credit y debit net value."""

    # --- 1. Boxplot general (con outliers) ---
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"{country} — Distribución de Net Value (con outliers)",
                 fontsize=14)
    for ax, col in zip(axes, TARGETS):
        data = df[df[col] > 0][col]
        sns.boxplot(y=data, ax=ax, color="steelblue", width=0.4)
        ax.set_title(col)
        ax.set_ylabel("Monto ($)")
    plt.tight_layout()
    path = os.path.join(PLOTS_FOLDER, f"boxplot_{country}_full.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardado: {path}")

    # --- 2. Boxplot sin outliers extremos (zoom al cuerpo) ---
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"{country} — Distribución de Net Value (sin outliers extremos)",
                 fontsize=14)
    for ax, col in zip(axes, TARGETS):
        data = df[df[col] > 0][col]
        sns.boxplot(y=data, ax=ax, color="steelblue", width=0.4,
                    showfliers=False)
        ax.set_title(col)
        ax.set_ylabel("Monto ($)")
    plt.tight_layout()
    path = os.path.join(PLOTS_FOLDER, f"boxplot_{country}_zoom.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardado: {path}")

    # --- 3. Boxplot por división ---
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"{country} — Net Value por División", fontsize=14)
    for ax, col in zip(axes, TARGETS):
        data = df[df[col] > 0]
        sns.boxplot(x="division", y=col, data=data, ax=ax, width=0.5)
        ax.set_title(col)
        ax.set_ylabel("Monto ($)")
        ax.set_xlabel("División")
    plt.tight_layout()
    path = os.path.join(PLOTS_FOLDER, f"boxplot_{country}_by_division.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardado: {path}")

    # --- 4. Histograma con log scale ---
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(f"{country} — Histograma de Net Value (escala log)",
                 fontsize=14)
    for ax, col in zip(axes, TARGETS):
        data = df[df[col] > 0][col]
        ax.hist(data, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
        ax.set_xscale("log")
        ax.set_title(col)
        ax.set_xlabel("Monto ($) — escala log")
        ax.set_ylabel("Frecuencia")
    plt.tight_layout()
    path = os.path.join(PLOTS_FOLDER, f"histogram_{country}_log.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Guardado: {path}")


# =============================================================================
# Ejecución
# =============================================================================
def run():
    os.makedirs(PLOTS_FOLDER, exist_ok=True)
    country = "USA"
    print(f"\n{'='*60}")
    print(f"  ANÁLISIS DE OUTLIERS — {country}")
    print(f"{'='*60}")

    df = load_dataset(country)

    # --- Estadísticas IQR ---
    print("\n[Estadísticas IQR]")
    for col in TARGETS:
        print_outlier_stats(df, col, country)

    # --- Conteo Z-score consolidado ---
    print("\n[Conteo por umbral Z-score — Consolidado]")
    for col in TARGETS:
        print_zscore_thresholds(df, col, country)

    # --- Conteo Z-score por año ---
    print("\n[Conteo por umbral Z-score — Por año]")
    for col in TARGETS:
        print_zscore_by_year(df, col, country)

    # --- Gráficos ---
    print("\n[Gráficos]")
    plot_boxplots(df, country)

    print("\nAnálisis de outliers completado.")


if __name__ == "__main__":
    run()
