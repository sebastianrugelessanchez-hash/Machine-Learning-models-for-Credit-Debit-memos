# Credit & Debit Memos Forecasting Pipeline

Predictive pipeline for Credit & Debit Memos using SAP transactional data (2023–2025). Forecasts memo volumes by customer for USA and Canada markets to support Finance planning.

## Project Structure

```
Proyecto ML/
├── code/
│   ├── config.py              # Business rules, column mappings, Stronghold lookup
│   ├── data_cleaning.py       # Pipeline steps A→F (load, validate, clean, enrich, split, target)
│   └── outlier_analysis.py    # IQR/Z-score analysis and visualizations
├── Data Bases/                # Raw SAP exports (.xlsx) + Stronghold info mapping
├── Documentation/
│   ├── project_overview.txt   # High-level pipeline description (A→Q)
│   ├── pipeline.md            # Mermaid flowchart of the full architecture
│   └── pipeline_reference.md  # Quick reference table of pipeline steps
└── output/                    # Generated datasets and plots
    ├── dataset_USA.csv
    ├── dataset_CAM.csv
    └── plots/
```

## Pipeline Overview

The pipeline is divided into 17 steps (A→Q):

| Phase | Steps | Description | Status |
|-------|-------|-------------|--------|
| Data Preparation | A→F | Load, validate, clean, enrich, split by country, target engineering | Implemented |
| Feature Engineering | G→I | Monthly aggregation, lag/rolling features, categorical encoding | Planned |
| Model Selection | J→K | Lasso, LightGBM, XGBoost, CatBoost with walk-forward validation | Planned |
| Training & Forecast | L→P | Winner model training on full history, January 2026 forecast | Planned |
| Reporting | Q | Customer ranking, export to Finance | Planned |

### Steps A→F (Implemented)

- **A** — Load all `.xlsx` files from `Data Bases/`, concatenate the `Reference` sheet, deduplicate by `Sales doc.`
- **B** — Validate schema, data types, ranges (`SD value >= 0`), and expected values (`SaTy`, `Dv`)
- **C** — Select and rename columns, cast types (dates, numerics)
- **D** — Merge with `Stronghold info.xlsx` to get Region and Stronghold; map division codes to names
- **E** — Split into USA (`US-ACM`) and CAM (`E-CAN`, `W-CAN`) by Stronghold
- **F** — Separate `net_value` into `credit_net_value` and `debit_net_value` based on memo type; extract `month`

### Output Columns

| Column | Description |
|--------|-------------|
| `division` | Product division (Agregados, Concreto, Asfalto, etc.) |
| `customer_id` | Sold-to party ID |
| `region` | Operational region (GMA, GTA, NER, TX-LA, etc.) |
| `stronghold` | Market grouping (US-ACM, E-CAN, W-CAN) |
| `credit_net_value` | Memo amount when type is credit (ZCR, ZICR) |
| `debit_net_value` | Memo amount when type is debit (ZDR) |
| `month` | Transaction month (YYYY-MM) |

## Setup

### Requirements

- Python 3.10+
- pandas
- numpy
- openpyxl
- matplotlib
- seaborn

### Data Files

Place the following files in `Data Bases/`:
- `Credit and Debit Memos 2023.xlsx`
- `Credit and Debit Memos 2024.xlsx`
- `Credit and Debit Memos 2025.xlsx`
- `Stronghold info.xlsx`

### Running the Pipeline

```bash
cd code

# Run data cleaning pipeline (steps A→F)
python data_cleaning.py

# Run outlier analysis
python outlier_analysis.py
```

## Parametrized Execution

The model pipeline (steps G→Q) runs **4 independent instances**:

| Instance | Country | Target |
|----------|---------|--------|
| 1 | USA | credit_net_value |
| 2 | USA | debit_net_value |
| 3 | CAM | credit_net_value |
| 4 | CAM | debit_net_value |

Each instance aggregates data monthly, engineers features (lags 1/3/12, rolling means 3/6), and evaluates models independently.
