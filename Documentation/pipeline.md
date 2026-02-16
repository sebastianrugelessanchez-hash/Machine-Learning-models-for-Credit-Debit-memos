flowchart TD

    A[Raw Data<br/>SAP Credit & Debit Memos<br/>(2023–2025)<br/>Nivel transaccional] --> B[Data Validation]

    B --> B1[Schema checks<br/>Tipos esperados por columna]
    B --> B2[Rangos válidos<br/>Net value >= 0 · Fechas dentro de rango]
    B --> B3[Distribuciones<br/>Detección de anomalías y data drift]

    B --> C[Data Cleaning & Normalization]

    C --> C1[Drop columnas prohibidas<br/>Sales doc.<br/>Assignment<br/>Original Billing doc<br/>Document]
    C --> C2[Type casting<br/>Fechas / Numéricos]
    C --> C3[Validación moneda<br/>USD / CAD]
    C --> C4[Outlier treatment<br/>Log-transform o Winsorize<br/>en Net value]

    C --> D[Dimensional Enrichment]

    D --> D1[Join SOrg → Region]
    D --> D2[Map Dv → Division<br/>Agregados / Asfalto / Concreto / Cemento]
    D --> D3[Definir customer_id<br/>(Sold-to pt)]
    D --> D4[Sold-to party<br/>Solo metadata]

    D --> E[Split por País]

    E --> E1[USA Dataset - USD]
    E --> E2[CAM Dataset - CAD]

    %% PIPELINE PARAMETRIZADO (misma clase, distinta config)
    %% Se ejecuta 1 instancia por país × target = 4 ejecuciones
    %% USA credit · USA debit · CAM credit · CAM debit

    E1 --> F1[Target Engineering USA]
    E2 --> F2[Target Engineering CAM]

    F1 --> F1a[credit_net_value]
    F1 --> F1b[debit_net_value]
    F2 --> F2a[credit_net_value]
    F2 --> F2b[debit_net_value]

    %% Cada target entra al pipeline parametrizado de forma independiente
    F1a --> G1[MemoPipeline<br/>USA · Credit]
    F1b --> G2[MemoPipeline<br/>USA · Debit]
    F2a --> G3[MemoPipeline<br/>CAM · Credit]
    F2b --> G4[MemoPipeline<br/>CAM · Debit]

    %% PIPELINE GENÉRICO (se describe una vez, se ejecuta 4 veces)
    subgraph Pipeline Parametrizado
        direction TD

        H[Monthly Aggregation<br/>1 fila = cliente · región · mes<br/>Meses sin actividad = 0]

        H --> I[Feature Engineering]

        I --> I1[Lag Features<br/>lag_1 · lag_3 · lag_12]
        I --> I2[Rolling Window Features<br/>mean_3 · mean_6]
        I --> I3[Categorical Features<br/>region · customer_id · division]
        I --> I4[Ratio Features<br/>credit/debit histórico por cliente]

        I --> J[Baseline Naive<br/>Seasonal naive · Media móvil 3m<br/>Piso mínimo de referencia]

        J --> K[Model Selection<br/>Walk-forward Validation]

        K --> K1[Lasso Regression<br/>Baseline ML]
        K --> K2[LightGBM]
        K --> K3[XGBoost]
        K --> K4[CatBoost]

        K --> K5[RandomSearch<br/>wMAPE + MAE<br/>Estabilidad temporal]

        K5 --> L[Experiment Tracking<br/>Registro de hiperparámetros<br/>métricas y artefactos]

        L --> M[Modelo Ganador<br/>por país × target]

        M --> N[Hold-out Temporal<br/>Evaluación Final<br/>Se evalúa una sola vez]

        N --> O[Entrenamiento Final<br/>Todo el histórico]

        O --> P[Forecast Enero 2026]
    end

    G1 --> H
    G2 --> H
    G3 --> H
    G4 --> H

    %% OUTPUT
    P --> Q[Post-processing & Reporting]

    Q --> Q1[Join customer_id → Sold-to party]
    Q --> Q2[Ranking Top Clientes<br/>Créditos / Débitos]
    Q --> Q3[Output para Facturación & Finanzas]
    Q --> Q4[Versionado<br/>Dataset hash · Modelo serializado<br/>con metadata]
