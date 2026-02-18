flowchart TD

    A[Raw Data<br/>SAP Credit & Debit Memos<br/>(2020–2025)<br/>Nivel transaccional] --> B[Data Validation]

    B --> B1[Schema checks<br/>Tipos esperados por columna]
    B --> B2[Rangos válidos<br/>Net value >= 0 · Fechas dentro de rango]
    B --> B3[Distribuciones<br/>Detección de anomalías y data drift]

    B --> C[Data Cleaning & Normalization]

    C --> C1[Drop columnas prohibidas<br/>Sales doc.<br/>Assignment<br/>Original Billing doc<br/>Document]
    C --> C2[Type casting<br/>Fechas / Numéricos]
    C --> C3[Outlier treatment<br/>Log-transform o Winsorize<br/>en Net value]

    C --> D[Dimensional Enrichment]

    D --> D1[Merge SOrg+SOff+SGrp → Region + Stronghold]
    D --> D2[Map Dv → Division<br/>Agregados / Asfalto / Concreto]
    D --> D3[Definir customer_id<br/>(Sold-to pt)]

    D --> E[Filter USA<br/>Stronghold = US-ACM]

    E --> F[Target Engineering]

    F --> F1[credit_net_value]
    F --> F2[debit_net_value]

    %% Cada target entra al pipeline parametrizado de forma independiente
    F1 --> G1[MemoPipeline<br/>USA · Credit]
    F2 --> G2[MemoPipeline<br/>USA · Debit]

    %% PIPELINE GENÉRICO (se describe una vez, se ejecuta 2 veces)
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

        L --> M[Modelo Ganador<br/>por target]

        M --> N[Hold-out Temporal<br/>Evaluación Final<br/>Se evalúa una sola vez]

        N --> O[Entrenamiento Final<br/>Todo el histórico]

        O --> P[Forecast Enero 2026]
    end

    G1 --> H
    G2 --> H

    %% OUTPUT
    P --> Q[Post-processing & Reporting]

    Q --> Q1[Join customer_id → Sold-to party]
    Q --> Q2[Ranking Top Clientes<br/>Créditos / Débitos]
    Q --> Q3[Output para Facturación & Finanzas]
    Q --> Q4[Versionado<br/>Dataset hash · Modelo serializado<br/>con metadata]
