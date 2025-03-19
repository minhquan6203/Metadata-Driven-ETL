# Metadata-Driven ETL Framework

A configuration-based ETL framework for processing data through bronze, silver, and gold layers using PySpark and Delta Lake.

## Overview

- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleansed data with quality checks
- **Gold Layer**: Analytics-ready data models

## Quick Start

### Windows:
```
run_etl_pipeline.bat
```

### Linux/macOS:
```
chmod +x run_etl_pipeline.sh  # First time only
./run_etl_pipeline.sh
```

These scripts:
1. Build and start Docker container
2. Initialize metadata tables
3. Generate sample data
4. Run ETL pipeline
5. View all tables in Bronze, Silver, and Gold layers

## Manual Steps

### Setup
```
docker-compose build
docker-compose up -d
```

### Run Pipeline Components
```
# Initialize metadata
docker exec -it metadata-etl python scripts/init_metadata_tables.py

# Generate sample data
docker exec -it metadata-etl python scripts/generate_sample_data.py

# Run ETL pipeline
docker exec -it metadata-etl python scripts/run_etl_pipeline.py

# View all tables (final step)
docker exec -it metadata-etl python query_delta_tables.py
```

### Optional Parameters
```
# Run specific layers
docker exec -it metadata-etl python scripts/run_etl_pipeline.py --layers bronze,silver
```

## Architecture

### Key Components
- **Configuration Files**: YAML definitions for ETL processes
- **Control Tables**: Metadata tracking
- **Layer Processors**: Bronze, Silver, Gold modules
- **Data Quality Engine**: Validation rules
- **Audit Logger**: Execution tracking


## Directory Structure
```
├── scripts/                     # Execution scripts
│   ├── generate_sample_data.py  # Data generator
│   ├── init_metadata_tables.py  # Initialize metadata
│   └── run_etl_pipeline.py      # Pipeline runner
├── query_delta_tables.py        # Table viewer script
└── src/
    ├── config/                  # YAML configurations
    ├── modules/                 # Processing modules
    ├── schema/                  # Data schemas
    └── utils/                   # Utility functions
``` 