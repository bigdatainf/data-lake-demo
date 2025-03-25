# Multi-Zone Data Lake Architecture Demo

This repository demonstrates a comprehensive, Docker-based implementation of a multi-zone data lake architecture. The architecture provides an efficient means for data analytics by enabling users to easily locate, access, interoperate, and reuse existing data, data preparation processes, and analyses.

## Architecture Overview

The multi-zone data lake architecture consists of four primary functional zones:

![Data Lake Architecture](https://raw.githubusercontent.com/yourusername/datalake-demo/main/docs/images/architecture.png)

### 1. Raw Ingestion Zone

- **Purpose**: Store data in its native format without modification
- **Characteristics**: 
  - Preserves original data fidelity
  - Maintains historical record of raw data
  - Supports batch and real-time data ingestion
  - Often uses formats like CSV, JSON, XML, or binary formats

### 2. Process Zone

- **Purpose**: Prepare and transform data according to business needs
- **Characteristics**:
  - Stores intermediate data and processes
  - Applies business logic and transformations
  - Standardizes formats and structures
  - Often uses Parquet format for better performance
  - Retains business knowledge applied to raw data

### 3. Access Zone

- **Purpose**: Make processed data accessible and consumable
- **Characteristics**:
  - Contains analytics-ready data
  - Optimized for visualization and analysis
  - Supports BI tools, dashboards, and ML models
  - Often structured for specific business domains
  - Enables self-service analytics

### 4. Govern Zone

- **Purpose**: Ensure data security, quality, lifecycle, and metadata management
- **Sub-zones**:
  - **Metadata Storage**: Catalogs data assets, lineage, and quality metrics
  - **Security Mechanisms**: Implements authentication, authorization, encryption
- **Characteristics**:
  - Maintains data quality and governance
  - Enforces access controls and policies
  - Tracks data lineage and transformations
  - Monitors resource consumption
  - Ensures compliance with regulations

## Technologies Used

This demonstration uses the following technologies:

- **MinIO**: S3-compatible object storage system serving as the data lake storage layer
- **Trino (formerly PrestoSQL)**: Distributed SQL query engine for data lake analytics
- **Python**: For data processing, transformation, and analysis
- **Pandas/PyArrow**: Data manipulation and analysis libraries
- **Docker & Docker Compose**: Containerization for easy deployment and management

## Directory Structure

```
data-lake-demo/
├── docker-compose.yml            # Docker configuration
├── config/                       # Configuration files
│   ├── trino/                    # Trino configuration
│   │   ├── catalog/              # Catalog configurations
│   │   │   ├── hive.properties
│   │   │   └── minio.properties
│   │   ├── config.properties
│   │   └── jvm.config
├── data/                         # Local data storage
│   ├── raw-ingestion-zone/       # Raw data files
│   ├── process-zone/             # Processed data files
│   ├── access-zone/              # Analytics-ready data files
│   └── govern-zone/              # Governance information
├── scripts/                      # Python scripts for data processing
│   ├── 01_ingest_data.py         # Raw ingestion zone demo
│   ├── 02_process_data.py        # Process zone demo
│   ├── 03_access_zone.py         # Access zone demo
│   ├── 04_govern_zone.py         # Govern zone demo
│   ├── 05_query_data.py          # Querying data from the lake
│   └── utils.py                  # Utility functions
└── README.md                     # This file
```

## Data Flow

The multi-zone architecture facilitates a clear data flow:

1. **Data Ingestion**: Raw data is ingested into the raw-ingestion-zone in its original format
2. **Data Processing**: Raw data is transformed in the process-zone, with business rules applied
3. **Data Access**: Processed data is prepared for analytics in the access-zone
4. **Governance**: Metadata, lineage, and security are managed in the govern-zone

This approach separates concerns between different data processing stages and enables better scalability, governance, and data management.

## Setup and Usage

### Prerequisites

- Docker and Docker Compose
- Internet connection (to pull Docker images)

### Getting Started

1. Clone this repository
   ```
   git clone https://github.com/yourusername/data-lake-demo.git
   cd data-lake-demo
   ```

2. Start the containers
   ```
   docker-compose up -d
   ```

3. Run the data ingestion script
   ```
   docker exec -it python-client python /scripts/01_ingest_data.py
   ```

4. Run the data processing script
   ```
   docker exec -it python-client python /scripts/02_process_data.py
   ```

5. Run the access zone preparation script
   ```
   docker exec -it python-client python /scripts/03_access_zone.py
   ```

6. Run the governance demo
   ```
   docker exec -it python-client python /scripts/04_govern_zone.py
   ```

7. Query the data lake
   ```
   docker exec -it python-client python /scripts/05_query_data.py
   ```

### Accessing the Components

- **MinIO Console**: http://localhost:9001 (Username: minioadmin, Password: minioadmin)
- **Trino UI**: http://localhost:8085 (User: trino)

## Zone Contributions and Examples

### Raw Ingestion Zone Contributions:

- **Data Preservation**: Original data is kept intact for compliance and auditability
- **Source of Truth**: Serves as the authoritative source for all derived data
- **Format Variety**: Handles various data formats from different source systems
- **Example Files**: CSV files containing raw transaction, customer, and product data

### Process Zone Contributions:

- **Data Standardization**: Cleans and standardizes data fields
- **Business Logic**: Applies transformations and business rules
- **Data Enrichment**: Adds calculated fields and derived values
- **Storage Optimization**: Converts data to Parquet format for better performance
- **Example Files**: Parquet files with cleaned transaction data, enriched customer data, standardized product data

### Access Zone Contributions:

- **Analytics Ready**: Prepares data specifically for analytics and reporting
- **Business View**: Structures data according to business domains and use cases
- **Aggregated Datasets**: Pre-calculates common metrics and aggregations
- **Format Options**: Provides data in formats suitable for different tools
- **Example Files**: Parquet and CSV files with sales aggregations, customer segments, and product performance metrics

### Govern Zone Contributions:

- **Metadata Tracking**: Catalogs all datasets and their attributes
- **Data Lineage**: Records how data moves and transforms through the lake
- **Quality Monitoring**: Tracks and reports on data quality metrics
- **Security Policies**: Defines and enforces access controls
- **Example Files**: JSON and YAML files with metadata, lineage information, quality metrics, and security policies

## Benefits of Multi-Zone Architecture

1. **Simplified Data Management**: Clear separation of concerns through different zones
2. **Enhanced Data Governance**: Comprehensive metadata and lineage tracking
3. **Optimized Performance**: Right storage formats for different processing stages
4. **Improved Data Quality**: Validation and quality monitoring throughout the pipeline
5. **Self-Service Analytics**: Well-organized data that's ready for business users
6. **Regulatory Compliance**: Preservation of raw data and detailed audit trails
7. **Reusable Data Assets**: Properly cataloged and documented data for reuse

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
