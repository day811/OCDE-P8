# GreenCoop Forecast 2.0 - Weather Data Pipeline

## Overview

This project implements a comprehensive data engineering pipeline for the GreenCoop electricity cooperative (Forecast 2.0 initiative). The pipeline collects meteorological data from multiple sources, normalizes the data into a unified format, loads it into MongoDB, and validates data quality post-ingestion.

**Project Status**: Steps 1-3 completed and fully functional (preprocessing, ingestion, and containerization)

## Project Context

GreenCoop requires accurate weather data to improve demand forecasting for renewable energy production. This pipeline integrates three external data sources with different formats and schemas into a single, queryable MongoDB database:

- **Weather Underground** (La Madeleine, France - ILAMAD25)
- **Weather Underground** (Ichtegem, Belgium - IICHTE19)
- **InfoClimat** (Open meteorological stations across Hauts-de-France)

## Architecture Overview

The pipeline follows an ETL (Extract, Transform, Load) pattern with post-load quality verification:

```
Data Sources (Excel/JSON)
        ↓
   [Preprocessing] - unified_data_pipeline.py
        ↓
  JSONL Normalization (S3 or local)
        ↓
   [Ingestion] - mongo_loader.py
        ↓
   MongoDB Collections
        ↓
 [Quality Checker] - quality_checker.py
        ↓
    Quality Report
```

## Technical Stack

- **Language**: Python 3.11
- **Containerization**: Docker & Docker Compose
- **Database**: MongoDB 7.0
- **Cloud Storage**: AWS S3 (optional)
- **Key Dependencies**:
  - `pymongo==4.6.0` - MongoDB driver
  - `pandas==2.3.3` - Data processing
  - `openpyxl==3.1.5` - Excel file parsing
  - `PyYAML==6.0.3` - Configuration management
  - `boto3==1.41.5` - AWS S3 integration
  - `requests==2.32.5` - HTTP requests

## Project Structure

```
.
├── README.md                              # Main documentation
├── docker-compose.yml                     # Container orchestration
├── requirements.txt                       # Python dependencies
├── .env.template                          # Environment variables template
├── .vscode/
│   ├── launch.json                       # VSCode debug configurations
│   └── settings.json                     # VSCode settings
├── config/
│   └── config.yaml                       # Configuration file
├── src/
│   ├── preprocessing/
│   │   ├── unified_data_pipeline.py      # Step 1: Data transformation
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── ingestion/
│   │   ├── mongo_loader.py               # Step 2: Data loading
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   └── quality_checker/
│       ├── quality_checker.py            # Step 3: Quality validation
│       ├── requirements.txt
│       └── Dockerfile
├── mongodb/
│   └── init-db.js                        # MongoDB initialization script
├── tests/
│   ├── test_preprocessing.py             # Preprocessing tests
│   └── test_data_quality.py              # Quality validation tests
├── logs/                                 # Execution logs (runtime)
└── data/
    └── clean/                            # Local storage for JSONL (optional)
```

## Implementation Details

### Step 1: Preprocessing - Data Extraction and Transformation

**File**: `src/preprocessing/unified_data_pipeline.py`

The preprocessing stage handles heterogeneous data sources through a pluggable handler architecture:

#### Data Source Handlers

1. **ExcelDataHandler** - Weather Underground (WU) stations
   - Reads multi-sheet Excel files (one sheet per date in DDMMYY format)
   - Handles unit conversions:
     - Fahrenheit → Celsius
     - Inches (pressure, precipitation) → hPa / mm
     - mph → km/h
   - Maps text-based wind directions (N, NE, E, etc.) → degrees (0-360)
   - Extracts station metadata from configuration

2. **JSONDataHandler** - InfoClimat API responses
   - Parses JSON structures with nested hourly records
   - Handles multiple date/time formats via `dateutil.parser`
   - Safely converts numeric fields (null/NaN handling)
   - Preserves complete station hierarchy

#### Unified Output Schema

All sources are normalized to a common JSONL structure:

```json
{
  "status": "OK",
  "stations": [
    {
      "id": "ILAMAD25",
      "name": "La Madeleine",
      "latitude": 50.659,
      "longitude": 3.07,
      "elevation": 23,
      "city": "La Madeleine",
      "state": "-/-",
      "hardware": "other",
      "software": "EasyWeatherPro_V5.1.6"
    }
  ],
  "hourly": {
    "ILAMAD25": [
      {
        "id_station": "ILAMAD25",
        "dh_utc": "2024-10-01T00:00:00Z",
        "temperature": 15.0,
        "pression": 1013.0,
        "humidite": 87.5,
        "point_de_rosee": 13.2,
        "visibilite": 8000,
        "vent_moyen": 12.5,
        "vent_rafales": 18.3,
        "vent_direction": 225,
        "pluie_3h": 0.0,
        "pluie_1h": 0.0,
        "neige_au_sol": 0,
        "nebulosite": "7",
        "temps_omm": 0
      }
    ]
  },
  "metadata": { /* output_metadata from config */ }
}
```

#### Features

- **Empty Row Filtering**: Removes observations with no measurement values
- **Source Selection**: `FILE_SELECT=latest` (default) loads only the most recent file; `FILE_SELECT=all` loads all available files
- **Dual Output**: 
  - **Local filesystem** (if `LOCAL_STORAGE` is set): Saves JSONL to `data/clean/` with timestamp
  - **AWS S3** (if `LOCAL_STORAGE` is empty): Uploads to configured S3 bucket with credentials from `.env`
- **Fallback Logic**: Automatically falls back to local save if S3 credentials are missing

### Step 2: Ingestion - MongoDB Data Loading

**File**: `src/ingestion/mongo_loader.py`

The ingestion stage loads normalized JSONL data into a schema-validated MongoDB database with 3 normalized collections:

#### MongoDB Collections & Indexes

1. **stations** - Station metadata (denormalized for reference)
   - Unique index on `id_station`
   - Index on `city`

2. **observations** - Weather measurements (normalized, linked via `id_station`)
   - Index on `id_station`
   - Index on `dh_utc` (descending for time-range queries)
   - Compound index on `(id_station, dh_utc)` for efficient station-time queries
   - Index on `_source` for provenance tracking

3. **schema_metadata** - Data dictionary
   - Unique index on `field_name`
   - Contains type hints and field descriptions from `output_metadata`

#### Data Validation

Before MongoDB insertion, two validators ensure data integrity:

```python
# Required fields check
- id_station: Must be present
- dh_utc: Must be ISO 8601 format with 'T' and 'Z' separators

# At least one measurement required
- Fields: temperature, pression, humidite, vent_moyen, vent_rafales, pluie_1h, visibilite
```

#### UPSERT Strategy

Uses MongoDB `upsert=True` on compound keys:
- **Stations**: Keyed by `id_station`
- **Observations**: Keyed by `(id_station, dh_utc)` pair

This allows safe re-runs without duplicate records.

#### Connection Management

- URI format: `mongodb://username:password@host:27017/database`
- For Docker: `mongodb://admin:password@mongodb:27017` (service name resolution)
- For local: `mongodb://admin:password@localhost:27017`
- Automatic protocol switching via `DOCKMODE` environment variable

#### Reporting

Post-load statistics capture:
- Stations inserted/updated
- Observations inserted/updated
- Schema fields loaded
- Skipped/error counts

### Step 3: Containerization - Docker Orchestration

**File**: `docker-compose.yml`

Four-service architecture with startup dependencies:

#### Service 1: Preprocessing
- Container: `greencoop-preprocessing`
- Image: Python 3.11-slim with requirements from `src/preprocessing/requirements.txt`
- Execution: Runs `unified_data_pipeline.py` once, then completes
- Volumes:
  - `config/` (read-only) → `/app/config`
  - `data/clean/` → `/app/data/clean` (JSONL output)
  - `logs/` → `/app/logs`
- Environment: AWS credentials, S3 paths, log level

#### Service 2: Ingestion
- Container: `greencoop-ingestion`
- Depends on: `preprocessing` (service_completed_successfully) and `mongodb` (service_healthy)
- Execution: Runs `mongo_loader.py`, loads JSONL from preprocessing output
- Volumes:
  - `data/clean/` (read-only) → `/app/data/clean`
  - `config/` (read-only) → `/app/config`
  - `logs/` → `/app/logs`
- MongoDB Connection: Automatically resolves `@mongodb:` (Docker service name) when `DOCKMODE=true`

#### Service 3: MongoDB
- Image: `mongo:7.0` (official MongoDB)
- Initialization: `mongodb/init-db.js` auto-executes on first startup
- Healthcheck: PING every 10 seconds with 3-retry threshold
- Volume: `mongodb_data` (persistent storage)
- Port: 27017 (internal only, not exposed by default)
- Credentials: From `.env` (MONGO_ROOT_USER, MONGO_PASSWORD)

#### Service 4: Quality Checker
- Container: `greencoop-quality-checker`
- Depends on: `ingestion` (service_completed_successfully)
- Execution: Runs `quality_checker.py` for post-load validation
- Volumes: `logs/` → `/app/logs`

#### Network

All services connect via `greencoop_network` (bridge driver) for internal service discovery.

#### Execution Order

```
Start MongoDB ── Wait for healthcheck (10s, 3 retries)
        ↓
Start Preprocessing ── Download files, transform to JSONL
        ↓ (service_completed_successfully)
Start Ingestion ── Load JSONL into MongoDB
        ↓ (service_completed_successfully)
Start Quality Checker ── Validate data quality
```

#### Volume Strategy

- **mongodb_data**: Persistent MongoDB storage (survives container restart)
- **logs/**: Shared across all containers for centralized debugging
- **data/clean/**: Transient JSONL storage (survives preprocessing failure but gets overwritten each run)
- **config/**: Read-only configuration (immutable during execution)

## Configuration

### Environment Variables (.env)

Create a `.env` file from `.env.template`:

```bash
# MongoDB
MONGO_PASSWORD=your_secure_password
MONGO_ROOT_USER=admin

# Preprocessing
CONFIG_FILE=config.yaml
LOCAL_STORAGE=                          # Empty = use S3; set to "data/clean" for local
FILE_SELECT=latest                      # "latest" or "all"
LOG_LEVEL=INFO

# Ingestion
MONGODB_URI=mongodb://admin:password@mongodb:27017
DATABASE_NAME=greencoop_forecast

# AWS (optional, for S3 storage)
AWS_ACCESS_KEY_ID=your_key_id
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=eu-west-3
S3_BUCKET=your-bucket-name
S3_PATH=weather_data_unified
```

### Config File (config/config.yaml)

Defines data sources, transformations, and output schema. Example structure:

```yaml
sources:
  wunderground_ilamad25:
    type: "excel"
    source_name: "Weather Underground"
    station_id: "ILAMAD25"
    file_path: "https://s3.amazonaws.com/.../Weather+Underground+-+La+Madeleine.xlsx"
    skip_empty_rows: true

  infoclimat:
    type: "json"
    source_name: "InfoClimat"
    file_path: "https://s3.amazonaws.com/.../Data_Source1.json"

output_metadata:
  temperature: "float - degrees Celsius"
  pression: "float - hPa (mean sea level pressure)"
  # ... (all field definitions)
```

## Running the Pipeline

### Prerequisites

- Docker & Docker Compose (v2.0+)
- Linux Debian (or any Docker-compatible OS)
- VSCode (optional, for debugging)

### Quick Start

```bash
# 1. Clone repository
git clone <repo-url>
cd ocde-p8

# 2. Setup environment
cp .env.template .env
# Edit .env with your credentials

# 3. Create logs directory
mkdir -p logs

# 4. Run full pipeline
docker-compose up --build

# 5. Verify MongoDB
docker exec greencoop-mongodb mongosh -u admin -p <password> --eval "db.greencoop_forecast.observations.countDocuments()"
```

### Step-by-Step Execution

```bash
# Run only preprocessing
docker-compose up preprocessing

# Run only MongoDB + preprocessing + ingestion
docker-compose up --build preprocessing ingestion mongodb

# Follow logs in real-time
docker-compose logs -f preprocessing

# Stop all services
docker-compose down

# Cleanup (remove volumes)
docker-compose down -v
```

### Local Development (without Docker)

```bash
# 1. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start MongoDB (locally or Docker)
docker run -d --name mongodb -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password -p 27017:27017 mongo:7.0

# 4. Run preprocessing
export MONGODB_URI=mongodb://admin:password@localhost:27017
python src/preprocessing/unified_data_pipeline.py --config-file config.yaml

# 5. Run ingestion
python src/ingestion/mongo_loader.py --config-file config.yaml --local-storage data/clean

# 6. Run quality checker
python src/quality_checker/quality_checker.py --database-name greencoop_forecast
```

## Testing

Unit tests validate data transformations before database insertion:

```bash
# Run preprocessing tests
python -m pytest tests/test_preprocessing.py -v

# Run data quality tests
python -m pytest tests/test_data_quality.py -v

# Run all tests
python -m pytest tests/ -v -s
```

### Test Coverage

- **Excel parsing**: Fahrenheit/Celsius, mph/kmh, wind direction conversions
- **JSON parsing**: Field type conversions, date normalization
- **Empty row filtering**: Measurement field presence checks
- **MongoDB validation**: Schema compliance, required field checks
- **Quality checks**: Null percentages, data ranges, type consistency, date coverage

## Logs

All execution logs are written to `logs/`:

- `logs/pipeline.log` - Preprocessing transformations and source selection
- `logs/ingestion.log` - MongoDB operations, upsert statistics, connection issues
- `logs/quality_checker.log` - Quality validation results, data integrity checks

Logs can be viewed in real-time:

```bash
# Follow preprocessing
tail -f logs/pipeline.log

# Follow ingestion
docker logs -f greencoop-ingestion

# View all service logs
docker-compose logs -f
```

## Debugging

### VSCode Configuration

Debug configurations are pre-configured in `.vscode/launch.json`:

- **Test Preprocessing - Unified Pipeline**: Run full transformation pipeline
- **Test Preprocessing - Excel Handler**: Unit test Excel parsing
- **Test Preprocessing - JSON Handler**: Unit test JSON parsing
- **Test All Preprocessing**: Run entire preprocessing test suite
- **Test Data Quality**: Run post-load quality checks
- **Test All Tests**: Execute full test suite with short traceback
- **Test Ingestion - Mongo Loader**: Load JSONL into MongoDB
- **Test Quality Checker**: Run data quality validation
- **Python: Current File (Debug)**: Run current open file

To debug:

```bash
# In VSCode, press F5 or use Debug sidebar
# Select configuration from dropdown
# Set breakpoints with left-click on line numbers
```

### Common Issues

**Problem**: MongoDB connection refused
```bash
# Check MongoDB status
docker ps | grep mongodb
# Verify healthcheck
docker ps --format="{{.Names}}\t{{.Status}}" | grep mongodb
```

**Problem**: S3 upload fails (credentials invalid)
```bash
# Check credentials in .env
echo $AWS_ACCESS_KEY_ID  # Should not be empty
# Fall back to local storage by setting LOCAL_STORAGE=data/clean
```

**Problem**: Empty JSONL output (no records)
```bash
# Check if skip_empty_rows is too aggressive
# Verify source files have measurement data (not just metadata)
# Review logs/pipeline.log for filtering details
```

## Monitoring & Alerts

### Quality Checker - 7 Key Validations

After ingestion, the quality checker performs:

1. **Missing Required Fields**: Count of records missing `id_station` or `dh_utc`
2. **Duplicates**: Detect duplicate `(id_station, dh_utc)` pairs
3. **Data Ranges**: Check temperature, pressure, humidity min/max/avg
4. **Null Percentages**: Per-field null rate with 20% threshold alert
5. **Unique Values**: Count distinct stations, sources, cities
6. **Type Consistency**: Verify temperature is numeric, id_station is string
7. **Date Coverage**: Check for gaps in time-series data

### Example Quality Report

```
MONGODB DATA QUALITY REPORT - POST-INGESTION ANALYSIS

Report generated: 2024-10-07T10:30:45Z

COLLECTION STATISTICS
Total observations: 14,532
Total stations: 3
Schema fields defined: 18

QUALITY CHECKS
✓ Check 1: Missing Required Fields
    id_station: 0 records (0.00%)
    dh_utc: 0 records (0.00%)

✓ Check 2: Duplicates: 0 records (0.00%)

✓ Check 3: Data Ranges
    temperature: Min: -5.2, Max: 32.1, Avg: 14.8
    pression: Min: 985.3, Max: 1025.7, Avg: 1013.2

✓ Check 4: Null Percentages
    temperature: 2.3%
    pression: 3.1%
    humidite: 2.5%

✓ Check 5: Unique Values
    unique_stations: 3
    unique_sources: 3
    schema_fields: 18

✓ Check 6: Type Consistency
    temperature_types: {double: 14200}
    station_types: {string: 14532}

✓ Check 7: Date Coverage
    Min date: 2024-10-01
    Max date: 2024-10-07
    Coverage: 98.5%

ALERTS
⚠️  High null rate for visibilite: 45.2%
```

## Database Queries

### Example Queries (MongoDB)

```javascript
// Get latest observations for all stations
db.observations
  .find({})
  .sort({ dh_utc: -1 })
  .limit(1000);

// Get observations for La Madeleine (2024-10-01)
db.observations.find({
  id_station: "ILAMAD25",
  dh_utc: { $gte: "2024-10-01T00:00:00Z", $lte: "2024-10-01T23:59:59Z" }
});

// Average temperature by station over time window
db.observations.aggregate([
  {
    $match: {
      dh_utc: { $gte: "2024-10-01T00:00:00Z", $lte: "2024-10-07T23:59:59Z" }
    }
  },
  {
    $group: {
      _id: "$id_station",
      avg_temp: { $avg: "$temperature" },
      count: { $sum: 1 }
    }
  }
]);

// Data quality: records with missing temperature
db.observations.countDocuments({ temperature: null });
```

## Next Steps (Steps 4-5 - Not Included)

This documentation covers **Steps 1-3 only**. The following phases remain:

- **Step 4**: AWS deployment (ECS, production database, billing configuration)
- **Step 5**: Presentation preparation (slides, demo, architecture diagrams)

For AWS setup guidance, refer to `docs/AWS_SETUP.md` (to be created in a separate task).

## Project Maintenance

### Updating Dependencies

```bash
# Regenerate requirements
pip freeze > requirements.txt

# Rebuild Docker images
docker-compose build --no-cache

# Test new versions
docker-compose up --build
```

### Data Cleanup

```bash
# Clear all data and start fresh
docker-compose down -v
docker volume rm ocde-p8_mongodb_data

# Rebuild from scratch
docker-compose up --build
```

## Additional Resources

- **MongoDB Documentation**: https://docs.mongodb.com/
- **Docker Compose Reference**: https://docs.docker.com/compose/compose-file/
- **Python Logging**: https://docs.python.org/3/library/logging.html
- **AWS S3**: https://docs.aws.amazon.com/s3/
- **InfoClimat API**: https://www.infoclimat.fr/
- **Weather Underground**: https://www.wunderground.com/

## Support

For questions or issues:

1. Check `logs/` directory for error messages
2. Review test suite output (`tests/`)
3. Consult configuration in `config/config.yaml`
4. Verify environment variables in `.env`
5. Inspect MongoDB directly with mongosh

---

**Project**: GreenCoop Forecast 2.0  
**Phase**: Data Pipeline Implementation (Steps 1-3)  
**Status**: Operational and tested  
**Last Updated**: December 2024
