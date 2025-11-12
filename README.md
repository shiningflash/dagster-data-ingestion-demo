# Dagster Weather Data Ingestion Pipeline

A **configuration-driven**, **scalable** ETL pipeline using **Dagster**, **PostgreSQL**, and **Docker** to fetch, transform, and load weather data from multiple APIs with comprehensive data quality validation.

## 🌟 Key Features

This project showcases a **modern, enterprise-ready data engineering pipeline** that:

- **📡 Multi-Source Data Ingestion**: Configurable data sources via JSON files - easily add new weather stations, APIs, or data providers
- **🔧 Configuration-Driven**: Complete control over data sources, schedules, validation rules, and database schemas through JSON configuration
- **🏗️ Scalable Architecture**: Supports multiple cities/locations simultaneously with individual or batch processing
- **✅ Data Quality Validation**: Configurable data quality checks, range validations, and null value handling
- **🛡️ Error Resilience**: Comprehensive error handling, retry logic, and graceful degradation
- **📊 Flexible Scheduling**: Per-source scheduling with cron expressions
- **🧹 Automated Cleanup**: Configurable data retention policies
- **🐳 Fully Containerized**: Complete Docker setup for development and production
- **📈 Observable**: Rich logging, monitoring, and Dagster UI for pipeline visibility

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Open-Meteo    │    │    Transform    │    │   PostgreSQL    │
│      API        │───▶│   with Pandas   │───▶│    Database     │
│  (Fetch Data)   │    │  (Clean & Prep) │    │  (Store Data)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │    Dagster      │
                    │  Orchestration  │
                    └─────────────────┘
```

## 📁 Project Structure

```
dagster-data-ingestion-demo/
├── dagster_project/
│   ├── __init__.py
│   ├── ops/
│   │   ├── fetch_weather.py      # Fetch data from Open-Meteo API
│   │   ├── transform_weather.py  # Transform data with pandas
│   │   └── load_to_db.py         # Load data into PostgreSQL
│   ├── jobs/
│   │   └── weather_job.py        # Dagster job definition
│   ├── repository.py             # Dagster repository
│   └── workspace.yaml            # Dagster workspace config
├── config/
│   ├── data_sources.json        # API configuration
│   ├── pipeline_settings.json   # Pipeline settings
│   └── environments.json        # Environment configs
├── utils/
│   ├── config_loader.py         # Configuration loader
│   ├── api_clients.py           # API client factory
│   └── database.py              # Database utilities
├── Dockerfile                   # Container setup
├── docker-compose.yml          # Multi-service orchestration
├── requirements.txt            # Python dependencies
├── .env                       # Environment variables
└── README.md                  # This file
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Git (to clone the repository)

### 1. Clone and Navigate

```bash
git clone <repository-url>
cd dagster-data-ingestion-demo
```

### 2. Environment Setup

The `.env` file is already configured with default values:

```bash
# PostgreSQL Configuration
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=weather
POSTGRES_USER=dagster
POSTGRES_PASSWORD=dagster123

# Database URL for SQLAlchemy
DATABASE_URL=postgresql://dagster:dagster123@postgres:5432/weather

# Dagster Configuration
DAGSTER_HOME=/opt/dagster/dagster_home
```

> **Note**: For production use, change the default passwords!

### 3. Launch the Pipeline

```bash
docker-compose up --build
```

This command will:
- Build the Dagster application container
- Start PostgreSQL database
- Launch Dagster daemon for job execution
- Start Dagster web UI on port 3000

### 4. Access Dagster UI

Open your browser and navigate to: **http://localhost:3000**

You'll see the Dagster web interface with your `weather_etl_job` ready to run.

### 5. Run the Pipeline

1. In the Dagster UI, go to **Jobs** → **weather_etl_job**
2. Click **Launch Run** to execute the pipeline
3. Monitor the execution in real-time with logs and progress

### 6. Verify Data in PostgreSQL

Check that data was successfully loaded:

```bash
# Check table structure
docker exec -it dagster_postgres psql -U dagster -d weather -c "\dt"

# View recent weather data
docker exec -it dagster_postgres psql -U dagster -d weather -c "
SELECT 
    timestamp,
    temperature_2m,
    relative_humidity_2m,
    wind_speed_10m,
    latitude,
    longitude,
    source_id
FROM weather_data 
ORDER BY timestamp DESC 
LIMIT 10;"

# Count total records and data range
docker exec -it dagster_postgres psql -U dagster -d weather -c "
SELECT 
    COUNT(*) as total_records,
    MIN(timestamp) as earliest_data,
    MAX(timestamp) as latest_data
FROM weather_data;"

# Data by location
docker exec -it dagster_postgres psql -U dagster -d weather -c "
SELECT 
    source_id,
    ROUND(latitude::numeric, 2) as lat,
    ROUND(longitude::numeric, 2) as lng,
    COUNT(*) as record_count,
    ROUND(AVG(temperature_2m)::numeric, 2) as avg_temp_c
FROM weather_data 
GROUP BY source_id, latitude, longitude
ORDER BY source_id;"
```

Expected output:
```
     timestamp      | temperature_2m | relative_humidity_2m | wind_speed_10m | latitude | longitude | source_id
--------------------+----------------+---------------------+---------------+----------+-----------+------------------
 2024-11-12 14:00:00|           8.2  |                  75 |           5.4 |    52.52 |     13.405| openmeteo_berlin
 2024-11-12 14:00:00|           9.1  |                  80 |           3.2 |    51.51 |     -0.13 | openmeteo_london
 2024-11-12 13:00:00|           7.8  |                  78 |           4.8 |    52.52 |     13.405| openmeteo_berlin
 ...
```

## 🔧 How It Works

### Pipeline Steps

1. **Fetch (`fetch_all_weather_data`)**: 
   - Connects to Open-Meteo API for all enabled sources
   - Fetches hourly weather data (temperature, humidity, wind speed)
   - Includes retry logic and error handling
   - Returns combined data from all sources

2. **Transform (`transform_all_weather_data`)**:
   - Standardizes column names across all sources
   - Validates and cleans data types
   - Removes null values and duplicates
   - Adds source identification

3. **Load (`load_all_weather_to_db`)**:
   - Creates PostgreSQL table if needed
   - Removes existing data for the same time period
   - Inserts new data using SQLAlchemy
   - Handles data conflicts gracefully

### Current Data Sources

Based on your `config/data_sources.json`:

| Location | Status | Coordinates | Source ID |
|----------|--------|-------------|-----------|
| Berlin   | ✅ Enabled | 52.52°N, 13.405°E | `openmeteo_berlin` |
| London   | ✅ Enabled | 51.51°N, -0.13°W | `openmeteo_london` |
| Tokyo    | ❌ Disabled | 35.68°N, 139.65°E | `openmeteo_tokyo` |

### Data Schema

The `weather_data` table has the following structure:

| Column               | Type     | Description                    |
|---------------------|----------|--------------------------------|
| `timestamp`         | DateTime | Date and time (hourly)        |
| `temperature_2m`    | Float    | Temperature at 2 meters (°C)  |
| `relative_humidity_2m` | Float | Relative humidity (%)         |
| `wind_speed_10m`    | Float    | Wind speed at 10 meters (m/s) |
| `latitude`          | Float    | Location latitude              |
| `longitude`         | Float    | Location longitude             |
| `source_id`         | String   | Data source identifier         |

## 📊 Data Analysis Examples

### Weather Trends
```sql
-- Temperature comparison between cities
SELECT 
    source_id,
    DATE(timestamp) as date,
    ROUND(AVG(temperature_2m)::numeric, 2) as avg_temp,
    ROUND(MIN(temperature_2m)::numeric, 2) as min_temp,
    ROUND(MAX(temperature_2m)::numeric, 2) as max_temp
FROM weather_data 
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY source_id, DATE(timestamp)
ORDER BY date DESC, source_id;
```

### Data Quality Check
```sql
-- Check for data completeness
SELECT 
    source_id,
    COUNT(*) as total_records,
    COUNT(temperature_2m) as temp_records,
    COUNT(relative_humidity_2m) as humidity_records,
    COUNT(wind_speed_10m) as wind_records,
    ROUND(100.0 * COUNT(temperature_2m) / COUNT(*), 2) as temp_completeness
FROM weather_data 
GROUP BY source_id;
```

## 🛠️ Configuration Management

### Adding New Weather Stations

To add a new location, edit `config/data_sources.json`:

```json
{
  "id": "openmeteo_paris",
  "name": "Open-Meteo Paris Weather", 
  "enabled": true,
  "api_type": "openmeteo",
  "api_config": {
    "base_url": "https://api.open-meteo.com/v1/forecast",
    "latitude": 48.8566,
    "longitude": 2.3522,
    "location_name": "Paris",
    "parameters": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
    "forecast_days": 1
  }
}
```

The pipeline will automatically pick up the new source on the next run!

## 🔍 Troubleshooting

### Common Issues

**1. No data in database**
```bash
# Check if pipeline ran successfully
docker logs dagster_daemon | grep -i error

# Check recent pipeline runs in Dagster UI
open http://localhost:3000
```

**2. API connection errors**
```bash
# Test API connectivity manually
curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.405&hourly=temperature_2m&past_days=1"
```

**3. Database connection errors**
```bash
# Check if PostgreSQL container is running
docker logs dagster_postgres

# Test database connection
docker exec -it dagster_postgres psql -U dagster -d weather -c "SELECT NOW();"
```

**4. Empty results**
```bash
# Check what data sources are enabled
cat config/data_sources.json | jq '.data_sources[] | select(.enabled == true) | .name'

# Check pipeline execution logs
docker logs dagster_webserver | tail -50
```

### Development and Testing

**Run specific pipeline steps:**
```bash
# In Dagster UI → Launchpad → Op Selection:
# - Select only "fetch_all_weather_data" to test API
# - Select only "load_all_weather_to_db" to test database
```

**Reset database:**
```bash
# Drop and recreate table
docker exec -it dagster_postgres psql -U dagster -d weather -c "DROP TABLE IF EXISTS weather_data;"
```

## 📈 Monitoring & Observability

### Key Metrics to Watch

1. **Data Freshness**: Latest timestamp in database
2. **Data Volume**: Records per hour/day
3. **Success Rate**: Pipeline success percentage
4. **API Response Time**: Open-Meteo API performance

### Alerting Setup

Monitor these conditions:
- Pipeline failures (check Dagster logs)
- No new data for > 2 hours
- Temperature readings outside expected ranges
- API rate limiting (429 errors)

## 🚀 Next Steps & Extensions

### Easy Enhancements:
- **📅 Scheduling**: Add hourly/daily schedules
- **🌍 More Cities**: Add major world cities
- **📊 Data Quality**: Add validation rules
- **📈 Metrics**: Add temperature alerts
- **🔔 Notifications**: Slack/email alerts

### Advanced Features:
- **☁️ Cloud Deployment**: AWS/GCP/Azure
- **📊 Dashboard**: Grafana/Superset visualization
- **🏪 Data Lake**: Store raw data in S3/GCS
- **🤖 ML Pipeline**: Weather prediction models
- **📡 Real-time**: Streaming data ingestion

## 📚 Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Open-Meteo API Docs](https://open-meteo.com/en/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

---

**🎯 Quick Test Commands:**
```bash
# Start pipeline
docker-compose up --build

# Check data
docker exec -it dagster_postgres psql -U dagster -d weather -c "SELECT COUNT(*), MAX(timestamp) FROM weather_data;"

# View recent data
docker exec -it dagster_postgres psql -U dagster -d weather -c "SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 5;"
```
