# Dagster Data Ingestion Demo (Open-Meteo Weather Data)

A complete ETL pipeline demonstration using **Dagster**, **PostgreSQL**, and **Docker** to fetch, transform, and load weather data from the Open-Meteo API.

## 🌟 Project Overview

This project showcases a modern data engineering pipeline that:

- **Fetches** hourly temperature data from the [Open-Meteo API](https://open-meteo.com/en/docs) for Berlin
- **Transforms** the raw data using pandas (cleaning, formatting, validation)
- **Loads** the processed data into a PostgreSQL database
- **Orchestrates** everything with Dagster for monitoring, scheduling, and data lineage
- **Runs** completely in Docker containers for easy deployment

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
├── Dockerfile                    # Container setup
├── docker-compose.yml           # Multi-service orchestration
├── requirements.txt             # Python dependencies
├── .env                        # Environment variables
└── README.md                   # This file
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
docker exec -it dagster_postgres psql -U dagster -d weather -c "SELECT * FROM weather_data LIMIT 10;"
```

Expected output:
```
     timestamp      | temperature_2m | latitude | longitude
--------------------+----------------+----------+-----------
 2024-11-12 00:00:00|           8.2  |    52.52 |     13.405
 2024-11-12 01:00:00|           7.8  |    52.52 |     13.405
 ...
```

## 🔧 How It Works

### Pipeline Steps

1. **Fetch (`fetch_weather_data`)**: 
   - Connects to Open-Meteo API with caching and retry logic
   - Fetches hourly temperature data for Berlin
   - Returns pandas DataFrame

2. **Transform (`transform_weather_data`)**:
   - Renames columns for database consistency
   - Validates and cleans data types
   - Removes null values
   - Rounds temperature values

3. **Load (`load_weather_to_db`)**:
   - Creates PostgreSQL table if needed
   - Removes existing data for the same time period
   - Inserts new data using SQLAlchemy

### Data Schema

The `weather_data` table has the following structure:

| Column         | Type     | Description                    |
|----------------|----------|--------------------------------|
| `timestamp`    | DateTime | Date and time (hourly)        |
| `temperature_2m` | Float  | Temperature at 2 meters (°C)  |
| `latitude`     | Float    | Location latitude              |
| `longitude`    | Float    | Location longitude             |

## 📊 Features

- **🔄 Caching**: Open-Meteo requests are cached for 1 hour
- **🔁 Retry Logic**: Automatic retry on API failures
- **📝 Comprehensive Logging**: Detailed logs at each pipeline step
- **🚫 Duplicate Prevention**: Removes existing data before inserting new records
- **🛡️ Error Handling**: Graceful error handling with informative messages
- **🐳 Fully Containerized**: No local Python setup required
- **📱 Web UI**: Beautiful Dagster interface for monitoring and management

## 🛠️ Development

### Local Development Setup

If you want to develop locally without Docker:

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up PostgreSQL** (or use Docker for just the DB):
   ```bash
   docker run --name postgres -e POSTGRES_DB=weather -e POSTGRES_USER=dagster -e POSTGRES_PASSWORD=dagster123 -p 5432:5432 -d postgres:15
   ```

3. **Update `.env`** for local development:
   ```bash
   POSTGRES_HOST=localhost
   DATABASE_URL=postgresql://dagster:dagster123@localhost:5432/weather
   ```

4. **Run Dagster webserver locally**:
   ```bash
   cd dagster_project
   dagster-webserver -w workspace.yaml
   ```

### Adding New Features

- **New ops**: Add to `dagster_project/ops/`
- **New jobs**: Add to `dagster_project/jobs/`
- **Update repository**: Import and register in `repository.py`

## 🔍 Troubleshooting

### Common Issues

**1. Port 3000 already in use**
```bash
# Find and kill the process
lsof -ti:3000 | xargs kill -9
```

**2. Database connection errors**
```bash
# Check if PostgreSQL container is running
docker logs dagster_postgres
```

**3. Dagster not finding modules**
```bash
# Rebuild containers
docker-compose down
docker-compose up --build
```

**4. API rate limiting**
The Open-Meteo API is free and has rate limits. The pipeline includes caching to minimize requests.

### Logs and Debugging

- **View Web UI logs**: `docker logs dagster_webserver`
- **View Daemon logs**: `docker logs dagster_daemon`
- **View PostgreSQL logs**: `docker logs dagster_postgres`

## 📈 Next Steps

Potential enhancements:

- **📅 Scheduling**: Add daily/hourly schedules with Dagster sensors
- **🌍 Multi-city**: Extend to fetch data for multiple cities
- **📊 Analytics**: Add data quality checks and metrics
- **☁️ Cloud Deploy**: Deploy to AWS/GCP/Azure
- **📧 Alerting**: Add email/Slack notifications for pipeline failures
- **🏪 Data Catalog**: Integrate with data catalog systems

## 📚 Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Open-Meteo API Docs](https://open-meteo.com/en/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

---
