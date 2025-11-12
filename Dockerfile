# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create dagster user and directories
RUN useradd -r -d /opt/dagster dagster && \
    mkdir -p $DAGSTER_HOME && \
    chown -R dagster:dagster /opt/dagster

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project code
COPY dagster_project/ ./dagster_project/
COPY utils/ ./utils/
COPY config/ ./config/
COPY .env .
COPY dagster.yaml $DAGSTER_HOME/

# Change ownership to dagster user
RUN chown -R dagster:dagster /opt/dagster

# Switch to dagster user
USER dagster

# Expose port for Dagit
EXPOSE 3000

# Default command to run Dagster webserver
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/dagster_project/workspace.yaml"]