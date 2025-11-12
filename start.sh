#!/bin/bash

# Dagster OpenMeteo Weather Data Pipeline - Startup Script

echo "🌤️  Starting Dagster OpenMeteo Weather Data Pipeline"
echo "================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Error: docker-compose is not installed."
    exit 1
fi

echo "✅ Docker is running"
echo "📦 Building and starting containers..."

# Stop any existing containers
docker-compose down

# Build and start the services
docker-compose up --build -d

echo ""
echo "🎉 Startup complete!"
echo ""
echo "🔗 Access points:"
echo "   • Dagster UI: http://localhost:3000"
echo "   • PostgreSQL: localhost:5432"
echo ""
echo "📊 To check container status:"
echo "   docker-compose ps"
echo ""
echo "📝 To view logs:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 To stop:"
echo "   docker-compose down"
echo ""
echo "⚡ To verify data in PostgreSQL:"
echo '   docker exec -it dagster_postgres psql -U dagster -d weather -c "SELECT * FROM weather_data LIMIT 5;"'