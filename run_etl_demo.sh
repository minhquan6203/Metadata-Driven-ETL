#!/bin/bash
# Shell script to run the complete ETL demo from data generation to processing

echo "===== Metadata-Driven ETL Framework Demo ====="
echo "This script will generate sample data and run the ETL pipeline to process it."

# Step 1: Build Docker container
echo
echo "[1/5] Building Docker container..."
docker build -t metadata-etl . || {
    echo "Error building Docker container. Exiting."
    exit 1
}

# Step 2: Start Docker container if not running
echo
echo "[2/5] Starting Docker container..."
if ! docker ps | grep -q "metadata-etl"; then
    # Container not running, check if it exists but stopped
    if docker ps -a | grep -q "metadata-etl"; then
        # Container exists but is stopped, start it
        echo "Container exists but is stopped. Starting it..."
        docker start metadata-etl
    else
        # Container doesn't exist, create it
        echo "Creating new container..."
        docker run -d --name metadata-etl metadata-etl
    fi
    
    # Verify container is now running
    if ! docker ps | grep -q "metadata-etl"; then
        echo "Container failed to start. Exiting."
        exit 1
    fi
else
    echo "Container already running."
fi

# Step 3: Generate sample data
echo
echo "[3/5] Generating sample data..."
docker exec -it metadata-etl python scripts/generate_sample_data.py || {
    echo "Error generating sample data. Exiting."
    exit 1
}

# Step 4: Initialize metadata tables
echo
echo "[4/5] Initializing metadata tables..."
docker exec -it metadata-etl python scripts/init_metadata_tables.py || {
    echo "Error initializing metadata tables. Exiting."
    exit 1
}

# Step 5: Run the ETL pipeline
echo
echo "[5/5] Running ETL pipeline to process data through all layers..."
docker exec -it metadata-etl python scripts/run_etl_pipeline.py || {
    echo "Warning: ETL pipeline completed with some errors. Check logs for details."
}

# Step 6: View all tables
echo
echo "Viewing tables in all layers..."
docker exec -it metadata-etl python scripts/query_delta_tables.py || {
    echo "Warning: Error viewing tables. Check the script for issues."
}

echo
echo "===== ETL Demo Completed ====="
echo "All tables have been processed and displayed."

read -p "Press Enter to continue..." 